import os, json, time, logging, requests
from datetime import datetime, timezone
from typing import Dict, Any, Optional
from kafka import KafkaConsumer, KafkaProducer
from elasticsearch import Elasticsearch, helpers
from pythonjsonlogger import jsonlogger
from prometheus_client import start_http_server, Counter, Gauge

# Logging
logger = logging.getLogger("dlq-handler")
handler = logging.StreamHandler()
handler.setFormatter(jsonlogger.JsonFormatter())
logger.addHandler(handler)
logger.setLevel(os.getenv("LOG_LEVEL","INFO"))

# Env
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
INPUT_TOPICS = (os.getenv("INPUT_TOPICS","errors-documents,retries-documents")).split(",")
OUTPUT_TOPIC = os.getenv("OUTPUT_TOPIC","enriched-documents")
RETRIES_TOPIC = os.getenv("RETRIES_TOPIC","retries-documents")
DLQ_TOPIC = os.getenv("DLQ_TOPIC","dlq-documents")

MAX_RETRIES = int(os.getenv("MAX_RETRIES","5"))
BASE_BACKOFF_SECONDS = int(os.getenv("BASE_BACKOFF_SECONDS","5"))
BACKOFF_MULTIPLIER = int(os.getenv("BACKOFF_MULTIPLIER","3"))

ES = Elasticsearch(os.getenv("ES_HOST","http://elasticsearch:9200"))
ES_ERROR_INDEX = os.getenv("ES_ERROR_INDEX","news-errors-v1")
ALERT_WEBHOOK_URL = os.getenv("ALERT_WEBHOOK_URL","").strip()

# Prom metrics
start_http_server(9105)
c_in = Counter("dlq_in_total","Total messages read (errors+retries)")
c_retried = Counter("dlq_retried_total","Total messages scheduled for retry")
c_requeued = Counter("dlq_requeued_total","Total messages requeued to output")
c_dlq = Counter("dlq_dead_total","Total messages moved to DLQ")
g_due = Gauge("dlq_due_now","Messages due for retry in this poll")

def ensure_error_index():
    if not ES.indices.exists(index=ES_ERROR_INDEX):
        ES.indices.create(index=ES_ERROR_INDEX, ignore=400)

def classify_error(err: str) -> str:
    e = err.lower()
    if "json" in e or "deserialize" in e:
        return "deserialization"
    if "validation" in e or "pydantic" in e or "missing required field" in e:
        return "validation"
    if "timeout" in e or "connection" in e or "network" in e:
        return "network"
    if "elasticsearch" in e or "bulk" in e:
        return "es_bulk"
    if "neo4j" in e:
        return "neo4j"
    return "unknown"

def now_ms() -> int:
    return int(time.time() * 1000)

def post_alert(text: str):
    if not ALERT_WEBHOOK_URL:
        return
    try:
        requests.post(ALERT_WEBHOOK_URL, json={"text": text}, timeout=3)
    except Exception:
        logger.warning("alert webhook failed")

def log_error_record(stage: str, payload: Dict[str,Any], error: str, retries: int, meta: Dict[str,Any]):
    doc = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "stage": stage,
        "error_type": classify_error(error),
        "message": error[:4000],
        "retries": retries,
        "original_topic": meta.get("topic"),
        "original_partition": meta.get("partition"),
        "original_offset": meta.get("offset"),
        "payload": payload
    }
    try:
        ES.index(index=ES_ERROR_INDEX, document=doc)
    except Exception as e:
        logger.warning("failed to write error doc: %s", e)

def parse_headers(headers) -> Dict[str,Any]:
    # kafka-python provides list[tuple[str, bytes]]
    out = {}
    for k, v in headers or []:
        try:
            out[k] = v.decode("utf-8")
        except Exception:
            out[k] = str(v)
    return out

def build_headers(existing: Dict[str,Any], **add) -> list[tuple[str, bytes]]:
    merged = {**existing, **add}
    return [(k, str(v).encode("utf-8")) for k, v in merged.items()]

def main():
    ensure_error_index()

    consumer = KafkaConsumer(
        *INPUT_TOPICS,
        bootstrap_servers=BOOTSTRAP,
        group_id="dlq-handler-v1",
        enable_auto_commit=False,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="earliest",
        max_poll_records=500
    )
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda m: json.dumps(m).encode("utf-8")
    )

    logger.info("DLQ handler consuming: %s", INPUT_TOPICS)

    while True:
        polled = consumer.poll(timeout_ms=1000)
        due_count = 0
        for tp, msgs in polled.items():
            for m in msgs:
                c_in.inc()
                headers = parse_headers(m.headers)
                retries = int(headers.get("x-retries","0"))
                stage = headers.get("x-stage","unknown-stage")
                error_msg = headers.get("x-error","")
                next_ts = int(headers.get("x-next-at","0"))
                meta = {"topic": m.topic, "partition": m.partition, "offset": m.offset}

                if m.topic == RETRIES_TOPIC and next_ts > 0:
                    # Only process when due
                    if now_ms() < next_ts:
                        continue
                    due_count += 1

                # Decide to retry or DLQ
                if retries < MAX_RETRIES:
                    # schedule next retry
                    from backoff import schedule
                    attempt = retries + 1
                    delay = schedule(attempt, BASE_BACKOFF_SECONDS, BACKOFF_MULTIPLIER)
                    next_at = now_ms() + (delay * 1000)
                    new_headers = build_headers(headers, **{
                        "x-retries": attempt,
                        "x-next-at": next_at
                    })
                    # Emit to RETRIES_TOPIC (delayed by consumer gate)
                    producer.send(RETRIES_TOPIC, m.value, headers=new_headers)
                    c_retried.inc()
                    logger.info("scheduled retry x%d in %ds", attempt, delay)
                else:
                    # give up → DLQ + error doc
                    new_headers = build_headers(headers, **{
                        "x-final": "true",
                        "x-dlq-at": now_ms()
                    })
                    producer.send(DLQ_TOPIC, m.value, headers=new_headers)
                    c_dlq.inc()
                    logger.error("moved to DLQ after %d retries", retries)
                    post_alert(f"DLQ: {stage} exhausted retries={retries} topic={m.topic} offset={m.offset}")
                    # Index an error record for Kibana
                    log_error_record(stage=stage, payload=m.value, error=error_msg, retries=retries, meta=meta)

        g_due.set(due_count)
        if polled:
            consumer.commit()
