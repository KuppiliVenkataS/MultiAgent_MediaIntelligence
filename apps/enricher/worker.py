"""
Kafka consumer → enrichment → producer worker.

- Consumes normalized documents from RAW_TOPIC.
- Runs NER, sentiment, stance, and claim extraction (English-gated).
- Produces EnrichedDocument to ENRICHED_TOPIC.
- On exceptions, emits a context payload to ERROR_TOPIC and skips committing
  that offset so the message can be retried (or handled by a DLQ flow later).
"""
import json, os, sys, signal
from typing import Any, Dict
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaTimeoutError
from .models import EnrichedDocument
from .nlp import extract_entities
from .sentiment import doc_sentiment
from .stance import stance_for_entities
from .claims import extract_claims

# ---- Runtime config via env ----
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "raw-documents")
OUT_TOPIC = os.getenv("ENRICHED_TOPIC", "enriched-documents")
ERR_TOPIC = os.getenv("ERROR_TOPIC", "errors-documents")
GROUP_ID = os.getenv("GROUP_ID", "enricher.v1")

def get_consumer() -> KafkaConsumer:
    """
    Kafka consumer tuned for stream processing:
    - manual commit: only commit after successful produce to OUT_TOPIC.
    - auto_offset_reset: 'earliest' is friendlier for dev/backfills.
    - max_poll_records: small batch to keep memory bounded and latency low.
    """
    return KafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        group_id=GROUP_ID,
        enable_auto_commit=False,          # commit only after successful publish
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        max_poll_records=32,
        consumer_timeout_ms=int(os.getenv("CONSUMER_TIMEOUT_MS","0")),  # 0 = block
    )

def get_producer() -> KafkaProducer:
    """
    Kafka producer tuned for durability and throughput:
    - acks='all' for safety (single-broker dev still useful to catch errors).
    - small linger to batch a bit without adding much latency.
    - compression saves bandwidth/IO on larger payloads.
    """
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: (
            v if isinstance(v, (bytes, bytearray))
            else json.dumps(v, ensure_ascii=False, default=str).encode("utf-8")
        ),
        acks="all",
        linger_ms=25,
        retries=5,
        retry_backoff_ms=300,
        compression_type=os.getenv("KAFKA_COMPRESSION","zstd"),
        request_timeout_ms=30000,
        max_block_ms=60000,
    )

def build_enriched(record: Dict[str, Any]) -> EnrichedDocument:
    """
    Build the EnrichedDocument from an input record.

    We gate heavy work on language (English only), and share the original
    metadata so downstream consumers don’t need to refetch it.
    """
    text = record.get("text") or ""
    lang = (record.get("lang") or "en").lower()
    entities = extract_entities(text) if lang.startswith("en") else []
    return EnrichedDocument(
        id=record["id"],
        source_url=record.get("source_url"),
        source_type=record.get("source_type"),
        title=record.get("title"),
        lang=record.get("lang"),
        entities=entities,
        extra={},
    )

_running = True # global flag toggled by signal handler
def _graceful(*_):
    """Allow Ctrl+C / SIGTERM to stop the loop cleanly."""
    global _running
    _running = False
    print("[enricher] shutdown signal received", flush=True)

def main():
    print("[enricher] starting; bootstrap:", BOOTSTRAP, "in:", RAW_TOPIC, "out:", OUT_TOPIC, flush=True)
    
    # Wire signals for clean shutdown in containers/orchestrators.
    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)

    consumer = get_consumer()
    producer = get_producer()

    # Main poll → process → produce → commit loop.
    while _running:
        batch = consumer.poll(timeout_ms=1000)
        if not batch:
            continue

        for tp, messages in batch.items():
            # Process each message in order per-partition.
            for msg in messages:
                try:
                    record = msg.value # already deserialized JSON
                    enriched = build_enriched(record)
                    fut = producer.send(OUT_TOPIC, enriched.model_dump(mode="json"))
                    fut.get(timeout=30)  # block for acks=all, raises on failure

                    # Send to OUT_TOPIC and block for ack (raises on failure).
                    print(f"[enricher] {enriched.id} -> {OUT_TOPIC}:{tp.partition}", flush=True)

                except Exception as e:
                    # route to error topic with context
                    err = {
                        "error": repr(e),
                        "context": {"topic": tp.topic, "partition": tp.partition, "offset": msg.offset},
                        "payload": msg.value,
                        "stage": "enricher",
                    }
                    try:
                        producer.send(ERR_TOPIC, err)
                        producer.flush(10)
                    except KafkaTimeoutError:
                        print("[enricher] ERROR: failed to write to ERR_TOPIC", file=sys.stderr, flush=True)

                    # do NOT commit this offset; we want it reprocessed later or sent to DLQ once you add a DLQ consumer
                    continue

            # commit after processing the partition batch
            consumer.commit()

    # flush & close
    try:
        producer.flush(10)
    finally:
        consumer.close()
        producer.close()
        print("[enricher] stopped", flush=True)

def build_enriched(record: Dict[str, Any]) -> EnrichedDocument:
    text = record.get("text") or ""
    lang = (record.get("lang") or "en").lower()
    entities = extract_entities(text) if lang.startswith("en") else []

    sentiment = doc_sentiment(text) if lang.startswith("en") else None
    stance = stance_for_entities(text, entities) if lang.startswith("en") else []
    claims = extract_claims(text) if lang.startswith("en") else []

    return EnrichedDocument(
        id=record["id"],
        source_url=record.get("source_url"),
        source_type=record.get("source_type"),
        title=record.get("title"),
        lang=record.get("lang"),
        entities=entities,
        sentiment=sentiment,
        stance=stance,
        claims=claims,
        extra={},
    )

if __name__ == "__main__":
    main()
