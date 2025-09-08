import json, os, time, signal, sys
from typing import Any, Dict, Iterable, List, Optional
import backoff
from elasticsearch import Elasticsearch, ApiError
from elasticsearch.helpers import bulk
from kafka import KafkaConsumer
from .mappings import SETTINGS, INDEX_V1, ALIAS

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
IN_TOPIC = os.getenv("ENRICHED_TOPIC", "enriched-documents")
GROUP_ID = os.getenv("GROUP_ID", "indexer.v1")

ES_URL = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
ES_ALIAS = os.getenv("ES_INDEX_ALIAS", ALIAS)
ES_INDEX = os.getenv("ES_INDEX", INDEX_V1)

BATCH_MAX_DOCS = int(os.getenv("BATCH_MAX_DOCS", "500"))
BATCH_FLUSH_SEC = float(os.getenv("BATCH_FLUSH_SEC", "3"))

_running = True
def _graceful(*_):  # ctrl+c, docker stop
    global _running
    _running = False
    print("[indexer] shutdown signal", flush=True)

def get_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        IN_TOPIC,
        bootstrap_servers=[BOOTSTRAP],
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        max_poll_records=256,
        consumer_timeout_ms=int(os.getenv("CONSUMER_TIMEOUT_MS","0")),
    )

@backoff.on_exception(backoff.expo, Exception, max_time=60)
def get_es() -> Elasticsearch:
    es = Elasticsearch(ES_URL, request_timeout=10)
    es.info()  # ping/verify
    return es

def ensure_index(es: Elasticsearch):
    # create index if not exists
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, **SETTINGS)
        print(f"[indexer] created index {ES_INDEX}", flush=True)
    # create/update alias
    if not es.indices.exists_alias(name=ES_ALIAS):
        es.indices.put_alias(index=ES_INDEX, name=ES_ALIAS)
        print(f"[indexer] alias {ES_ALIAS} -> {ES_INDEX}", flush=True)

def _as_list(value) -> List[Any]:
    if value is None: return []
    if isinstance(value, list): return value
    return [value]

def transform(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform enriched-documents payload into an ES document following our mapping.

    We keep original arrays in _source (entities/stance/claims) and also compute
    flattened facet arrays (ent_names, stance_targets, claim_kinds, …).
    """
    entities = _as_list(record.get("entities"))
    stance = _as_list(record.get("stance"))
    claims = _as_list(record.get("claims"))
    sentiment = record.get("sentiment") or {}

    ent_names = [e.get("text") for e in entities if isinstance(e, dict) and e.get("text")]
    ent_labels = [e.get("label") for e in entities if isinstance(e, dict) and e.get("label")]

    stance_targets = [s.get("target") for s in stance if isinstance(s, dict) and s.get("target")]
    stance_labels = [s.get("label") for s in stance if isinstance(s, dict) and s.get("label")]

    claim_kinds = [c.get("kind") for c in claims if isinstance(c, dict) and c.get("kind")]

    doc = {
        "id": record.get("id"),
        "source_url": record.get("source_url"),
        "source_type": record.get("source_type"),
        "title": record.get("title"),
        "text": record.get("text"),  # may be None if not passed through by enricher
        "lang": record.get("lang"),
        "published_at": record.get("published_at"),
        "entities": entities,
        "ent_names": ent_names,
        "ent_labels": ent_labels,
        "sentiment_label": sentiment.get("label"),
        "sentiment_compound": sentiment.get("compound"),
        "stance": stance,
        "stance_targets": stance_targets,
        "stance_labels": stance_labels,
        "claims": claims,
        "claim_kinds": claim_kinds,
    }
    return doc

def to_bulk_actions(batch: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
    for r in batch:
        doc = transform(r)
        if not doc.get("id"):
            continue  # skip malformed
        yield {
            "_op_type": "index",        # upsert-like
            "_index": ES_ALIAS,         # write via alias → current version
            "_id": doc["id"],           # stable id
            "_source": doc,
        }

def flush_bulk(es: Elasticsearch, actions: Iterable[Dict[str, Any]]) -> int:
    success, errors = bulk(es, actions, request_timeout=30, refresh=False)
    if errors:
        print(f"[indexer] bulk errors: {errors}", file=sys.stderr, flush=True)
    return success

def main():
    print(f"[indexer] starting; bootstrap={BOOTSTRAP} in={IN_TOPIC} es={ES_URL} -> {ES_ALIAS}", flush=True)
    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)

    es = get_es()
    ensure_index(es)
    consumer = get_consumer()

    batch: List[Dict[str, Any]] = []
    last = time.time()

    while _running:
        polled = consumer.poll(timeout_ms=1000)
        if polled:
            for tp, messages in polled.items():
                for msg in messages:
                    batch.append(msg.value)
                    if len(batch) >= BATCH_MAX_DOCS or (time.time() - last) >= BATCH_FLUSH_SEC:
                        actions = list(to_bulk_actions(batch))
                        if actions:
                            n = flush_bulk(es, actions)
                            consumer.commit()
                            print(f"[indexer] indexed={n} commit ok", flush=True)
                        batch.clear()
                        last = time.time()
        else:
            # idle flush
            if batch and (time.time() - last) >= BATCH_FLUSH_SEC:
                actions = list(to_bulk_actions(batch))
                if actions:
                    n = flush_bulk(es, actions)
                    consumer.commit()
                    print(f"[indexer] indexed={n} commit ok", flush=True)
                batch.clear()
                last = time.time()

    # graceful shutdown flush
    if batch:
        actions = list(to_bulk_actions(batch))
        if actions:
            n = flush_bulk(es, actions)
            consumer.commit()
            print(f"[indexer] indexed={n} commit ok", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[indexer] FATAL:", repr(e), file=sys.stderr, flush=True)
        sys.exit(1)
