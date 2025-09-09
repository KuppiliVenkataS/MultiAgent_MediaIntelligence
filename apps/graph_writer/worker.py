import json, os, time, signal, sys, hashlib
from typing import Any, Dict, List, Iterable
import backoff
from kafka import KafkaConsumer
from neo4j import GraphDatabase, basic_auth

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
IN_TOPIC  = os.getenv("ENRICHED_TOPIC", "enriched-documents")
GROUP_ID  = os.getenv("GROUP_ID", "graph-writer.v1")

NEO4J_URI  = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASS = os.getenv("NEO4J_PASS", "testpassword")

BATCH_MAX_DOCS = int(os.getenv("BATCH_MAX_DOCS", "200"))
BATCH_FLUSH_SEC = float(os.getenv("BATCH_FLUSH_SEC", "3"))

_running = True
def _graceful(*_):
    global _running
    _running = False
    print("[graph] shutdown signal", flush=True)

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
def get_driver():
    drv = GraphDatabase.driver(NEO4J_URI, auth=basic_auth(NEO4J_USER, NEO4J_PASS))
    with drv.session() as s:
        s.run("RETURN 1").consume()
    return drv

def ensure_constraints(driver):
    cy = """
    CREATE CONSTRAINT article_id IF NOT EXISTS FOR (a:Article) REQUIRE a.id IS UNIQUE;
    CREATE CONSTRAINT entity_key  IF NOT EXISTS FOR (e:Entity)  REQUIRE (e.name, e.label) IS UNIQUE;
    CREATE CONSTRAINT claim_id    IF NOT EXISTS FOR (c:Claim)   REQUIRE c.id IS UNIQUE;
    """
    with driver.session() as s:
        for stmt in cy.strip().split(";"):
            if stmt.strip():
                s.run(stmt).consume()
    print("[graph] constraints ensured", flush=True)

def _sha1(text: str) -> str:
    return hashlib.sha1(text.encode("utf-8")).hexdigest()

def _as_list(v):
    if v is None: return []
    return v if isinstance(v, list) else [v]

def transform(record: Dict[str, Any]) -> Dict[str, Any]:
    """Extract upsert payloads from an enriched doc."""
    article = {
        "id": record.get("id"),
        "title": record.get("title"),
        "source_url": record.get("source_url"),
        "source_type": record.get("source_type"),
        "lang": record.get("lang"),
        "published_at": record.get("published_at"),
        "sentiment_label": (record.get("sentiment") or {}).get("label"),
        "sentiment_compound": (record.get("sentiment") or {}).get("compound"),
    }

    ents = []
    for e in _as_list(record.get("entities")):
        if isinstance(e, dict) and e.get("text") and e.get("label"):
            ents.append({"name": e["text"], "label": e["label"]})

    mentions = [{"article_id": article["id"], "name": e["name"], "label": e["label"]} for e in ents]

    stances = []
    for s in _as_list(record.get("stance")):
        if isinstance(s, dict) and s.get("target") and s.get("label"):
            stances.append({
                "article_id": article["id"],
                "name": s["target"],
                "label": "ORG",  # heuristic; entity label unknown here
                "stance": s["label"],
                "confidence": float(s.get("confidence") or 0.0),
            })

    claims = []
    for c in _as_list(record.get("claims")):
        if isinstance(c, dict) and c.get("text"):
            claims.append({
                "id": _sha1(c["text"]),
                "article_id": article["id"],
                "text": c["text"],
                "kind": c.get("kind", "other"),
                "confidence": float(c.get("confidence") or 0.0),
            })

    return {"article": article, "entities": ents, "mentions": mentions, "stances": stances, "claims": claims}

def to_batches(items: List[Dict[str, Any]], key: str) -> List[Dict[str, Any]]:
    return [x[key] for x in items if x.get(key)]

def upsert_batch(driver, batch: List[Dict[str, Any]]):
    """Run a few compact UNWIND queries in one transaction for the batch."""
    arts   = [{"id": r["article"]["id"], "props": r["article"]} for r in batch if r.get("article") and r["article"].get("id")]
    ents   = [e for r in batch for e in r.get("entities", [])]
    ments  = [m for r in batch for m in r.get("mentions", [])]
    stcs   = [s for r in batch for s in r.get("stances", [])]
    clms   = [c for r in batch for c in r.get("claims", [])]

    with driver.session() as s:
        tx = s.begin_transaction()

        if arts:
            tx.run("""
                UNWIND $rows AS row
                MERGE (a:Article {id: row.id})
                SET a += row.props
            """, rows=arts).consume()

        if ents:
            tx.run("""
                UNWIND $rows AS row
                MERGE (e:Entity {name: row.name, label: row.label})
            """, rows=ents).consume()

        if ments:
            tx.run("""
                UNWIND $rows AS row
                MATCH (a:Article {id: row.article_id})
                MATCH (e:Entity  {name: row.name, label: row.label})
                MERGE (a)-[r:MENTIONS]->(e)
                ON CREATE SET r.count = 1
                ON MATCH  SET r.count = coalesce(r.count,0) + 1
            """, rows=ments).consume()

        if stcs:
            tx.run("""
                UNWIND $rows AS row
                MATCH (a:Article {id: row.article_id})
                MATCH (e:Entity  {name: row.name})
                MERGE (a)-[r:STANCE]->(e)
                SET r.label = row.stance, r.confidence = row.confidence
            """, rows=stcs).consume()

        if clms:
            tx.run("""
                UNWIND $rows AS row
                MERGE (c:Claim {id: row.id})
                SET c.text = row.text, c.kind = row.kind, c.confidence = row.confidence
                WITH row, c
                MATCH (a:Article {id: row.article_id})
                MERGE (a)-[:CLAIMS]->(c)
            """, rows=clms).consume()

        tx.commit()

def main():
    print(f"[graph] starting; in={IN_TOPIC} -> neo4j={NEO4J_URI}", flush=True)
    signal.signal(signal.SIGINT, _graceful)
    signal.signal(signal.SIGTERM, _graceful)

    driver = get_driver()
    ensure_constraints(driver)
    consumer = get_consumer()

    batch, last = [], time.time()
    while _running:
        polled = consumer.poll(timeout_ms=1000)
        if polled:
            for _, messages in polled.items():
                for msg in messages:
                    batch.append(transform(msg.value))
                    if len(batch) >= BATCH_MAX_DOCS or (time.time() - last) >= BATCH_FLUSH_SEC:
                        upsert_batch(driver, batch)
                        consumer.commit()
                        print(f"[graph] upserted={len(batch)} commit ok", flush=True)
                        batch.clear(); last = time.time()
        else:
            if batch and (time.time() - last) >= BATCH_FLUSH_SEC:
                upsert_batch(driver, batch)
                consumer.commit()
                print(f"[graph] upserted={len(batch)} commit ok", flush=True)
                batch.clear(); last = time.time()

    if batch:
        upsert_batch(driver, batch)
        consumer.commit()
        print(f"[graph] upserted={len(batch)} commit ok", flush=True)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("[graph] FATAL:", repr(e), file=sys.stderr, flush=True)
        sys.exit(1)
