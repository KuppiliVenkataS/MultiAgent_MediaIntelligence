# feedparser pulls articles → we canonicalize + extract full text → create Document → publish JSON to Kafka topic raw-documents.

import time, feedparser, os, redis
from typing import List, Iterable, Dict, Any
from .models import Document
from .normalizer import make_doc_id, extract_text, detect_lang, parse_published_dt
from .producer import get_producer, wait_for_broker, ensure_topic, BOOTSTRAP

import os
USE_STUB = os.getenv("USE_STUB_FEEDS","0") == "1"
INTERVAL = int(os.getenv("INGEST_INTERVAL_SEC", "300"))  # default 5 min
RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"

# Start with 2 public feeds; add more later
FEEDS: List[str] = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.theverge.com/rss/index.xml",
]
R = redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"))
DEDUP_TTL = 7*24*3600  # 7 days

TOPIC = os.getenv("RAW_TOPIC", "raw-documents")

def iter_feed_entries() -> Iterable[Dict[str, Any]]:
    
    if USE_STUB:
        yield {"link":"https://example.com/a","title":"Stub A","summary":"Alpha","published":"2024-01-01"}
        yield {"link":"https://example.com/b","title":"Stub B","summary":"Beta","published":"2024-01-02"}
        return

    for url in FEEDS:
        feed = feedparser.parse(url)
        for e in feed.entries:
            yield {
                "link": e.get("link"),
                "title": e.get("title"),
                "summary": e.get("summary", ""),
                "published": e.get("published", ""),
            }

def run_once(limit:int|None=None) -> int:
    # Wait for broker, ensure topic
    if not wait_for_broker(timeout=90):
        print("[ingest] Kafka not ready after 90s")
        return 0
    ensure_topic(TOPIC, partitions=3, replication=1)

    prod = get_producer()
    sent = 0
    for i, e in enumerate(iter_feed_entries()):
        if not e.get("link"):
            continue
        text = extract_text(e["link"], fallback_html=e.get("summary",""))
        lang = detect_lang(text) or "en"
        published_dt = parse_published_dt(e.get("published"))

        doc = Document(
            id=make_doc_id(e["link"], e.get("published")),
            source_url=e["link"],
            source_type="rss",
            title=e.get("title"),
            text=text,
            lang=lang,
            published_at=published_dt,         # <-- set it (or None)
        )
        
        # skip if we've seen this doc.id recently
        if not R.setnx(f"seen:{doc.id}", 1):
            continue
        R.expire(f"seen:{doc.id}", DEDUP_TTL)

        
        # prod.send(TOPIC, doc.model_dump(mode="json"))
        future = prod.send(TOPIC, doc.model_dump(mode="json"))
        future.add_callback(lambda md: print(f"[ingest] sent {doc.id} to {md.topic}:{md.partition}@{md.offset}", flush=True))
        future.add_errback(lambda exc: print(f"[ingest] send error for {doc.id}: {exc!r}", flush=True))
        
        sent += 1
        if limit and sent >= limit:
            break
    prod.flush(30)
    return sent

if __name__ == "__main__":
    print("[ingest] rss_worker starting...")
    try:
        n = run_once(limit=None)
        print(f"[ingest] published {n} docs")
    except Exception as e:
        print("[ingest] ERROR:", e, flush=True)

    if RUN_ONCE:
        print("[ingest] RUN_ONCE=1 -> exiting")
    else:
        while True:
            try:
                time.sleep(INTERVAL)
                n = run_once(limit=None)
                print(f"[ingest] published {n} docs")
            except Exception as e:
                print("[ingest] ERROR:", e, flush=True)

