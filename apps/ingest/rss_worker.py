# feedparser pulls articles → we canonicalize + extract full text → create Document → publish JSON to Kafka topic raw.documents.

import time, feedparser, os
from typing import List, Iterable, Dict, Any
from .models import Document
from .normalizer import make_doc_id, extract_text, detect_lang, parse_published_dt
from .producer import get_producer, wait_for_broker, ensure_topic, BOOTSTRAP



# Start with 2 public feeds; add more later
FEEDS: List[str] = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.theverge.com/rss/index.xml",
]

TOPIC = os.getenv("RAW_TOPIC", "raw.documents")

def iter_feed_entries() -> Iterable[Dict[str, Any]]:
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
        prod.send(TOPIC, doc.model_dump(mode="json"))
        sent += 1
        if limit and sent >= limit:
            break
    prod.flush(5)
    return sent

if __name__ == "__main__":
    print("[ingest] rss_worker starting...")
    while True:
        try:
            n = run_once(limit=None)
            print(f"[ingest] published {n} docs")
        except Exception as e:
            print("[ingest] ERROR:", e, flush=True)
        time.sleep(300)  # every 5 minutes
