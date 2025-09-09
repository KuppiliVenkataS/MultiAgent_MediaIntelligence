# feedparser pulls articles → we canonicalize + extract full text → create Document → publish JSON to Kafka topic raw-documents.

import time, feedparser, os, redis
from typing import List, Iterable, Dict, Any
import  hashlib, backoff, requests
from dateutil import parser as dtp
from datetime import datetime, timezone
from .models import Document
from .normalizer import make_doc_id, extract_text, detect_lang, parse_published_dt
from .producer import get_producer, wait_for_broker, ensure_topic, BOOTSTRAP

import os
USE_STUB = os.getenv("USE_STUB_FEEDS","0") == "1"
INTERVAL = int(os.getenv("INGEST_INTERVAL_SEC", "300"))  # default 5 min
RUN_ONCE = os.getenv("RUN_ONCE", "0") == "1"
UA = os.getenv("HTTP_USER_AGENT", "Mozilla/5.0 (AgenticNewsroom/1.0; +https://example.com)")
TIMEOUT = (5, 15)  # connect, read seconds


# Start with 2 public feeds; add more later
FEEDS: List[str] = [
    "https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
    "https://www.theverge.com/rss/index.xml",
]
# R = redis.from_url(os.getenv("REDIS_URL","redis://redis:6379/0"))
DEDUP_TTL = 7*24*3600  # 7 days

TOPIC = os.getenv("RAW_TOPIC", "raw-documents")

def get_redis(max_wait_sec: int = 60):
    url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    t0 = time.time()
    while time.time() - t0 < max_wait_sec:
        try:
            r = redis.from_url(url, socket_connect_timeout=2, socket_timeout=2)
            r.ping()
            return r
        except Exception as e:
            print(f"[ingest] Redis not ready ({e}); retrying...", flush=True)
            time.sleep(2)
    raise RuntimeError("Redis not reachable")


R = None
try:
    R = get_redis()
except Exception as e:
    print("[ingest] WARNING: continuing without Redis dedupe:", e, flush=True)

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

def load_feeds():
    if os.getenv("RSS_FEEDS"):
        return [u.strip() for u in os.getenv("RSS_FEEDS").split(",") if u.strip()]
    
    path = os.getenv("RSS_FEEDS_FILE")
    # path = 'feeds/feeds.txt'
    if path and os.path.exists(path):
        return [ln.strip() for ln in open(path) if ln.strip() and not ln.startswith("#")]
    return []

@backoff.on_exception(backoff.expo, (requests.RequestException,), max_time=30)
def fetch_url(url: str) -> bytes:
    r = requests.get(url, headers={"User-Agent": UA}, timeout=TIMEOUT)
    r.raise_for_status()
    return r.content

def parse_entry(feed_url: str, e) -> Document | None:
    # prefer link+title+summary/content
    link = getattr(e, "link", None) or getattr(e, "id", None)
    title = getattr(e, "title", "") or ""
    text = getattr(e, "summary", "") or ""
    if not text and getattr(e, "content", None):
        try:
            text = e.content[0].value
        except Exception:
            pass

    # published_at
    published_at = None
    for cand in ("published", "updated"):
        val = getattr(e, cand, None)
        if val:
            try:
                published_at = dtp.parse(val).astimezone(timezone.utc).isoformat()
                break
            except Exception:
                pass
    if not published_at:
        published_at = datetime.now(timezone.utc).isoformat()

    if not link or not (title or text):
        return None

    # stable id by link
    did = hashlib.sha1(link.encode("utf-8")).hexdigest()

    return Document(
        id=did,
        source_url=link,
        source_type="rss",
        title=title.strip()[:500],
        text=(text or "").strip(),
        lang="en",  # let enricher gate non-en later if you detect language upstream
        published_at=published_at,
        authors=[],
        media=[],
        raw={"feed": feed_url},
    )

def run_once():
    feeds = load_feeds()
    if not feeds:
        print("[ingest] no feeds configured", flush=True); return 0

    prod = get_producer()
    sent = 0
    for u in feeds:
        try:
            blob = fetch_url(u)
            fp = feedparser.parse(blob)
            print(f"[ingest] {u} entries={len(fp.entries)}", flush=True)
            for e in fp.entries:
                doc = parse_entry(u, e)
                if not doc: continue
                # optional Redis de-dupe
                try:
                    #from .redis_client import R
                    k = f"seen:{doc.id}"
                    if R and not R.setnx(k, 1):
                        continue
                    if R: R.expire(k, 7*24*3600)
                except Exception:
                    pass
                prod.send(os.getenv("RAW_TOPIC","raw-documents"), doc.model_dump(mode="json")).get(timeout=10)
                sent += 1
        except Exception as ex:
            print(f"[ingest] ERROR feed {u}: {ex}", flush=True)
    print(f"[ingest] sent total={sent}", flush=True)
    return sent



def run_once_old(limit:int|None=None) -> int:
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
    print("[ingest] starting real feeds…", flush=True)
    if os.getenv("RUN_ONCE","0") == "1":
        run_once()
    else:
        interval = int(os.getenv("INGEST_INTERVAL_SEC","600"))
        while True:
            run_once()
            time.sleep(interval)


    # try:
    #     n = run_once(limit=None)
    #     print(f"[ingest] published {n} docs")
    # except Exception as e:
    #     print("[ingest] ERROR:", e, flush=True)

    # if RUN_ONCE:
    #     print("[ingest] RUN_ONCE=1 -> exiting")
    # else:
    #     while True:
    #         try:
    #             time.sleep(INTERVAL)
    #             n = run_once(limit=None)
    #             print(f"[ingest] published {n} docs")
    #         except Exception as e:
    #             print("[ingest] ERROR:", e, flush=True)

