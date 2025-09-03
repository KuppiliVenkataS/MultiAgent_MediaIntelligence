# Helps with canonical URL, deterministic ID, article text, language
import hashlib
from urllib.parse import urlsplit, urlunsplit
from datetime import datetime
from typing import Optional
from email.utils import parsedate_to_datetime

from langdetect import detect, LangDetectException
import trafilatura

def canonical_url(url: str) -> str:
    """Drop query/fragment to make dedupe more reliable."""
    parts = urlsplit(url)
    return urlunsplit((parts.scheme, parts.netloc, parts.path, '', ''))

def make_doc_id(url: str, published_at: Optional[str|datetime] = None) -> str:
    """Stable ID from canonical URL + published string."""
    base = canonical_url(url) + '|' + (published_at.isoformat() if isinstance(published_at, datetime) else str(published_at or ''))
    return hashlib.sha1(base.encode()).hexdigest()

def extract_text(url: str, fallback_html: str|None = None) -> str:
    """Try full-page extraction; fall back to RSS summary."""
    downloaded = trafilatura.fetch_url(url)
    if downloaded:
        text = trafilatura.extract(downloaded, include_comments=False, include_tables=False) or ''
        return text.strip()
    return (fallback_html or '').strip()

def detect_lang(text: str) -> str|None:
    """Language ID (returns None when uncertain)."""
    if not text or len(text) < 20:
        return None
    try:
        return detect(text)
    except LangDetectException:
        return None
    
def parse_published_dt(published: Optional[str]) -> Optional[datetime]:
    if not published:
        return None
    try:
        return parsedate_to_datetime(published)
    except Exception:
        return None
