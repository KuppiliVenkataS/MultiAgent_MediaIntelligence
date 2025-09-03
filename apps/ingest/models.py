# Document Schema (pydantic)
from pydantic import BaseModel, HttpUrl
from typing import List, Optional
from datetime import datetime


class Document(BaseModel):
    id: str # deterministic hash of source_url + published_at
    source_url: HttpUrl
    source_type: str # rss, web, youtube, podcast, pdf, image
    title: Optional[str]
    text: Optional[str]
    lang: Optional[str]
    published_at: Optional[datetime]
    authors: List[str] = []
    media: List[str] = [] # image/video urls
    raw: dict = {} # raw payload