# What this piece of code does: 
# everything downstream (enrichment, retrieval, agents) consumes this one schema.

# Document Schema (pydantic)
from pydantic import BaseModel, HttpUrl, Field, field_serializer
from typing import List, Optional
from datetime import datetime


class Document(BaseModel):
    id: str # deterministic hash of source_url + published_at
    source_url: HttpUrl
    source_type: str # rss, web, youtube, podcast, pdf, image
    title: Optional[str]
    text: Optional[str]
    lang: Optional[str]
    published_at: Optional[datetime] = None
    authors: List[str] = Field(default_factory=list)
    media: List[str] = Field(default_factory=list)
    raw: dict = Field(default_factory=dict)
    @field_serializer("source_url")
    def _ser_url(self, v: HttpUrl):
        return str(v)