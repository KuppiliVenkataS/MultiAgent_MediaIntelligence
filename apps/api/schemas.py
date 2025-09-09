from typing import List, Optional, Literal, Dict, Any
from pydantic import BaseModel, Field

SortKey = Literal["relevance", "published_at.desc", "published_at.asc"]

class SearchRequest(BaseModel):
    q: Optional[str] = None
    ent_names: List[str] = Field(default_factory=list)
    ent_labels: List[str] = Field(default_factory=list)
    sentiment_labels: List[str] = Field(default_factory=list)  # ["pos","neu","neg"]
    stance_targets: List[str] = Field(default_factory=list)
    stance_labels: List[str] = Field(default_factory=list)     # ["pro","con","neutral","unknown"]
    claim_kinds: List[str] = Field(default_factory=list)       # ["event","commitment",...]
    lang: List[str] = Field(default_factory=list)
    date_from: Optional[str] = None   # ISO8601 (e.g., "2024-01-01")
    date_to: Optional[str] = None
    page: int = 1
    size: int = 10
    sort: SortKey = "relevance"

class SearchHit(BaseModel):
    id: str
    title: Optional[str] = None
    source_url: Optional[str] = None
    published_at: Optional[str] = None
    ent_names: List[str] = Field(default_factory=list)
    sentiment_label: Optional[str] = None
    claim_kinds: List[str] = Field(default_factory=list)
    score: Optional[float] = None
    highlight: Dict[str, List[str]] = Field(default_factory=dict)

class SearchResponse(BaseModel):
    total: int
    page: int
    size: int
    hits: List[SearchHit] = Field(default_factory=list)

class FacetsResponse(BaseModel):
    entities: List[Dict[str, Any]] = Field(default_factory=list)
    claim_kinds: List[Dict[str, Any]] = Field(default_factory=list)
    sentiments: List[Dict[str, Any]] = Field(default_factory=list)
    stance_labels: List[Dict[str, Any]] = Field(default_factory=list)

class RelatedEntitiesResponse(BaseModel):
    entity: str
    related: List[Dict[str, Any]]  # [{"name": "...", "count": 42}]
