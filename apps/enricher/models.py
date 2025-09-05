from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict

class Entity(BaseModel):
    text: str
    label: Literal["PERSON","ORG","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","LAW","LANGUAGE","NORP","FAC"]
    start: int
    end: int

class EnrichedDocument(BaseModel):
    # Carry original doc id + a minimal echo of source fields for convenience
    id: str
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    title: Optional[str] = None
    lang: Optional[str] = None

    # Enrichment payload
    entities: List[Entity] = Field(default_factory=list)

    # Freeform extras for later modules (sentiment, stance, claims, etc.)
    extra: Dict[str, str] = Field(default_factory=dict)
