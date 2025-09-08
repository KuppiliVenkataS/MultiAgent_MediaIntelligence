from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict


class Entity(BaseModel):
    text: str
    label: Literal["PERSON","ORG","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","LAW","LANGUAGE","NORP","FAC"]
    start: int
    end: int

class Sentiment(BaseModel):
    label: Literal["neg","neu","pos"]
    compound: float

class Stance(BaseModel):
    target: str               # usually an entity text
    label: Literal["pro","con","neutral","unknown"]
    confidence: float = 0.0

class Claim(BaseModel):
    text: str                 # the sentence/snippet
    kind: Literal["event","forecast","commitment","metric","uncertain","other"]
    cues: List[str] = Field(default_factory=list)
    confidence: float = 0.5

class EnrichedDocument(BaseModel):
    # Carry original doc id + a minimal echo of source fields for convenience
    id: str
    source_url: Optional[str] = None
    source_type: Optional[str] = None
    title: Optional[str] = None
    lang: Optional[str] = None

    # Enrichment payload
    entities: List[Entity] = Field(default_factory=list)
    extra: Dict[str, str] = Field(default_factory=dict)

    # NEW
    sentiment: Optional[Sentiment] = None
    stance: List[Stance] = Field(default_factory=list)
    claims: List[Claim] = Field(default_factory=list)
