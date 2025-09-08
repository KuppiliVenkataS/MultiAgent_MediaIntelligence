# Pydantic models for the enricher’s input/output contract.

# We keep the schema additive and JSON-friendly so downstream consumers (indexers,
# API, analytics) can evolve independently. These models intentionally mirror a
# subset of the ingest document plus new enrichment fields.
from pydantic import BaseModel, Field
from typing import List, Optional, Literal, Dict


class Entity(BaseModel):
    # A named entity span extracted from text.
    # start/end are character offsets in the *original* document text, so UIs can
    # highlight spans without re-running NLP.
    text: str
    label: Literal["PERSON","ORG","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","LAW","LANGUAGE","NORP","FAC"]
    start: int
    end: int

class Sentiment(BaseModel):
    # Document-level sentiment (VADER polarity).
    # - label: a coarse bucket for quick filtering.
    # - compound: continuous polarity in [-1, 1] for ranking/thresholding.
    
    label: Literal["neg","neu","pos"]
    compound: float

class Stance(BaseModel):
    # Attitude toward a *target* (usually an entity) inferred from local context.
    # We expose a confidence so you can gate actions (e.g., only alert if > 0.7).
    
    target: str               # usually an entity text
    label: Literal["pro","con","neutral","unknown"]
    confidence: float = 0.0

class Claim(BaseModel):
    # A pattern-mined 'claim' sentence. We attach:
    # - kind: coarse type for routing (event/forecast/commitment/metric/uncertain).
    # - cues: which regex patterns fired (for explainability).
    # - confidence: naive score based on cues count (tunable later).
    
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
