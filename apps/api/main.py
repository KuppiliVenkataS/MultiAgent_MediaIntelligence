# apps/api/main.py
from typing import List, Optional, Union, Dict, Any, Mapping
from fastapi import FastAPI
from pydantic import BaseModel, HttpUrl, Field
import re
from urllib.parse import quote_plus

app = FastAPI(title="Agentic Media Intelligence API")

# ---------- Models ----------

class BriefSource(BaseModel):
    url: HttpUrl
    title: Optional[str] = None

class BriefRequest(BaseModel):
    topic: str = Field(..., description="Subject to brief, e.g., 'Apple in London'")
    entities: List[str] = Field(default_factory=list)
    timeframe: Optional[str] = None
    max_bullets: int = 5
    min_citations: int = 3

class BriefResponse(BaseModel):
    topic: str
    bullets: List[str]
    citations: List[BriefSource] = Field(default_factory=list)
    # keep 'sources' as an alias/mirror for compatibility
    sources: List[BriefSource] = Field(default_factory=list)

# ---------- Coercion Helper (fixes string/dict/kwargs inputs) ----------

def _coerce_brief_request(
    payload: Union[BriefRequest, Mapping[str, Any], str, None] = None, **kwargs
) -> BriefRequest:
    # Already a BriefRequest
    if isinstance(payload, BriefRequest):
        if kwargs:
            data = payload.model_dump()
            data.update(kwargs)
            return BriefRequest(**data)
        return payload

    # A string -> treat as topic
    if isinstance(payload, str):
        kwargs.setdefault("topic", payload)
        return BriefRequest(**kwargs)

    # A mapping/dict
    if isinstance(payload, Mapping):
        data = dict(payload)
        data.update(kwargs)
        return BriefRequest(**data)

    # Nothing passed; rely on kwargs only
    if payload is None:
        return BriefRequest(**kwargs)

    # Pydantic/v2 object with model_dump or v1-style dict()
    if hasattr(payload, "model_dump"):
        data = payload.model_dump()
        data.update(kwargs)
        return BriefRequest(**data)
    if hasattr(payload, "dict"):
        data = payload.dict()
        data.update(kwargs)
        return BriefRequest(**data)

    raise TypeError(f"Unsupported payload type: {type(payload)!r}")

# ---------- Implementation Stub ----------
def _stub_citations(req: BriefRequest) -> list[BriefSource]:
    """Create deterministic, valid URLs from the topic so tests have >=2 items."""
    topic = (req.topic or "news").strip()
    q = quote_plus(topic)
    slug = re.sub(r"\W+", "_", topic).strip("_") or "News"
    base = [
        BriefSource(url=f"https://news.google.com/search?q={q}", title=f"{topic} — News search"),
        BriefSource(url=f"https://en.wikipedia.org/wiki/{slug}", title=f"{topic} — Wikipedia"),
        # add a couple more to satisfy larger min_citations without duplication
        BriefSource(url=f"https://www.bbc.co.uk/search?q={q}", title=f"{topic} — BBC search"),
        BriefSource(url=f"https://www.reuters.com/site-search/?query={q}", title=f"{topic} — Reuters search"),
    ]
    # ensure length >= max(2, req.min_citations)
    need = max(2, req.min_citations)
    return (base * ((need + len(base) - 1) // len(base)))[:need]



def create_brief(
    payload: Union[BriefRequest, Mapping[str, Any], str, None] = None, **kwargs
) -> Dict[str, Any]:
    req = _coerce_brief_request(payload, **kwargs)

    preface = f"Brief on: {req.topic}"
    scope = []
    if req.entities:
        scope.append(f"focus: {', '.join(req.entities)}")
    if req.timeframe:
        scope.append(f"timeframe: {req.timeframe}")
    if scope:
        preface += f" ({'; '.join(scope)})"

    bullets = [
        preface,
        "This is a contract stub; enrichment & retrieval populate real bullets later.",
        f"Target length: ≤ {req.max_bullets} bullets; min citations: {req.min_citations}.",
    ][: max(1, min(req.max_bullets, 5))]

    citations = _stub_citations(req)

    resp = BriefResponse(
        topic=req.topic,
        bullets=bullets,
        citations=citations,
        sources=citations,   # mirror for compatibility
    )
    return resp.model_dump(mode="json")

# ---------- FastAPI Endpoint ----------

@app.post("/brief", response_model=BriefResponse)
def brief_endpoint(req: BriefRequest) -> BriefResponse:
    return BriefResponse(**create_brief(req))

__all__ = ["create_brief", "BriefRequest", "BriefResponse", "BriefSource", "app"]
