import os
from typing import Any, Dict, List
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from elasticsearch import NotFoundError
from .clients import es_client, get_neo4j
from .schemas import SearchRequest, SearchResponse, SearchHit, FacetsResponse, RelatedEntitiesResponse

ES_ALIAS = os.getenv("ES_INDEX_ALIAS", "news-enriched")

app = FastAPI(title="Agentic Newsroom Search API", version="0.1.0")

# Dev CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

@app.get("/health")
def health():
    info = {}
    try:
        info["elasticsearch"] = es_client().info().body.get("version", {})
    except Exception as e:
        info["elasticsearch_error"] = repr(e)
    try:
        drv = get_neo4j()
        if drv:
            with drv.session() as s:
                res = s.run("CALL dbms.components() YIELD name, versions RETURN name, versions LIMIT 1").data()
            info["neo4j"] = res[0] if res else {"ok": True}
        else:
            info["neo4j"] = {"disabled": True}
    except Exception as e:
        info["neo4j_error"] = repr(e)
    return {"ok": True, "info": info}

def _build_query(payload: SearchRequest) -> Dict[str, Any]:
    must: List[Dict[str, Any]] = []
    filt: List[Dict[str, Any]] = []

    if payload.q:
        must.append({
            "simple_query_string": {
                "query": payload.q,
                "fields": ["title^3", "text"],
                "default_operator": "and"
            }
        })

    def _terms(field: str, values: List[str]):
        if values:
            filt.append({"terms": {field: values}})

    _terms("ent_names", payload.ent_names)
    _terms("ent_labels", payload.ent_labels)
    _terms("sentiment_label", payload.sentiment_labels)
    _terms("stance_targets", payload.stance_targets)
    _terms("stance_labels", payload.stance_labels)
    _terms("claim_kinds", payload.claim_kinds)
    _terms("lang", payload.lang)

    if payload.date_from or payload.date_to:
        rng: Dict[str, Any] = {}
        if payload.date_from: rng["gte"] = payload.date_from
        if payload.date_to:   rng["lte"] = payload.date_to
        filt.append({"range": {"published_at": rng}})

    if not must:
        must = [{"match_all": {}}]

    sort = []
    if payload.sort == "relevance":
        sort = ["_score"]
    elif payload.sort == "published_at.desc":
        sort = [{"published_at": {"order": "desc", "unmapped_type": "date"}}]
    elif payload.sort == "published_at.asc":
        sort = [{"published_at": {"order": "asc", "unmapped_type": "date"}}]

    body = {
        "from": max(0, (payload.page - 1) * payload.size),
        "size": max(1, min(payload.size, 100)),
        "track_total_hits": True,
        "query": {"bool": {"must": must, "filter": filt}},
        "highlight": {
            "pre_tags": ["<mark>"], "post_tags": ["</mark>"],
            "fields": {"title": {}, "text": {}}
        },
        "_source": [
            "id","title","source_url","published_at",
            "ent_names","sentiment_label","claim_kinds"
        ],
        "sort": sort
    }
    return body

@app.post("/search", response_model=SearchResponse)
def search(payload: SearchRequest):
    try:
        body = _build_query(payload)
        res = es_client().search(index=ES_ALIAS, body=body)
        total = int(res["hits"]["total"]["value"]) if isinstance(res["hits"]["total"], dict) else int(res["hits"]["total"])
        hits: List[SearchHit] = []
        for h in res["hits"]["hits"]:
            src = h.get("_source", {})
            hits.append(SearchHit(
                id=src.get("id") or h.get("_id"),
                title=src.get("title"),
                source_url=src.get("source_url"),
                published_at=src.get("published_at"),
                ent_names=src.get("ent_names", []),
                sentiment_label=src.get("sentiment_label"),
                claim_kinds=src.get("claim_kinds", []),
                score=h.get("_score"),
                highlight={k: v for k, v in (h.get("highlight") or {}).items()}
            ))
        return SearchResponse(total=total, page=payload.page, size=payload.size, hits=hits)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Index/alias {ES_ALIAS} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.post("/facets", response_model=FacetsResponse)
def facets(payload: SearchRequest):
    # reuse filters; just change size=0 and add aggs
    body = _build_query(payload)
    body["size"] = 0
    body["aggs"] = {
        "entities": {"terms": {"field": "ent_names", "size": 15}},
        "claim_kinds": {"terms": {"field": "claim_kinds", "size": 10}},
        "sentiments": {"terms": {"field": "sentiment_label", "size": 5}},
        "stance_labels": {"terms": {"field": "stance_labels", "size": 5}},
    }
    try:
        res = es_client().search(index=ES_ALIAS, body=body)
        def _b(name): 
            return [{"key": b["key"], "count": b["doc_count"]} for b in res["aggregations"][name]["buckets"]]
        return FacetsResponse(
            entities=_b("entities"),
            claim_kinds=_b("claim_kinds"),
            sentiments=_b("sentiments"),
            stance_labels=_b("stance_labels"),
        )
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Index/alias {ES_ALIAS} not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))

@app.get("/entity/{name}/related", response_model=RelatedEntitiesResponse)
def related_entities(name: str, limit: int = 10):
    drv = get_neo4j()
    if not drv:
        raise HTTPException(status_code=503, detail="Neo4j disabled or unavailable")
    cy = """
    MATCH (a:Article)-[:MENTIONS]->(e:Entity {name: $name})
    MATCH (a)-[:MENTIONS]->(o:Entity)
    WHERE o.name <> $name
    RETURN o.name AS name, count(*) AS c
    ORDER BY c DESC
    LIMIT $limit
    """
    try:
        with drv.session() as s:
            rows = s.run(cy, name=name, limit=limit).data()
        return RelatedEntitiesResponse(entity=name, related=[{"name": r["name"], "count": r["c"]} for r in rows])
    except Exception as e:
        raise HTTPException(status_code=500, detail=repr(e))
