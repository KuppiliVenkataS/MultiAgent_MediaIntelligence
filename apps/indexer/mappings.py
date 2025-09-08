ALIAS = "news-enriched"
INDEX_V1 = "news-enriched-v1"

SETTINGS = {
    "settings": {
        "number_of_shards": int(__import__("os").getenv("ES_SHARDS", "1")),
        "number_of_replicas": int(__import__("os").getenv("ES_REPLICAS", "0")),
        "refresh_interval": __import__("os").getenv("ES_REFRESH", "1s"),
        "analysis": {
            "analyzer": {
                "english_folded": {
                    "type": "custom",
                    "tokenizer": "standard",
                    "filter": ["lowercase", "asciifolding", "stop"]
                }
            }
        }
    },
    "mappings": {
        "dynamic": False,
        "properties": {
            "id": {"type": "keyword"},
            "source_url": {"type": "keyword"},
            "source_type": {"type": "keyword"},
            "title": {
                "type": "text",
                "analyzer": "english_folded",
                "fields": {"raw": {"type": "keyword"}}
            },
            "text": {  # optional; only present if your enricher passes it through
                "type": "text",
                "analyzer": "english_folded"
            },
            "lang": {"type": "keyword"},
            "published_at": {"type": "date"},  # ISO8601 or epoch_millis

            # flattened facets
            "ent_names": {"type": "keyword"},
            "ent_labels": {"type": "keyword"},
            "sentiment_label": {"type": "keyword"},
            "sentiment_compound": {"type": "float"},
            "stance_targets": {"type": "keyword"},
            "stance_labels": {"type": "keyword"},
            "claim_kinds": {"type": "keyword"},

            # keep original objects for detail views
            "entities": {
                "type": "object",
                "enabled": True  # stored in _source, not indexed separately
            },
            "stance": { "type": "object", "enabled": True },
            "claims": { "type": "object", "enabled": True }
        }
    }
}
