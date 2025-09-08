from apps.indexer.worker import transform

def test_transform_facets():
    enriched = {
        "id": "x1",
        "title": "Apple plans UK hub",
        "text": "Apple plans to open a new London hub next year.",
        "lang": "en",
        "entities": [{"text":"Apple","label":"ORG"},{"text":"London","label":"GPE"}],
        "sentiment": {"label":"pos","compound":0.7},
        "stance": [{"target":"Apple","label":"pro","confidence":0.8}],
        "claims": [{"text":"Apple plans to open...","kind":"commitment","confidence":0.6}],
    }
    doc = transform(enriched)
    assert "Apple" in doc["ent_names"]
    assert "ORG" in doc["ent_labels"]
    assert doc["sentiment_label"] == "pos"
    assert "commitment" in doc["claim_kinds"]
