from apps.graph_writer.worker import transform, _sha1

def test_transform_shapes():
    rec = {
        "id": "x1",
        "title": "Apple plans UK hub",
        "source_url": "https://example.com/a",
        "lang": "en",
        "entities": [{"text":"Apple","label":"ORG"},{"text":"London","label":"GPE"}],
        "sentiment": {"label":"pos","compound":0.7},
        "stance": [{"target":"Apple","label":"pro","confidence":0.8}],
        "claims": [{"text":"Apple plans to open a UK hub","kind":"commitment","confidence":0.6}],
    }
    out = transform(rec)
    assert out["article"]["id"] == "x1"
    assert {"name":"Apple","label":"ORG"} in out["entities"]
    assert any(m["article_id"]=="x1" and m["name"]=="Apple" for m in out["mentions"])
    assert out["stances"][0]["stance"] in {"pro","con","neutral","unknown"}
    cid = _sha1("Apple plans to open a UK hub")
    assert any(c["id"] == cid for c in out["claims"])
