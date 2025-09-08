from apps.enricher.nlp import extract_entities

def test_extract_entities_basic():
    text = "Apple hired John Smith in London."
    ents = extract_entities(text)
    labels = {e.label for e in ents}
    assert "ORG" in labels or "PERSON" in labels or "GPE" in labels