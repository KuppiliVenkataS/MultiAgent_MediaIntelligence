from apps.ingest.normalizer import canonical_url, make_doc_id, detect_lang

def test_canonical_url_drops_query():
    u = "https://example.com/path?a=1#frag"
    assert canonical_url(u) == "https://example.com/path"

def test_make_doc_id_stable():
    a = make_doc_id("https://example.com/a?x=1", "2024-01-01")
    b = make_doc_id("https://example.com/a?y=2", "2024-01-01")
    assert a == b

def test_detect_lang_returns_code():
    assert detect_lang("This is a simple English sentence for testing.") in (None, "en")
