# Time‑to‑brief (p50/p95), factuality score, % briefs with ≥3 citations, 
# diversity index, cost/brief, approval ratio.



from apps.api.main import create_brief


def test_brief_has_citations(monkeypatch):
    # monkeypatch run_graph to return a canned response
    def fake_run(ctx):
        return {"brief":"...","actions":["alert"],"citations":["https://example.com","https://news.example"]}
    # inject fake
    # ...
    out = create_brief("What happened to Brand X?")
    assert len(out["citations"]) >= 2