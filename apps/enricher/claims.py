from typing import List
import re, spacy
from .models import Claim

_NLP = spacy.blank("en")  # just for sentencizer
if "sentencizer" not in _NLP.pipe_names:
    _NLP.add_pipe("sentencizer")

PATS = [
    (r"\b(plans to|aims to|intends to)\b",            "commitment"),
    (r"\b(will|set to|poised to)\b",                  "forecast"),
    (r"\b(announced|unveiled|launched|introduced)\b", "event"),
    (r"\b(agreed to acquire|acquires|merges with)\b", "event"),
    (r"\b(expects|is expected to|forecast[s]?)\b",    "forecast"),
    (r"\b(raises|cuts) guidance\b",                   "forecast"),
    (r"\b(up|down)\s+\d+(\.\d+)?%(\s+yoy|\s+qoq)?\b", "metric"),
    (r"\b(report[s]? that|according to)\b",           "uncertain"),
]

COMP = [(re.compile(p, re.I), kind) for p, kind in PATS]

def extract_claims(text: str, max_sents: int = 20) -> List[Claim]:
    text = (text or "").strip()
    if not text:
        return []
    doc = _NLP(text[:15000])
    out: List[Claim] = []
    for i, sent in enumerate(doc.sents):
        if i >= max_sents: break
        s = sent.text.strip()
        cues = []
        kinds = set()
        for rx, kind in COMP:
            if rx.search(s):
                cues.append(rx.pattern)
                kinds.add(kind)
        if cues:
            # pick the most specific kind heuristically (prefer event/forecast/commitment over metric/uncertain)
            priority = ["event","commitment","forecast","metric","uncertain","other"]
            chosen = next((k for k in priority if k in kinds), "other")
            conf = min(0.9, 0.5 + 0.1*len(cues))
            out.append(Claim(text=s, kind=chosen, cues=cues, confidence=conf))
    return out
