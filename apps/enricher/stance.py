from typing import List
import os, re, math
from .models import Stance, Entity

SUPPORT_CUES = {"supports","backs","approves","praises","benefits","boosts","wins","gains","surges",
                "record","strong","beats","outperforms","bullish","positive"}
OPPOSE_CUES  = {"criticizes","opposes","slams","hits","hurts","threatens","fails","drops",
                "weak","misses","underperforms","bearish","negative","lawsuit","probe"}

# USE_HF = os.getenv("USE_HF_STANCE","1") == "1" # when you want to stay rule-based
USE_HF = os.getenv("USE_HF_STANCE","1") == "1"
_HF = None
if USE_HF:
    try:
        from transformers import pipeline
        _HF = pipeline("text-classification", model="facebook/bart-large-mnli")
    except Exception:
        _HF = None  # fallback to rules

def _window(text: str, start: int, end: int, radius: int = 80) -> str:
    a = max(0, start - radius); b = min(len(text), end + radius)
    return text[a:b]

def _score_rules(snippet: str) -> float:
    t = snippet.lower()
    pos = sum(1 for w in SUPPORT_CUES if re.search(rf"\b{re.escape(w)}\b", t))
    neg = sum(1 for w in OPPOSE_CUES  if re.search(rf"\b{re.escape(w)}\b", t))
    if pos==neg==0: return 0.0
    return (pos - neg) / (pos + neg)

def stance_for_entities(text: str, entities: List[Entity]) -> List[Stance]:
    if not text or not entities:
        return []

    out: List[Stance] = []
    # rule-based default
    for e in entities:
        ctx = _window(text, e.start, e.end)
        r = _score_rules(ctx)
        if r >  0.2: lab, conf = "pro",  min(0.9, 0.5 + r)
        elif r < -0.2: lab, conf = "con",  min(0.9, 0.5 + abs(r))
        elif r == 0.0: lab, conf = "unknown", 0.0
        else:           lab, conf = "neutral", 0.3
        out.append(Stance(target=e.text, label=lab, confidence=conf))

    # optional HF refinement (doc-level toward each target)
    if _HF:
        refined: List[Stance] = []
        for s in out:
            try:
                # NLI hack: compare hypotheses 'supports X' vs 'opposes X'
                result = _HF([f"This text supports {s.target}.", f"This text opposes {s.target}."],
                             hypothesis_template=None, return_all_scores=True, truncation=True, top_k=None)
                # result is list per hypothesis -> flatten prob of 'ENTAILMENT'
                sup = next((sc["score"] for sc in result[0] if sc["label"]=="ENTAILMENT"), 0.0)
                con = next((sc["score"] for sc in result[1] if sc["label"]=="ENTAILMENT"), 0.0)
                if max(sup, con) < 0.6:
                    refined.append(s)  # keep rule stance
                else:
                    refined.append(Stance(
                        target=s.target,
                        label="pro" if sup > con else "con",
                        confidence=max(sup, con)
                    ))
            except Exception:
                refined.append(s)
        out = refined

    return out
