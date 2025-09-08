'''
Document-level sentiment using VADER.

VADER is rule-based, fast, and works well on headlines/business news. We keep a
simple mapping to 'neg/neu/pos' for filtering and store the raw compound score.

'''
from typing import Optional
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from .models import Sentiment

_an = SentimentIntensityAnalyzer()


# Compute sentiment on the first N characters.  Returns  Sentiment or None if text is empty.
def doc_sentiment(text: str) -> Optional[Sentiment]:
    text = (text or "").strip()
    if not text:
        return None
    s = _an.polarity_scores(text[:6000])  # cap for speed
    comp = s["compound"]
    if comp >= 0.05:   lab = "pos"
    elif comp <= -0.05: lab = "neg"
    else:              lab = "neu"
    return Sentiment(label=lab, compound=comp)
