from typing import Optional
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from .models import Sentiment

_an = SentimentIntensityAnalyzer()

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
