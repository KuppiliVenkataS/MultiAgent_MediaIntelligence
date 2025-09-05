import os, spacy
from .models import Entity
from typing import List

_ALLOWED = {"PERSON","ORG","GPE","LOC","PRODUCT","EVENT","WORK_OF_ART","LAW","LANGUAGE","NORP","FAC"}
_MODEL = os.getenv("SPACY_MODEL", "en_core_web_sm")

try:
    _NLP = spacy.load(_MODEL)
except OSError:
    if os.getenv("ALLOW_RUNTIME_MODEL_DOWNLOAD", "0") == "1":
        from spacy.cli import download
        download(_MODEL)
        _NLP = spacy.load(_MODEL)
    else:
        raise  # keep failing loudly if not allowed to download at runtime

def extract_entities(text: str) -> List[Entity]:
    if not text:
        return []
    doc = _NLP(text[:100_000])
    return [
        Entity(text=e.text.strip(), label=e.label_, start=e.start_char, end=e.end_char)
        for e in doc.ents if e.label_ in _ALLOWED and e.text.strip()
    ]
