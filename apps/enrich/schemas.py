from pydantic import BaseModel
from typing import List, Optional


class Entity(BaseModel):
    id: str # canonical entity id
    name: str
    type: str # PERSON, ORG, GEO, PRODUCT etc


class Event(BaseModel):
    id: str
    type: str # e.g., ACQUISITION, LAWSUIT, OUTAGE
    actors: List[str] # entity ids
    when: str
    where: Optional[str]
    summary: str