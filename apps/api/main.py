from fastapi import FastAPI
from pydantic import BaseModel
from typing import List

app = FastAPI()

@app.get("/health")
def health():
    return {"status": "ok"}

class BriefResponse(BaseModel):
    brief: str
    citations: List[str] = []
    actions: List[str] = []

@app.post("/brief", response_model=BriefResponse)
def brief(payload: dict):
    goal = payload.get("goal", "Summarize recent events")
    return BriefResponse(
        brief=f"(stub) Brief for: {goal}",
        citations=["https://example.com"],
        actions=["notify_pr","start_monitor"]
    )
