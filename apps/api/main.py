from fastapi import FastAPI
from orchestrator.graph import run_graph, Context


app = FastAPI()


@app.post("/brief")
def create_brief(goal: str):
    ctx = Context(goal=goal)
    result = run_graph(ctx)
    return {"brief": result["brief"], "citations": result.get("citations", []), "actions": result["actions"]}