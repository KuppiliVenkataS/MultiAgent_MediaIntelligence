#Policy hooks: max tokens, domain allowlist, cost caps, min citations.

# Memory: thread (short‑term) + per‑entity/brand (long‑term) store (Redis/Postgres).
from enum import Enum


class Node(Enum): PLAN=1; RESEARCH=2; VERIFY=3; BRIEF=4; ACT=5; DONE=6


class Context(dict): pass # holds query, evidence, citations, costs, decisions


def run_graph(ctx: Context):
    node = Node.PLAN
    while node != Node.DONE:
        if node==Node.PLAN:
            ctx["plan"] = plan(ctx["goal"]) ; node = Node.RESEARCH
        elif node==Node.RESEARCH:
            ctx["evidence"] = research(ctx["plan"]) ; node = Node.VERIFY
        elif node==Node.VERIFY:
            ok, notes = verify(ctx["evidence"]) ; ctx["verify_notes"]=notes
            node = Node.BRIEF if ok else Node.RESEARCH
        elif node==Node.BRIEF:
            ctx["brief"] = write_brief(ctx) ; node = Node.ACT
        elif node==Node.ACT:
            ctx["actions"] = propose_actions(ctx) ; node = Node.DONE
    return ctx