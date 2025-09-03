An end‑to‑end agentic media intelligence system that contains following steps: 
ingest → enrich → KG+vector retrieval → multi‑agent research→verify→brief→act
with a demo UI 

The medium links: TBD

Repo contains important folders. The explanation of each folder is added to info.txt

**MLOps, Evaluation & Governance** (More on Wiki)

*Objective:* Make it measurable, affordable, and safe.

Eval harness

Offline: small labeled set → coverage, factuality, citation count, diversity.

Online: A/B prompts, latency, cost per brief, approval ratio.

Guardrails

Defamation & safety filters; minimum citation policy; source diversity.

PII controls, retention windows, audit logs.

Observability

Traces (tokens/tools), structured logs, dashboards.

Metrics Checklist

Time‑to‑brief (p50/p95), factuality score, % briefs with ≥3 citations, diversity index, cost/brief, approval ratio.

**Sample Prompts (to adapt)**

Plan: "Break the goal into steps: research → collect diverse sources → identify claims → verify disagreement → propose actions. Return a JSON plan."

Verify: "Given citations and claims, check for agreement across at least 3 independent sources. Flag potential defamation or uncertainty."

Brief: "Write a concise, neutral brief with timestamps, bullet points, and inline numbered citations. Include ‘What/Why/Impact/Next’."