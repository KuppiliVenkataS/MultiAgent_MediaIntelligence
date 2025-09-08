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

**Folder Information**
apps/ingest/models.py: Basic structure of the documents received.
apps/ingest/normalizer.py: Contains utility functions for text extraction, language detection, etc.
apps/ingest/producer.py: To create a Kafka producer instance. A Kafka Producer is a client application that writes (publishes) messages or records to Kafka topics.
apps/ingest/rss_worker.py: This file is typically a Python script designed to ingest data from one or more RSS feeds and publish it as a stream of events to a Kafka topic (add as many feeds as you want). We used a STUB for checking the initial setup.
apps/ingest/Dockerfile: A text file that contains all the commands needed to build a Docker image.

scripts/create_topics.sh: The purpose of this file is to create the topics the pipeline expects with the right names, partitions, and replication. It creates raw-documents, error-documents, and enriched-documents. (There is a version issue with ‘-’ and ‘.’ in the document names. Please follow the requirements of the version. )