SHELL := /bin/bash
DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

.PHONY: up down restart logs test seed brief fmt

up:
	$(DOCKER_COMPOSE) up -d --build

down:
	$(DOCKER_COMPOSE) down

restart: down up

logs:
	$(DOCKER_COMPOSE) logs -f --tail=200

test:
	python -m pytest -q

seed:
	python scripts/seed_sample.py

brief:
	curl -s -X POST "http://localhost:8080/brief" -H "Content-Type: application/json" -d '{"goal":"$(q)"}' | jq

DOCKER_COMPOSE := $(shell docker compose version >/dev/null 2>&1 && echo "docker compose" || echo "docker-compose")

topics:
	 bash scripts/create_topics.sh

ingest:
	 $(DOCKER_COMPOSE) up -d ingest

tap:
	 $(DOCKER_COMPOSE) exec -T kafka kafka-console-consumer --bootstrap-server kafka:9092 --topic raw.documents --from-beginning --max-messages 5
