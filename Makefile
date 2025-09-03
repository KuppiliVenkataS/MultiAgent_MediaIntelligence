SHELL := /bin/bash

.PHONY: up down restart logs test seed brief fmt

up:
	docker compose up -d --build

down:
	docker compose down

restart: down up

logs:
	docker compose logs -f --tail=200

test:
	python -m pytest -q

seed:
	python scripts/seed_sample.py

brief:
	curl -s -X POST "http://localhost:8080/brief" -H "Content-Type: application/json" -d '{"goal":"$(q)"}' | jq
