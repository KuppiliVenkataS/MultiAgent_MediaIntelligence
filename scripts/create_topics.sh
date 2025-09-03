#!/usr/bin/env bash
set -euo pipefail
echo "Creating Kafka topics: raw.documents, enriched.documents, errors.documents"
docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic raw.documents --partitions 3 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic enriched.documents --partitions 3 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists --topic errors.documents --partitions 1 --replication-factor 1 || true
docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list
