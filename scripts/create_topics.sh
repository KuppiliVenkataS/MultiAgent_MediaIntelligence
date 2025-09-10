#!/usr/bin/env bash
set -euo pipefail
echo "Creating Kafka topics (hyphen names)"
# docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
#   --create --if-not-exists --topic raw-documents --partitions 3 --replication-factor 1 || true
# docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
#   --create --if-not-exists --topic enriched-documents --partitions 3 --replication-factor 1 || true
# docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 \
#   --create --if-not-exists --topic errors-documents --partitions 1 --replication-factor 1 || true
# docker-compose exec -T kafka kafka-topics --bootstrap-server kafka:9092 --list




KAFKA=${KAFKA:-localhost:9092}

create_topic () {
  local topic=$1
  docker-compose exec -T kafka kafka-topics \
    --bootstrap-server $KAFKA \
    --create --if-not-exists \
    --topic "$topic" --partitions 1 --replication-factor 1
}

# Existing:
create_topic raw-documents
create_topic enriched-documents
create_topic errors-documents

# New (3D):
create_topic retries-documents
create_topic dlq-documents

echo "Topics ready."
