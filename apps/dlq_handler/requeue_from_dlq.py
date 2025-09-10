"""
Requeue DLQ items by ES query (e.g., error_type:network last 24h).
Usage (inside container):
  python requeue_from_dlq.py 'error_type:network AND ts:[now-24h TO now]'
"""
import os, sys, json
from elasticsearch import Elasticsearch
from kafka import KafkaProducer

ES = Elasticsearch(os.getenv("ES_HOST","http://elasticsearch:9200"))
INDEX = os.getenv("ES_ERROR_INDEX","news-errors-v1")
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS","kafka:9092")
OUTPUT = os.getenv("OUTPUT_TOPIC","enriched-documents")

def main():
    q = sys.argv[1] if len(sys.argv) > 1 else "error_type:network"
    res = ES.search(index=INDEX, body={"query":{"query_string":{"query":q}}, "_source":["payload"], "size":100})
    prod = KafkaProducer(bootstrap_servers=BROKER, value_serializer=lambda m: json.dumps(m).encode("utf-8"))
    cnt = 0
    for h in res["hits"]["hits"]:
        payload = h["_source"]["payload"]
        prod.send(OUTPUT, payload, headers=[("x-requeued-from-dlq", b"true")])
        cnt += 1
    prod.flush()
    print(f"Requeued {cnt} messages to {OUTPUT}")

if __name__ == "__main__":
    main()
