from kafka import KafkaConsumer, KafkaProducer
import json
from schemas import Entity, Event


consumer = KafkaConsumer("raw.documents", bootstrap_servers=["kafka:9092"], value_deserializer=lambda b: json.loads(b))
producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda v: json.dumps(v).encode())


for msg in consumer:
    doc = msg.value
    # run NER/linking .. TBD
    entities = link_entities(run_ner(doc["text"]))
    sentiment = clf_sentiment(doc["text"]) # [-1..1]
    events = extract_events(doc["text"], entities)
    payload = {"doc_id": doc["id"], "entities": [e.__dict__ for e in entities],
    "events": [ev.__dict__ for ev in events], "sentiment": sentiment}
    producer.send("enriched.documents", payload)