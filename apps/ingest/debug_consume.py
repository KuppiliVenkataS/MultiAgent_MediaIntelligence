import json, os
from kafka import KafkaConsumer

topic = os.getenv("TOPIC", "raw.documents")
bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")

consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[bootstrap],
    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False,
    group_id=None,
)

for i, msg in enumerate(consumer):
    print(json.dumps(msg.value, indent=2, ensure_ascii=False))
    if i >= int(os.getenv("MAX", "5")) - 1:
        break
