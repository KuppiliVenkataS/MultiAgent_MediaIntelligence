# all stream info from kafka

import json, os, time
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")




def wait_for_broker(timeout=60):
    """Poll until the broker responds to AdminClient."""
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="ingest-wait")
            admin.close()
            return True
        except Exception:
            time.sleep(2)
    return False

def ensure_topic(topic: str, partitions: int = 3, replication: int = 1):
    """Create the topic if it doesn't exist (works even if auto-create is off)."""
    try:
        admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP, client_id="ingest-admin")
        existing = set(admin.list_topics())
        if topic not in existing:
            admin.create_topics([NewTopic(name=topic, num_partitions=partitions, replication_factor=replication)])
        admin.close()
    except Exception:
        # topic may already exist or broker not yet ready; that's OK—producer will retry
        pass

def get_producer():
    # retries/backoff + generous timeouts
    return KafkaProducer(
        bootstrap_servers=[BOOTSTRAP],
        value_serializer=lambda v: (
            v if isinstance(v, (bytes, bytearray))
            else json.dumps(v, ensure_ascii=False, default=str).encode("utf-8")
        ),
        acks="all",
        retries=10,
        retry_backoff_ms=300,
        request_timeout_ms=30000,
        max_block_ms=60000,
        linger_ms=50,
    )

