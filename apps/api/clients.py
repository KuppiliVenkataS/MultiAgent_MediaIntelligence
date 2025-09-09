import os
from functools import lru_cache
from typing import Optional
from elasticsearch import Elasticsearch

NEO4J_ENABLED = os.getenv("NEO4J_ENABLED", "1") == "1"

@lru_cache(maxsize=1)
def es_client() -> Elasticsearch:
    url = os.getenv("ELASTIC_URL", "http://elasticsearch:9200")
    return Elasticsearch(url, request_timeout=10)

neo4j_driver_cached = None

def get_neo4j():
    global neo4j_driver_cached
    if not NEO4J_ENABLED:
        return None
    if neo4j_driver_cached is not None:
        return neo4j_driver_cached
    try:
        from neo4j import GraphDatabase, basic_auth
        uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
        user = os.getenv("NEO4J_USER", "neo4j")
        pwd  = os.getenv("NEO4J_PASS", "testpassword")
        drv = GraphDatabase.driver(uri, auth=basic_auth(user, pwd))
        # sanity ping
        with drv.session() as s:
            s.run("RETURN 1").consume()
        neo4j_driver_cached = drv
    except Exception:
        neo4j_driver_cached = None
    return neo4j_driver_cached
