# continuous streams of data
import feedparser, hashlib, json, time
from kafka import KafkaProducer
from models import Document


producer = KafkaProducer(bootstrap_servers=["kafka:9092"], value_serializer=lambda v: json.dumps(v).encode())


FEEDS = [
"https://feeds.a.dj.com/rss/RSSMarketsMain.xml",
"https://www.theverge.com/rss/index.xml"
]


while True:
    for url in FEEDS:
        feed = feedparser.parse(url)
        for e in feed.entries:
            doc = Document(
            id=hashlib.sha1((e.link+str(e.get('published',''))).encode()).hexdigest(),
            source_url=e.link,
            source_type="rss",
            title=e.title,
            text=e.get("summary",""),
            published_at=e.get("published_parsed", None)
            )
            producer.send("raw.documents", doc.model_dump())
    time.sleep(300)