import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(r"C:\Users\MSI\IdeaProjects\LinkHarvester\src\main\resources\people_1000_development.json", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue


        obj = json.loads(line)
        producer.send("test", obj)

        time.sleep(1)  

producer.flush()