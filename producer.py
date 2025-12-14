import json
import time
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(
    r"C:\Users\Sanasys\Desktop\LinkHarvester\src\main\resources\people_1000_development.json",
    "r",
    encoding="utf-8"
) as f:
    data = json.load(f)   # ✅ Array JSON

    for obj in data:
        producer.send(
            "test",
            key=obj.get("person_id", "unknown").encode("utf-8"),
            value=obj
        )
        print("Sent:", obj.get("person_id", "unknown"))
        time.sleep(1)

producer.flush()
producer.close()
