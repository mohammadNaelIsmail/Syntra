import os
import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta

DATA_PATH = os.getenv(
    "DATA_PATH",
    "src/main/resources/people_1000_development.json"
)
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "linkedin-profiles")
SEND_DELAY  = float(os.getenv("SEND_DELAY", "0.1"))

def add_one_month(date_str):
    year, month, day = map(int, date_str.split("-"))
    month += 1
    if month > 12:
        month = 1
        year += 1
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        first_of_next = datetime(year, month + 1 if month < 12 else 1,
                                  1, year + (1 if month == 12 else 0))
        return (first_of_next - timedelta(days=1)).strftime("%Y-%m-%d")

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    retries=3
)

# stream بدل load كامل في RAM
with open(DATA_PATH, "r", encoding="utf-8") as f:
    data = json.load(f)

data_sorted = sorted(
    data,
    key=lambda x: (x.get("date", ""), x.get("person_id", ""))
)

for obj in data_sorted:
    for company in obj.get("companies", []):
        date_from = company.get("date_from")
        if date_from:
            company["date_to"] = add_one_month(date_from)

    producer.send(
        KAFKA_TOPIC,
        key=obj["person_id"].encode("utf-8"),
        value=obj
    )
    print(f"Sent: {obj['person_id']} {obj.get('date', '')}")
    time.sleep(SEND_DELAY)

producer.flush()
producer.close()
