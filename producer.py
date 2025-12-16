import json
import time
from kafka import KafkaProducer
from datetime import datetime, timedelta


def add_one_month(date_str):
    """
    يزيد شهر واحد على تاريخ بصيغة YYYY-MM-DD
    بدون أي مكتبات خارجية
    """
    year, month, day = map(int, date_str.split("-"))

    # زيادة شهر
    month += 1
    if month > 12:
        month = 1
        year += 1

    # معالجة نهاية الشهر (مثل 31 فبراير)
    try:
        return datetime(year, month, day).strftime("%Y-%m-%d")
    except ValueError:
        # آخر يوم في الشهر
        if month == 12:
            return datetime(year + 1, 1, 1) - timedelta(days=1)
        else:
            return datetime(year, month + 1, 1) - timedelta(days=1)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

with open(
    r"C:\Users\Sanasys\Desktop\LinkHarvester\src\main\resources\people_1000_development.json",
    "r",
    encoding="utf-8"
) as f:
    data = json.load(f)   # Array JSON

    # ترتيب حسب التاريخ ثم person_id
    data_sorted = sorted(
        data,
        key=lambda x: (
            datetime.strptime(x["date"], "%Y-%m-%d"),
            x["person_id"]
        )
    )

    for obj in data_sorted:

        # 🔹 تعديل date_to فقط (date_from يبقى كما هو)
        for company in obj["companies"]:
            old_date_from = company.get("date_from")
            if old_date_from:
                company["date_to"] = add_one_month(old_date_from)

        producer.send(
            "test",
            key=obj["person_id"].encode("utf-8"),
            value=obj
        )

        print("Sent:", obj["person_id"], obj["date"])
        time.sleep(1)

producer.flush()
producer.close()
