# LinkHarvester

## تشغيل كل شيء بأمر واحد

```bash
docker-compose up --build
```

هذا يشغّل بالترتيب:
1. Zookeeper
2. Kafka (ينتظر Zookeeper)
3. Elasticsearch (بالتوازي)
4. Kibana (ينتظر Elasticsearch)
5. Spark/Scala stream (ينتظر Kafka + Elasticsearch)
6. Python Producer (ينتظر Kafka)

## الروابط بعد التشغيل

| الخدمة        | الرابط                    |
|---------------|--------------------------|
| Kibana        | http://localhost:5601     |
| Elasticsearch | http://localhost:9200     |
| Kafka         | localhost:9092            |

## تشغيل بدون Docker (local)

### 1. Kafka
```bash
# تشغيل Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# تشغيل Kafka
bin/kafka-server-start.sh config/server.properties
```

### 2. Elasticsearch + Kibana
```bash
./bin/elasticsearch
./bin/kibana
```

### 3. Spark (Scala)
```bash
sbt runMain MainForStreamData
```

### 4. Python Producer
```bash
pip install kafka-python
python producer.py
```

## هيكل المشروع

```
src/main/scala/
├── AppConfig.scala                        ← كل الإعدادات
├── MainForStreamData.scala                ← نقطة البداية الوحيدة
├── analytics/MonthlyAnalyticsJob.scala
├── ingestion/LinkedInIngestion.scala
├── processing/
│   ├── Best_Kperson_company.scala
│   ├── Best_Kperson_skills.scala
│   ├── countMinSkitch.scala
│   └── localitySensitiveHashing.scala
└── storage/
    ├── ElasticsearchWriter.scala
    └── ElasticsearchProcessDataWriter.scala

src/main/resources/
└── application.conf                       ← إعدادات البيئة

queries/                                   ← Python للاستعلامات
producer.py                                ← يرسل البيانات لـ Kafka
docker-compose.yml                         ← تشغيل كل شيء
```
