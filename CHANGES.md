# Pipeline Refactor — Simplified Streaming Architecture

Removed Components:
- MainForMonthDataProcess.scala: duplicate entry point
- MonthlyKafkaWriter.scala: unnecessary intermediate Kafka layer
- ProcessDataReader.scala: replaced by direct foreachBatch processing

Architecture:

Before:
JSON → Kafka(raw) → Spark Job 1 → Kafka(monthly) → Spark Job 2 → Elasticsearch

After:
JSON → Kafka(raw) → Spark Streaming → Elasticsearch
└── analytics computed inside foreachBatch

Changes:
- Spark jobs: 2 → 1
- Removed intermediate Kafka topic layer
- Analytics merged into streaming job

Impact:
- Lower complexity
- Fewer moving parts
- Simpler execution model

Trade-offs:
- Less modular separation
- Reduced replay of intermediate states
- More logic inside single pipeline

Result:
- Faster pipeline
- Easier development and testing
- Cleaner architecture
