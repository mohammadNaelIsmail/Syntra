# How to Test Each Component Independently

No Kafka. No Elasticsearch. No Docker. Each test runs in isolation.

## RAM Guide

| Test | Command | RAM needed |
|---|---|---|
| Producer data | `python test_producer.py` | ~100 MB |
| Python queries | `python test_queries.py` | ~100 MB |
| Ingestion/parsing | `sbt "runMain TestIngestion"` | ~1.5 GB |
| Count-Min Sketch | `sbt "runMain TestCMS"` | ~1.5 GB |
| Top-K | `sbt "runMain TestTopK"` | ~1 GB |
| ES merge logic | `sbt "runMain TestElasticsearchWriter"` | ~1.5 GB |
| Analytics pipeline | `sbt "runMain TestAnalyticsPipeline"` | ~2 GB |
| LSH similarity | `sbt "runMain TestLSH"` | ~2 GB |

**Never run two sbt tests at the same time on 8GB RAM.**

## Recommended Order (lightest to heaviest)

```bash
# Step 1 — Python only, no Spark
python test_producer.py
python test_queries.py

# Step 2 — Spark, lightweight
sbt "runMain TestIngestion"
sbt "runMain TestTopK"

# Step 3 — Spark, medium
sbt "runMain TestCMS"
sbt "runMain TestElasticsearchWriter"

# Step 4 — Spark, heavy (run separately, close browser first)
sbt "runMain TestAnalyticsPipeline"
sbt "runMain TestLSH"
```

## What Each Test Covers

- **TestIngestion**: schema, null handling, date parsing
- **TestCMS**: skill/company counts, CMS vs exact comparison
- **TestTopK**: best person by skills, best person by companies
- **TestLSH**: similarity results, no self-matches (also exposes the DenseVector bug)
- **TestAnalyticsPipeline**: full MonthlyAnalyticsJob JSON output, all keys present
- **TestElasticsearchWriter**: merge logic, uniqueness, date formatting
- **TestPythonQueries**: field shape compatibility between Scala output and Python input
- **test_producer.py**: JSON file integrity, sorting, serialization
- **test_queries.py**: query logic with mocked ES responses

## To run all Python tests at once

```bash
python test_producer.py && python test_queries.py
```
