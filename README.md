# Syntra

Syntra is a local streaming analytics prototype for LinkedIn-style profile snapshots. A Python producer publishes JSON records to Kafka; Spark Structured Streaming parses each micro-batch, updates profile and monthly-analytics documents in Elasticsearch, and computes similarity and ranking outputs. A FastAPI query layer and a React dashboard expose the data.

## Architecture

`JSON fixture → Python producer → Kafka → Spark/Scala → Elasticsearch → FastAPI → React`

Spark uses exact `groupBy(...).count()` aggregation for skill and employer frequencies, top-K ranking, and MLlib `MinHashLSH` for approximate Jaccard similarity. Despite historical class names in the source, no Count-Min Sketch is implemented.

## Implemented API

| Method | Route | Purpose |
|---|---|---|
| GET | `/profiles` | Return profile documents |
| GET | `/profiles/search` | Filter profiles by company, skill, or date |
| GET | `/profiles/detailed-search` | Flatten matching experience records |
| GET | `/profiles/work-duration` | Longest recorded job per profile |
| GET | `/analytics/monthly` | Query monthly analytics |
| GET | `/analytics/yearly` | Query a yearly index (currently not produced) |
| GET | `/mock/*` | Fixture-backed endpoints for the dashboard |

## Technology

Scala 2.13.14, Spark 3.5.0, Kafka/Zookeeper 7.6.0, Elasticsearch/Kibana 8.13.0, Python, FastAPI, pandas, React 19, Vite and Recharts. Docker Compose defines the infrastructure, producer and Spark services; it does not include the API or frontend.

## Run

Prerequisites: Docker Compose, Java 17, sbt, Python 3.11+, and Node.js.

```bash
docker compose up --build

python -m venv .venv
source .venv/bin/activate
pip install -r queries/requirements.txt
uvicorn queries.api:app --reload --port 8000

cd frontend
npm install
npm run dev
```

For local pipeline development, start Kafka and Elasticsearch with Compose, then run `python producer.py` and `sbt "runMain MainForStreamData"` from the repository root.

## Verification

```bash
python test_producer.py
python test_queries.py
sbt compile
sbt "runMain TestIngestion"
sbt "runMain TestTopK"
sbt "runMain TestLSH"
sbt "runMain TestElasticsearchWriter"
sbt "runMain TestAnalyticsPipeline"
```

These are executable verification programs, not a conventional unit-test suite. See `HOW_TO_TEST.md` for details.

## Structure

- `producer.py` — fixture-to-Kafka producer
- `src/main/scala/ingestion` — schema and parsing
- `src/main/scala/processing` — counts, top-K and similarity
- `src/main/scala/analytics` — monthly aggregation
- `src/main/scala/storage` — Elasticsearch writers
- `queries` — FastAPI query service
- `frontend` — React dashboard

## Known limitations

- This is a local demonstration, not a production-ready service: there is no authentication, authorization, TLS, monitoring, dead-letter handling, CI, or deployment configuration.
- The dashboard currently calls mock routes, so it does not prove the full Kafka-to-Elasticsearch path.
- `/analytics/yearly` reads `people_yearly_stats`, but no committed job writes that index.
- Profile experiences are queried as ordinary objects; no explicit Elasticsearch nested mapping is created.
- Processing is micro-batch based and uses `collect()` in several paths, which limits scale.
- Kafka and Elasticsearch run without security for local use.
