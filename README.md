# Syntra — Production-Ready Fixes (Critical Architecture Gaps)

## 1. Startup Race Conditions (Docker)

### Problem
`depends_on` does NOT guarantee service readiness.

- Kafka may be “started” but not accepting connections
- Elasticsearch may be running but not ready for indexing
- Spark / Producer may crash on first boot

### Fix
Add retry + health-wait logic inside Spark and Producer:

- Retry Kafka connection until bootstrap is reachable
- Retry Elasticsearch `_cluster/health` until status = yellow/green
- Add exponential backoff (recommended: 5–30 seconds)

---

## 2. Kafka Readiness Issue

### Problem
Docker healthcheck ≠ real broker readiness

### Fix Options
- Add explicit “wait-for-kafka” script
- OR wrap producer initialization with retry loop:
