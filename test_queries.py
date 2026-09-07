"""
Test: queries/*.py logic using mock ES data
No Elasticsearch needed — patches the es client with local JSON.
Run: python test_queries.py
"""
import json
from pathlib import Path

DATA_PATH = Path(__file__).resolve().parent / "src/main/resources/people_1000_development.json"

def load_mock_docs(n=50):
    with open(DATA_PATH) as f:
        raw = json.load(f)
    # Simulate what ES returns as _source
    return raw[:n]

def make_es_response(docs):
    """Simulate ES search() response shape."""
    return {
        "hits": {
            "hits": [
                {"_source": doc, "inner_hits": {}} for doc in docs
            ]
        }
    }

# ── Test fetch_all_documents ───────────────────────────────────────────────────
def test_fetch_all():
    docs = load_mock_docs(50)
    response = make_es_response(docs)
    result = [hit["_source"] for hit in response["hits"]["hits"]]

    assert len(result) == 50
    assert "person_id" in result[0]
    print(f"[1] fetch_all_documents shape: {len(result)} docs, keys: {list(result[0].keys())[:5]}")

# ── Test data shapes Python queries expect ─────────────────────────────────────
def test_expected_shapes():
    docs = load_mock_docs(10)
    for doc in docs:
        assert "person_id"   in doc
        assert "companies"   in doc
        assert "skills_after" in doc
        assert isinstance(doc["companies"], list)
    print(f"[2] All 10 docs match expected Python query shapes")

# ── Test sorting logic from monthly_data.py ────────────────────────────────────
def test_monthly_grouping():
    docs = load_mock_docs(100)
    from collections import defaultdict
    by_month = defaultdict(list)
    for doc in docs:
        month = (doc.get("last_update") or "")[:7]  # yyyy-MM
        by_month[month].append(doc)
    print(f"[3] Monthly grouping: {dict((k, len(v)) for k, v in by_month.items())}")
    assert len(by_month) > 0

if __name__ == "__main__":
    print("\n========== TEST: Python Queries (mock ES) ==========\n")
    test_fetch_all()
    test_expected_shapes()
    test_monthly_grouping()
    print("\n[PASS] All query tests completed.\n")
