"""
Test: producer.py data loading and serialization
No Kafka needed — tests the data prep logic only.
Run: python test_producer.py
"""
import json, os, sys

DATA_PATH = "src/main/resources/people_1000_development.json"

def test_file_loads():
    with open(DATA_PATH) as f:
        data = json.load(f)
    assert isinstance(data, list), "Expected a list"
    assert len(data) > 0, "Empty data file"
    print(f"[1] Loaded {len(data)} records")
    return data

def test_schema(data):
    required = ["person_id", "name", "skills_after", "companies", "last_update"]
    for i, record in enumerate(data[:10]):
        for field in required:
            assert field in record, f"Record {i} missing field: {field}"
    print(f"[2] Schema check passed on first 10 records")

def test_sorting(data):
    sorted_data = sorted(data, key=lambda x: (
        x.get("last_update") or "",
        x.get("person_id")   or ""
    ))
    assert len(sorted_data) == len(data), "Sort changed record count"
    print(f"[3] Sorting produces {len(sorted_data)} records — OK")

def test_serialization(data):
    for i, record in enumerate(data[:50]):
        try:
            serialized = json.dumps(record).encode("utf-8")
            assert len(serialized) > 0
        except Exception as e:
            print(f"[FAIL] Record {i} failed serialization: {e}")
            sys.exit(1)
    print(f"[4] Serialization check passed on first 50 records")

def test_null_handling(data):
    nulls = [r for r in data if r.get("person_id") is None]
    assert len(nulls) == 0, f"{len(nulls)} records have null person_id"
    no_skills = [r for r in data if not r.get("skills_after")]
    print(f"[5] Null person_ids: 0 — OK")
    print(f"[5] Records with no skills_after: {len(no_skills)}")

if __name__ == "__main__":
    print("\n========== TEST: Producer Data ==========\n")
    data = test_file_loads()
    test_schema(data)
    test_sorting(data)
    test_serialization(data)
    test_null_handling(data)
    print("\n[PASS] All producer tests completed.\n")
