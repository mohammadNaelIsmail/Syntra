from elasticsearch import Elasticsearch
import pandas as pd

# Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])
INDEX_NAME = "people_snapshot"

def fetch_all_documents(index_name=INDEX_NAME, size=1000):
    """
    Fetch all documents from the index.
    """
    res = es.search(
        index=index_name,
        body={"query": {"match_all": {}}},
        size=size
    )
    return [doc["_source"] for doc in res["hits"]["hits"]]


def fetch_filtered_documents(company=None, date_from=None, date_to=None,
                             index_name=INDEX_NAME, size=1000):
    """
    Fetch documents filtered by company, date range on experiences.
    Supports nested experiences with inner_hits.
    """
    must_clauses = []

    if company or date_from or date_to:
        nested_bool = {"bool": {"must": []}}

        # Filter by company name
        if company:
            nested_bool["bool"]["must"].append({
                "match": {"experiences.company_name": company}
            })

        # Filter by date range
        if date_from or date_to:
            nested_bool["bool"]["must"].append({
                "range": {"experiences.date_from": {"lte": date_to if date_to else "now"}}
            })
            nested_bool["bool"]["must"].append({
                "range": {"experiences.date_to": {"gte": date_from if date_from else "1970-01-01"}}
            })

        must_clauses.append({
            "nested": {
                "path": "experiences",
                "query": nested_bool,
                "inner_hits": {}  # fetch nested experiences
            }
        })

    query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

    res = es.search(index=index_name, body={"query": query}, size=size)

    docs = []
    for hit in res["hits"]["hits"]:
        source = hit["_source"].copy()
        inner = hit.get("inner_hits", {}).get("experiences", {}).get("hits", {}).get("hits", [])
        if inner:
            source["experiences"] = [i["_source"] for i in inner]
        docs.append(source)

    return docs


