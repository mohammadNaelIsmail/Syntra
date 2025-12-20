from elasticsearch import Elasticsearch
from elastic_core import fetch_all_documents

es = Elasticsearch(["http://localhost:9200"])

SOURCE_INDEX = "people_monthly_analytics"  
TARGET_INDEX = "people_yearly_stats"   

docs = fetch_all_documents(index_name=SOURCE_INDEX)

yearly = {}

for doc in docs:
    month = doc.get("month")
    if not month:
        continue

    year = month.split("-")[0]

    if year not in yearly:
        yearly[year] = {
            "skills_people_count": {},
            "companies_people_count": {},
            "best_by_skills": {},
            "best_by_company": {},
            "similarity_by_skills": {},
            "similarity_by_company": {}
        }

    y = yearly[year]

    for s in doc.get("skills_people_count", []):
        y["skills_people_count"][s["skill"]] = (
            y["skills_people_count"].get(s["skill"], 0) + s["count"]
        )

    for c in doc.get("companies_people_count", []):
        y["companies_people_count"][c["company"]] = (
            y["companies_people_count"].get(c["company"], 0) + c["count"]
        )

    p_skill = doc.get("best_person_in_month_by_skills")
    if p_skill:
        y["best_by_skills"][p_skill] = y["best_by_skills"].get(p_skill, 0) + 1

    p_company = doc.get("best_person_in_month_by_company")
    if p_company:
        y["best_by_company"][p_company] = y["best_by_company"].get(p_company, 0) + 1

    for sim in doc.get("similarity_by_skills", []):
        key = (sim["person"], sim["most_similar_person"])
        y["similarity_by_skills"][key] = y["similarity_by_skills"].get(key, 0) + 1

    for sim in doc.get("similarity_by_company", []):
        key = (sim["person"], sim["most_similar_person"])
        y["similarity_by_company"][key] = y["similarity_by_company"].get(key, 0) + 1

yearly_docs = []

for year, data in yearly.items():
    yearly_docs.append({
        "year": year,
        "skills_people_count": [
            {"skill": k, "count": v}
            for k, v in sorted(data["skills_people_count"].items(), key=lambda x: x[1], reverse=True)
        ],
        "companies_people_count": [
            {"company": k, "count": v}
            for k, v in sorted(data["companies_people_count"].items(), key=lambda x: x[1], reverse=True)
        ],
        "similarity_by_skills": [
            {"person": k[0], "most_similar_person": k[1]}
            for k, _ in sorted(data["similarity_by_skills"].items(), key=lambda x: x[1], reverse=True)
        ],
        "similarity_by_company": [
            {"person": k[0], "most_similar_person": k[1]}
            for k, _ in sorted(data["similarity_by_company"].items(), key=lambda x: x[1], reverse=True)
        ],
        "best_person_in_year_by_skills": (
            max(data["best_by_skills"], key=data["best_by_skills"].get)
            if data["best_by_skills"] else None
        ),
        "best_person_in_year_by_company": (
            max(data["best_by_company"], key=data["best_by_company"].get)
            if data["best_by_company"] else None
        )
    })

for doc in yearly_docs:
    es.index(
        index=TARGET_INDEX,
        id=doc["year"],   
        document=doc
    )

print(f"Indexed {len(yearly_docs)} yearly documents into '{TARGET_INDEX}'")
