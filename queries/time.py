import pandas as pd
from datetime import datetime
from elastic_core import fetch_filtered_documents  

target_company = input("Filter by company? (y/n): ").strip().lower() == 'y'
company_name = input("Enter company name: ").strip() if target_company else None

target_skill = input("Filter by skill? (y/n): ").strip().lower() == 'y'
skill_name = input("Enter skill name: ").strip() if target_skill else None

target_period = input("Filter by period? (y/n): ").strip().lower() == 'y'
start_date = input("Enter start date (YYYY-MM-DD): ").strip() if target_period else None
end_date = input("Enter end date (YYYY-MM-DD): ").strip() if target_period else None

docs = fetch_filtered_documents(
    company=company_name if target_company else None,
    date_from=start_date if target_period else None,
    date_to=end_date if target_period else None
)

rows = []
for person in docs:
    skills_after = [s.lower() for s in person.get("skills_after_list", [])]

    if target_skill and skill_name.lower() not in skills_after:
        continue

    for exp in person.get("experiences", []):
        date_from = pd.to_datetime(exp.get("date_from"))
        date_to = pd.to_datetime(exp.get("date_to")) if exp.get("date_to") else datetime.today()
        duration_days = (date_to - date_from).days
        duration_str = f"{duration_days // 365}y - {(duration_days % 365) // 30}m - {duration_days % 30}d"

        rows.append({
            "person_id": person["person_id"],
            "name": person["name"],
            "company": exp["company_name"],
            "date_from": exp["date_from"],
            "date_to": exp["date_to"],
            "duration": duration_str,
            "skills_after_list": person.get("skills_after_list", []),
            "skills_count": len(skills_after)
        })

df = pd.DataFrame(rows)
print(df.head(10))
