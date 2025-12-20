import pandas as pd
from elastic_core import fetch_filtered_documents

docs = fetch_filtered_documents(company="Google")


# --- User Inputs ---
target_company = input("Filter by company? (y/n): ").strip().lower() == 'y'
company_name = input("Enter company name: ").strip() if target_company else None

target_skill = input("Filter by skill? (y/n): ").strip().lower() == 'y'
skill_name = input("Enter skill name: ").strip().lower() if target_skill else None

target_period = input("Filter by period? (y/n): ").strip().lower() == 'y'
start_date = pd.to_datetime(input("Enter start date (YYYY-MM-DD): ").strip()) if target_period else None
end_date = pd.to_datetime(input("Enter end date (YYYY-MM-DD): ").strip()) if target_period else None

# --- Fetch documents ---
docs = fetch_filtered_documents(
    company=company_name if target_company else None,
    date_from=start_date.strftime("%Y-%m-%d") if target_period else None,
    date_to=end_date.strftime("%Y-%m-%d") if target_period else None
)

rows = []
seen_experiences = set()  # Deduplicate

for person in docs:
    skills_after = [s.strip().lower() for s in person.get("skills_after_list", [])]

    if target_skill and skill_name not in skills_after:
        continue

    experiences = person.get("inner_hits_experiences", person.get("experiences", []))
    for exp in experiences:
        date_from = pd.to_datetime(exp["date_from"])
        date_to = pd.to_datetime(exp["date_to"])

        if target_period and (date_to < start_date or date_from > end_date):
            continue

        # Deduplicate by person + company + date_from + date_to
        exp_id = (person["person_id"], exp["company_name"], str(date_from), str(date_to))
        if exp_id in seen_experiences:
            continue
        seen_experiences.add(exp_id)

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
if df.empty:
    print("No matching records found.")
else:
    print(df.head(20))  