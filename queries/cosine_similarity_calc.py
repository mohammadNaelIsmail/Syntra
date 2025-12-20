import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from elastic_core import fetch_all_documents 

def build_experience_dataframe(documents):
    """
    يبني DataFrame لكل تجربة عمل لكل شخص.
    """
    rows = []
    for person in documents:
        for exp in person.get("experiences", []):
            date_from = pd.to_datetime(exp.get("date_from")) if exp.get("date_from") else None
            date_to = pd.to_datetime(exp.get("date_to")) if exp.get("date_to") else datetime.today()
            
            if date_from:
                delta = relativedelta(date_to, date_from)
                total_months = delta.years * 12 + delta.months
                duration_str = f"{delta.years} years - {delta.months} months - {delta.days} days"
            else:
                total_months = -1
                duration_str = None

            rows.append({
                "person_id": person.get("person_id"),
                "name": person.get("name"),
                "company": exp.get("company_name"),
                "duration": duration_str,
                "total_months": total_months
            })

    df = pd.DataFrame(rows)
    return df

def compute_longest_work_time(df):
    """
    يحسب أطول مدة عمل لكل شخص ويظهر الشركة المرتبطة بها.
    """
    longest = df.loc[df.groupby("person_id")["total_months"].idxmax()]
    result = longest.set_index("person_id")[["name", "company", "total_months"]]
    result.rename(columns={"company": "longest_company"}, inplace=True)
    return result

if __name__ == "__main__":
    docs = fetch_all_documents()  
    df = build_experience_dataframe(docs)
    
    print("first 10 rows")
    print(df.head(10))
    print(f"num of rows: {len(df)}\n")

    longest_df = compute_longest_work_time(df)
    print("most work time for each person:")
    print(longest_df.head(10))
