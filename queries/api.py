from fastapi import FastAPI, Query
from typing import Optional
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from elastic_core import fetch_all_documents, fetch_filtered_documents
from elasticsearch import Elasticsearch
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="LinkHarvester API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

es  = Elasticsearch(["http://localhost:9200"])

# ── 1. كل الأشخاص ──────────────────────────────────────────────────────────
@app.get("/profiles")
def get_profiles(size: int = 100):
    return fetch_all_documents(size=size)

# ── 2. بحث بالشركة / المهارة / الفترة ─────────────────────────────────────
@app.get("/profiles/search")
def search_profiles(
    company:    Optional[str] = None,
    skill:      Optional[str] = None,
    date_from:  Optional[str] = None,
    date_to:    Optional[str] = None,
    size:       int = 100
):
    docs = fetch_filtered_documents(
        company=company, date_from=date_from, date_to=date_to, size=size
    )
    if skill:
        skill_l = skill.lower()
        docs = [
            d for d in docs
            if skill_l in [s.strip().lower() for s in d.get("skills_after_list", [])]
        ]
    return docs

# ── 3. مدة العمل لكل شخص ───────────────────────────────────────────────────
@app.get("/profiles/work-duration")
def work_duration(size: int = 100):
    docs = fetch_all_documents(size=size)
    rows = []
    for person in docs:
        for exp in person.get("experiences", []):
            date_from = pd.to_datetime(exp.get("date_from")) if exp.get("date_from") else None
            date_to   = pd.to_datetime(exp.get("date_to"))   if exp.get("date_to")   else datetime.today()
            if not date_from:
                continue
            delta = relativedelta(date_to, date_from)
            rows.append({
                "person_id":    person.get("person_id"),
                "name":         person.get("name"),
                "company":      exp.get("company_name"),
                "total_months": delta.years * 12 + delta.months,
                "duration":     f"{delta.years}y {delta.months}m {delta.days}d"
            })
    df = pd.DataFrame(rows)
    if df.empty:
        return []
    longest = df.loc[df.groupby("person_id")["total_months"].idxmax()]
    return longest.to_dict(orient="records")

# ── 4. التحليلات الشهرية ────────────────────────────────────────────────────
@app.get("/analytics/monthly")
def monthly_analytics(month: Optional[str] = None, size: int = 100):
    docs = fetch_all_documents(index_name="people_monthly_analytics", size=size)
    if month:
        docs = [d for d in docs if d.get("month") == month]
    return docs

# ── 5. التحليلات السنوية ────────────────────────────────────────────────────
@app.get("/analytics/yearly")
def yearly_analytics(year: Optional[str] = None, size: int = 100):
    docs = fetch_all_documents(index_name="people_yearly_stats", size=size)
    if year:
        docs = [d for d in docs if d.get("year") == year]
    return docs

# ── 6. بحث تفاعلي بالمهارة والشركة والفترة ─────────────────────────────────
@app.get("/profiles/detailed-search")
def detailed_search(
    company:    Optional[str] = None,
    skill:      Optional[str] = None,
    date_from:  Optional[str] = None,
    date_to:    Optional[str] = None,
    size:       int = 100
):
    docs = fetch_filtered_documents(
        company=company, date_from=date_from, date_to=date_to, size=size
    )
    rows = []
    seen = set()
    for person in docs:
        skills_after = [s.lower() for s in person.get("skills_after_list", [])]
        if skill and skill.lower() not in skills_after:
            continue
        for exp in person.get("experiences", []):
            date_from_dt = pd.to_datetime(exp.get("date_from"))
            date_to_dt   = pd.to_datetime(exp.get("date_to")) if exp.get("date_to") else datetime.today()
            exp_id = (person["person_id"], exp.get("company_name"), str(date_from_dt), str(date_to_dt))
            if exp_id in seen:
                continue
            seen.add(exp_id)
            duration_days = (date_to_dt - date_from_dt).days
            rows.append({
                "person_id":        person["person_id"],
                "name":             person["name"],
                "company":          exp.get("company_name"),
                "date_from":        exp.get("date_from"),
                "date_to":          exp.get("date_to"),
                "duration":         f"{duration_days // 365}y {(duration_days % 365) // 30}m {duration_days % 30}d",
                "skills_after_list": person.get("skills_after_list", []),
                "skills_count":     len(skills_after)
            })
    return rows

# ── Mock endpoints (no Elasticsearch needed) ────────────────────────────────
from mock_data import MONTHLY, PROFILES, DURATION

@app.get("/mock/analytics/monthly")
def mock_monthly(): return [MONTHLY]

@app.get("/mock/profiles/detailed-search")
def mock_search(company: str = None, skill: str = None):
    results = PROFILES
    if company:
        results = [r for r in results if company.lower() in r["company"].lower()]
    if skill:
        results = [r for r in results if skill.lower() in [s.lower() for s in r["skills_after_list"]]]
    return results

@app.get("/mock/profiles/work-duration")
def mock_duration(): return DURATION
