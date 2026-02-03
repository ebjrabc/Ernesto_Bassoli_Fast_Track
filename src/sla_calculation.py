"""SLA calculation utilities.

Calculates resolution time in business days (each business day = 24 hours),
excluding weekends and national holidays.
"""

from __future__ import annotations

# Bring in date/time helpers.
from datetime import datetime, timedelta
# Bring in typing helpers.
from typing import Iterable, Set

# Bring in numeric and table libraries.
import numpy as np
import pandas as pd
# Bring in requests to call the public holidays API.
import requests

# Public API URL for Brazilian national holidays (year will be formatted).
HOLIDAYS_API = "https://brasilapi.com.br/api/feriados/v1/{year}"


# Fetch national holidays for the given years and return a set of date objects.
def fetch_national_holidays(years: Iterable[int]) -> Set[datetime.date]:
    holidays: Set[datetime.date] = set()
    for year in sorted(set(years)):
        try:
            resp = requests.get(HOLIDAYS_API.format(year=year), timeout=10)
            resp.raise_for_status()
            data = resp.json()
            for item in data:
                try:
                    holidays.add(datetime.strptime(item["date"], "%Y-%m-%d").date())
                except Exception:
                    # Skip malformed entries.
                    continue
        except requests.RequestException:
            # If the API fails for a year, skip holidays for that year (fail-safe).
            continue
    return holidays


# Count business days between two timestamps (inclusive), excluding weekends and holidays.
def _business_days_between(start: pd.Timestamp, end: pd.Timestamp, holidays: Set[datetime.date]) -> int:
    if pd.isna(start) or pd.isna(end):
        return 0
    start_date = start.normalize().date()
    end_date = end.normalize().date()
    if start_date > end_date:
        return 0
    days = 0
    current = start_date
    while current <= end_date:
        # Weekday < 5 means Monday-Friday.
        if current.weekday() < 5 and current not in holidays:
            days += 1
        current += timedelta(days=1)
    return days


# Calculate resolution hours per row as business days * 24.
def calculate_resolution_hours_business_days(df: pd.DataFrame) -> pd.Series:
    # Parse created_at and resolved_at as timezone-aware UTC datetimes.
    created = pd.to_datetime(df["created_at"], errors="coerce", utc=True)
    resolved = pd.to_datetime(df["resolved_at"], errors="coerce", utc=True)

    # Collect all years present in the dates to fetch holidays once per year.
    years = set(created.dt.year.dropna().astype(int).tolist()) | set(resolved.dt.year.dropna().astype(int).tolist())
    holidays = fetch_national_holidays(list(years))

    # Compute hours for a single row.
    def compute_row_hours(s, e):
        if pd.isna(s) or pd.isna(e):
            return np.nan
        bdays = _business_days_between(s, e, holidays)
        return float(bdays * 24)

    # Apply the computation row by row and return a pandas Series.
    return pd.Series([compute_row_hours(s, e) for s, e in zip(created, resolved)], index=df.index)


# Map priority strings to expected SLA hours.
def get_sla_expected(priority: str) -> float:
    mapping = {"High": 24.0, "Medium": 72.0, "Low": 120.0}
    return mapping.get(str(priority).title(), np.nan)


# Return a boolean Series indicating whether SLA was met (True) or violated (False).
def check_sla_compliance(df: pd.DataFrame) -> pd.Series:
    res = df["resolution_hours"]
    expected = df["sla_expected_hours"]
    return (res <= expected) & res.notna() & expected.notna()