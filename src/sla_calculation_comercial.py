"""SLA calculation utilities (vectorized)."""

import pandas as pd
import numpy as np
import requests
from datetime import datetime, time

HOLIDAYS_API = "https://brasilapi.com.br/api/feriados/v1/{year}"


def get_holidays(years: list[int]) -> set:
    """Fetch national holidays for given years."""
    holidays = set()
    for year in years:
        response = requests.get(HOLIDAYS_API.format(year=year))
        for h in response.json():
            holidays.add(datetime.strptime(h["date"], "%Y-%m-%d").date())
    return holidays


def calculate_resolution_hours_vectorized(df: pd.DataFrame) -> pd.Series:
    """
    Vectorized calculation of resolution time in business hours (07:00–18:00),
    excluding weekends and national holidays.
    """
    # Ensure datetime in UTC
    df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
    df["resolved_at"] = pd.to_datetime(df["resolved_at"], utc=True)

    # Collect all years present
    years = list(set(df["created_at"].dt.year) | set(df["resolved_at"].dt.year))
    holidays = get_holidays(years)

    # Business day mask
    bdays = pd.bdate_range(df["created_at"].min().normalize(),
                           df["resolved_at"].max().normalize(),
                           freq="C", holidays=holidays)

    # Map each date to business day flag
    bday_set = set(bdays.date)

    # Vectorized function to compute hours
    def compute_hours(start, end):
        if pd.isna(start) or pd.isna(end):
            return np.nan

        total_hours = 0
        current = start.normalize()

        while current <= end.normalize():
            if current.weekday() < 5 and current.date() not in holidays:
                work_start = pd.Timestamp(datetime.combine(current.date(), time(7, 0))).tz_localize(start.tzinfo)
                work_end   = pd.Timestamp(datetime.combine(current.date(), time(18, 0))).tz_localize(start.tzinfo)

                day_start = max(work_start, start)
                day_end   = min(work_end, end)

                if day_end > day_start:
                    total_hours += (day_end - day_start).total_seconds() / 3600

            current += pd.Timedelta(days=1)

        return round(total_hours, 2)

    return df.apply(lambda row: compute_hours(row["created_at"], row["resolved_at"]), axis=1)


def get_sla_expected_vectorized(df: pd.DataFrame) -> pd.Series:
    """Vectorized SLA expected hours based on priority."""
    mapping = {"High": 24, "Medium": 72, "Low": 120}
    return df["priority"].str.title().map(mapping)


def check_sla_compliance_vectorized(df: pd.DataFrame) -> pd.Series:
    """Vectorized SLA compliance check."""
    return df["resolution_hours"] <= df["sla_expected_hours"]