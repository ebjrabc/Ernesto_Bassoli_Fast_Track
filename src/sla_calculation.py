# src/sla_calculation.py
"""
SLA calculation utilities.

This module contains small, reusable functions that:
- calculate resolution hours counting only business days,
- map ticket priority to expected SLA hours,
- check whether the SLA was met.

All comments are written in plain English to help a non-technical reader.
"""

from __future__ import annotations
# Import datetime helpers for working with dates and times.
from datetime import datetime, timedelta, time
# Import typing helpers to annotate optional values and sets.
from typing import Optional, Set
# pandas is used for parsing and validating timestamps.
import pandas as pd
# numpy is used for NaN values and numeric helpers.
import numpy as np
# holidays provides Brazilian national holiday dates.
import holidays


def calculate_resolution_hours_business_days(
    created_at: pd.Timestamp,
    resolved_at: pd.Timestamp,
    br_holidays: Optional[Set[datetime.date]] = None,
) -> float:
    """
    Calculate how many hours a ticket took to be resolved, counting only business days.

    Business days are Monday through Friday, excluding Brazilian national holidays.
    The function returns a floating point number representing hours.
    If inputs are invalid or missing, the function returns numpy.nan.
    """
    # If either timestamp is missing, we cannot compute a meaningful SLA.
    if pd.isna(created_at) or pd.isna(resolved_at):
        return np.nan

    # Convert inputs to pandas Timestamps in UTC to ensure consistent parsing.
    start = pd.to_datetime(created_at, errors="coerce", utc=True)
    end = pd.to_datetime(resolved_at, errors="coerce", utc=True)

    # If parsing failed for either timestamp, return NaN.
    if pd.isna(start) or pd.isna(end):
        return np.nan

    # If the resolved time is earlier than or equal to the created time,
    # treat the duration as zero hours to avoid negative values.
    if end <= start:
        return 0.0

    # Convert timezone-aware timestamps to naive UTC datetimes for simple arithmetic.
    # This removes timezone objects but keeps the times in UTC.
    start_naive = start.tz_convert("UTC").tz_localize(None)
    end_naive = end.tz_convert("UTC").tz_localize(None)

    # If the caller did not provide a set of holiday dates, build one for the years
    # that appear between the start and end timestamps.
    if br_holidays is None:
        years = list(range(start_naive.year, end_naive.year + 1))
        # holidays.Brazil returns a mapping of date -> holiday name; we take the keys.
        br_holidays = set(holidays.Brazil(years=years).keys())

    # Initialize a counter for total seconds that fall on business days.
    total_seconds = 0.0

    # current_day is the midnight of the start date; last_day is midnight of the end date.
    current_day = start_naive.normalize()
    last_day = end_naive.normalize()

    # Iterate day by day from the start date to the end date (inclusive).
    while current_day <= last_day:
        # weekday() returns 0 for Monday through 6 for Sunday.
        is_weekday = current_day.weekday() < 5
        # Check if the current day is a Brazilian national holiday.
        is_holiday = current_day.date() in br_holidays

        # Only count time for days that are weekdays and not holidays.
        if is_weekday and not is_holiday:
            # Define the full span of the current day (00:00:00 to 23:59:59.999999).
            day_start = datetime.combine(current_day.date(), time.min)
            day_end = datetime.combine(current_day.date(), time.max)

            # The actual interval we should count is the overlap between:
            # - the ticket's [start_naive, end_naive] interval, and
            # - the current day's [day_start, day_end] interval.
            interval_start = max(start_naive.to_pydatetime(), day_start)
            interval_end = min(end_naive.to_pydatetime(), day_end)

            # If there is a positive overlap, add the overlapping seconds to the total.
            if interval_end > interval_start:
                total_seconds += (interval_end - interval_start).total_seconds()

        # Move to the next calendar day.
        current_day += timedelta(days=1)

    # Convert total seconds to hours and return the result.
    return total_seconds / 3600.0


def get_sla_expected(priority: Optional[str]) -> Optional[float]:
    """
    Map ticket priority to expected SLA hours.

    Rules:
    - 'High'   -> 24 hours
    - 'Medium' -> 72 hours
    - 'Low'    -> 120 hours

    If priority is missing or not recognized, return None.
    """
    # If priority is missing, return None to indicate unknown expected SLA.
    if priority is None:
        return None

    # Normalize the priority string: convert to lowercase and strip whitespace.
    priority_normalized = str(priority).strip().lower()

    # Return the expected SLA hours according to the mapping rules.
    if priority_normalized == "high":
        return 24.0
    if priority_normalized == "medium":
        return 72.0
    if priority_normalized == "low":
        return 120.0

    # If the priority value is not one of the expected labels, return None.
    return None


def check_sla_compliance(
    resolution_hours: Optional[float], sla_expected_hours: Optional[float]
) -> Optional[bool]:
    """
    Determine whether the SLA was met.

    Returns:
    - True if resolution_hours <= sla_expected_hours
    - False if resolution_hours > sla_expected_hours
    - None if either value is missing (unknown)
    """
    # If resolution_hours is missing or NaN, we cannot decide.
    if resolution_hours is None or pd.isna(resolution_hours):
        return None

    # If expected SLA is missing or NaN, we cannot decide.
    if sla_expected_hours is None or pd.isna(sla_expected_hours):
        return None

    # Compare numeric values and return the boolean result.
    return float(resolution_hours) <= float(sla_expected_hours)