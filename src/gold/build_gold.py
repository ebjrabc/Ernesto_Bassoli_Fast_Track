# src/gold/build_gold.py
"""
Build Gold layer: produce the final SLA table and aggregated reports.

This script:
- reads the silver parquet file,
- filters out tickets that are Open or missing resolved_at,
- keeps only Done and Resolved tickets,
- computes resolution hours counting only business days and excluding Brazilian national holidays,
- maps priority to expected SLA hours,
- checks SLA compliance and labels results in Portuguese,
- writes a parquet and an Excel file with the final table,
- writes two aggregated XLSX reports: SLA by analyst and SLA by issue type.
"""

from __future__ import annotations
# Path helps build file system paths relative to the project root.
from pathlib import Path
from typing import Dict
# pandas for data manipulation.
import pandas as pd
# numpy for NaN values.
import numpy as np
# datetime to get the current year if needed.
from datetime import datetime
# holidays to build Brazilian national holiday dates.
import holidays

# Import the SLA helper functions implemented in src/sla_calculation.py.
from src.sla_calculation import (
    calculate_resolution_hours_business_days,
    get_sla_expected,
    check_sla_compliance,
)

# Define the project root relative to this file.
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
# Define the expected location of the silver parquet input file.
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"
# Define the gold output directory.
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
# Define the gold parquet output file path.
GOLD_FILE = GOLD_DIR / "gold_sla_issues.parquet"
# Define the gold Excel output file path.
GOLD_EXCEL = GOLD_DIR / "gold_sla_issues.xlsx"
# Define the aggregated report file paths.
REPORT_ANALYST = GOLD_DIR / "gold_sla_by_analyst.xlsx"
REPORT_TYPE = GOLD_DIR / "gold_sla_by_issue_type.xlsx"


def build_gold() -> Dict[str, Path]:
    """
    Run the gold build and return a dictionary with paths to generated files.
    """
    # Read the silver parquet into a pandas DataFrame.
    df = pd.read_parquet(SILVER_FILE)

    # If the status column exists, remove rows where status is missing.
    if "status" in df.columns:
        df = df[df["status"].notna()].copy()

        # Remove rows where status is exactly "Open" because Open tickets
        # do not have a resolution and must not be considered.
        df = df[df["status"] != "Open"].copy()

    # Keep only tickets whose status is "Done" or "Resolved".
    # These are the tickets that should appear in the final SLA table.
    df = df[df["status"].isin(["Done", "Resolved"])].copy()

    # Remove rows where resolved_at is missing because we cannot compute SLA without it.
    if "resolved_at" in df.columns:
        df = df[df["resolved_at"].notna()].copy()

    # Convert date-like columns to timezone-aware UTC datetimes for consistent calculations.
    for col in ["created_at", "resolved_at", "extracted_at"]:
        if col in df.columns:
            # errors="coerce" will set invalid values to NaT (missing).
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Determine which years appear in the data so we can build the holiday set for those years.
    years = set()
    for col in ["created_at", "resolved_at"]:
        if col in df.columns:
            # Extract years from the column, drop missing values, convert to int, and add to the set.
            years.update(
                pd.to_datetime(df[col], errors="coerce", utc=True)
                .dt.year.dropna()
                .astype(int)
                .unique()
                .tolist()
            )

    # If no years were found (empty dataset), default to the current year.
    if not years:
        years = {datetime.utcnow().year}

    # Build a set of Brazilian national holiday dates for the relevant years.
    br_holidays = set(holidays.Brazil(years=sorted(years)).keys())

    # Define a small helper that computes resolution hours for a single row using the SLA helper.
    def _compute_resolution_hours(row: pd.Series) -> float:
        # Pass the created_at and resolved_at timestamps and the holiday set.
        return calculate_resolution_hours_business_days(
            row["created_at"], row["resolved_at"], br_holidays
        )

    # Apply the helper row-wise to compute the resolution_hours column.
    df["resolution_hours"] = df.apply(_compute_resolution_hours, axis=1)

    # Map the priority column to expected SLA hours using the helper function.
    if "priority" in df.columns:
        df["sla_expected_hours"] = df["priority"].apply(get_sla_expected)
    else:
        # If priority is missing, set expected SLA to NaN.
        df["sla_expected_hours"] = np.nan

    # Determine SLA compliance for each row using the helper function.
    df["is_sla_met"] = df.apply(
        lambda r: check_sla_compliance(r["resolution_hours"], r["sla_expected_hours"]),
        axis=1,
    )

    # Convert boolean compliance results to Portuguese labels:
    # True -> "atendido" (met), False -> "violado" (violated), None remains None.
    df["is_sla_met"] = df["is_sla_met"].map({True: "atendido", False: "violado", None: None})

    # Convert datetime columns to ISO 8601 UTC strings for stable export and readability.
    for col in ["created_at", "resolved_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Ensure the gold directory exists; create it if necessary.
    GOLD_DIR.mkdir(parents=True, exist_ok=True)

    # Save the final SLA table to a parquet file for efficient storage and downstream use.
    df.to_parquet(GOLD_FILE, index=False)

    # Prepare an Excel-friendly copy of the DataFrame.
    excel_df = df.copy()
    for col in excel_df.columns:
        # If a column is timezone-aware, convert to UTC and remove timezone info.
        if pd.api.types.is_datetime64tz_dtype(excel_df[col]):
            excel_df[col] = excel_df[col].dt.tz_convert("UTC").dt.tz_localize(None)
        elif pd.api.types.is_datetime64_any_dtype(excel_df[col]):
            # If it's a datetime without tz, try to remove tz localization safely.
            try:
                excel_df[col] = excel_df[col].dt.tz_localize(None)
            except Exception:
                # If localization fails, coerce to datetime and remove tz.
                excel_df[col] = pd.to_datetime(excel_df[col], errors="coerce").dt.tz_localize(None)

    # Save the Excel file with the final SLA table.
    excel_df.to_excel(GOLD_EXCEL, index=False)

    # Build the aggregated report: SLA average by analyst.
    if "assignee_name" in df.columns:
        # Group by assignee_name, count issues, and compute average resolution hours.
        df_by_analyst = (
            df.groupby("assignee_name")
            .agg(issue_count=("issue_id", "count"), avg_resolution_hours=("resolution_hours", "mean"))
            .reset_index()
        )
    else:
        # If the column is missing, create an empty DataFrame with the expected columns.
        df_by_analyst = pd.DataFrame(columns=["assignee_name", "issue_count", "avg_resolution_hours"])

    # Save the analyst report to XLSX.
    df_by_analyst.to_excel(REPORT_ANALYST, index=False)

    # Build the aggregated report: SLA average by issue type.
    if "issue_type" in df.columns:
        # Group by issue_type, count issues, and compute average resolution hours.
        df_by_type = (
            df.groupby("issue_type")
            .agg(issue_count=("issue_id", "count"), avg_resolution_hours=("resolution_hours", "mean"))
            .reset_index()
        )
    else:
        # If the column is missing, create an empty DataFrame with the expected columns.
        df_by_type = pd.DataFrame(columns=["issue_type", "issue_count", "avg_resolution_hours"])

    # Save the issue type report to XLSX.
    df_by_type.to_excel(REPORT_TYPE, index=False)

    # Return a dictionary with paths to the generated files so callers can find them.
    return {
        "gold_file": GOLD_FILE,
        "gold_excel": GOLD_EXCEL,
        "gold_sla_by_analyst": REPORT_ANALYST,
        "gold_sla_by_issue_type": REPORT_TYPE,
    }


# If this script is executed directly, run the build and print the generated file paths.
if __name__ == "__main__":
    outputs = build_gold()
    print("Generated files:", outputs)