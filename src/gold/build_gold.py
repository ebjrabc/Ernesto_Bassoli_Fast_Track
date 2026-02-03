"""Gold layer with SLA calculation and reports.

Reads silver parquet, computes SLA metrics, saves gold parquet and Excel,
and generates aggregated reports.
"""

from __future__ import annotations

# Bring in Path to build file paths.
from pathlib import Path

# Bring in pandas for data manipulation.
import pandas as pd

# Import SLA helper functions from the sla_calculation module.
from src.sla_calculation import (
    calculate_resolution_hours_business_days,
    get_sla_expected,
    check_sla_compliance,
)

# Project-relative paths for Silver input and Gold outputs.
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
GOLD_FILE = GOLD_DIR / "gold_sla_issues.parquet"
GOLD_EXCEL = GOLD_DIR / "gold_sla_issues.xlsx"
REPORT_ANALYST = GOLD_DIR / "gold_sla_by_analyst.xlsx"
REPORT_TYPE = GOLD_DIR / "gold_sla_by_issue_type.xlsx"
REPORT_DISTRIBUTION = GOLD_DIR / "gold_sla_distribution.xlsx"


# Build the Gold layer: compute SLA metrics and generate reports.
def build_gold() -> dict[str, Path]:
    """Build Gold layer with SLA calculation and aggregated reports."""
    df = pd.read_parquet(SILVER_FILE)

    df = df[df["status"].isin(["Done", "Resolved"])].copy()
    df = df[df["resolved_at"].notna()].copy()

    for col in ["created_at", "resolved_at", "extracted_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    df["resolution_hours"] = calculate_resolution_hours_business_days(df)
    df["sla_expected_hours"] = df["priority"].apply(get_sla_expected)
    df["is_sla_met"] = check_sla_compliance(df)

    for col in ["created_at", "resolved_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(GOLD_FILE, index=False)

    # Prepare Excel: convert ISO strings back to naive datetimes (no timezone).
    excel_df = df.copy()

    # Ensure timezone-aware datetimes are converted to timezone-naive datetimes.
    for col in excel_df.columns:
        if pd.api.types.is_datetime64tz_dtype(excel_df[col]):
            excel_df[col] = excel_df[col].dt.tz_convert("UTC").dt.tz_localize(None)
        elif pd.api.types.is_datetime64_any_dtype(excel_df[col]):
            try:
                excel_df[col] = excel_df[col].dt.tz_localize(None)
            except Exception:
                excel_df[col] = pd.to_datetime(excel_df[col], errors="coerce").dt.tz_localize(None)
        else:
            pass

    excel_df.to_excel(GOLD_EXCEL, index=False)

    df_by_analyst = df.groupby("assignee_name").agg(
        issue_count=("issue_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
    ).reset_index()
    df_by_analyst.to_excel(REPORT_ANALYST, index=False)

    df_by_type = df.groupby("issue_type").agg(
        issue_count=("issue_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
    ).reset_index()
    df_by_type.to_excel(REPORT_TYPE, index=False)

    df_distribution = df.groupby("is_sla_met").agg(issue_count=("issue_id", "count")).reset_index()
    df_distribution["percentage"] = (
        df_distribution["issue_count"] / df_distribution["issue_count"].sum() * 100
    ).round(2)
    df_distribution.to_excel(REPORT_DISTRIBUTION, index=False)

    print(f"Gold file generated at: {GOLD_FILE}")
    print(f"Gold Excel generated at: {GOLD_EXCEL}")
    print(f"Report by analyst generated at: {REPORT_ANALYST}")
    print(f"Report by issue type generated at: {REPORT_TYPE}")
    print(f"Report SLA distribution generated at: {REPORT_DISTRIBUTION}")

    return {
        "gold_file": GOLD_FILE,
        "gold_excel": GOLD_EXCEL,
        "gold_sla_by_analyst": REPORT_ANALYST,
        "gold_sla_by_issue_type": REPORT_TYPE,
        "gold_sla_distribution": REPORT_DISTRIBUTION,
    }


# Allow running this module directly for testing.
if __name__ == "__main__":
    build_gold()