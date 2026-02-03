# Indicate that future type annotations are enabled.
from __future__ import annotations

# Import Path to build project-relative file paths.
from pathlib import Path
# Import pandas for data manipulation.
import pandas as pd

# Import SLA helper functions from the sla_calculation module.
from src.sla_calculation import (
    calculate_resolution_hours_business_days,
    get_sla_expected,
    check_sla_compliance,
)

# Define the project root directory.
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
# Define the Silver input parquet file path.
SILVER_FILE = PROJECT_ROOT / "data" / "silver" / "silver_issues.parquet"
# Define the Gold output directory path.
GOLD_DIR = PROJECT_ROOT / "data" / "gold"
# Define the Gold parquet output file path.
GOLD_FILE = GOLD_DIR / "gold_sla_issues.parquet"
# Define the Gold Excel output file path.
GOLD_EXCEL = GOLD_DIR / "gold_sla_issues.xlsx"
# Define the aggregated report file paths.
REPORT_ANALYST = GOLD_DIR / "gold_sla_by_analyst.xlsx"
REPORT_TYPE = GOLD_DIR / "gold_sla_by_issue_type.xlsx"
REPORT_DISTRIBUTION = GOLD_DIR / "gold_sla_distribution.xlsx"


# Build the Gold layer: compute SLA metrics and generate reports.
def build_gold() -> dict[str, Path]:
    # Read the Silver parquet file into a pandas DataFrame.
    df = pd.read_parquet(SILVER_FILE)

    # Filter issues to those with status Done or Resolved.
    df = df[df["status"].isin(["Done", "Resolved"])].copy()
    # Keep only rows that have a resolved_at timestamp.
    df = df[df["resolved_at"].notna()].copy()

    # Convert date-like columns to timezone-aware UTC datetimes.
    for col in ["created_at", "resolved_at", "extracted_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Calculate resolution hours in business days using the helper function.
    df["resolution_hours"] = calculate_resolution_hours_business_days(df)
    # Map priority to expected SLA hours using the helper function.
    df["sla_expected_hours"] = df["priority"].apply(get_sla_expected)
    # Determine SLA compliance using the helper function (boolean).
    df["is_sla_met"] = check_sla_compliance(df)

    # Map boolean SLA compliance to Portuguese labels: 'atendido' or 'violado'.
    df["is_sla_met"] = df["is_sla_met"].map({True: "atendido", False: "violado"})

    # Convert datetime columns to ISO strings for stable export.
    for col in ["created_at", "resolved_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True).dt.strftime(
                "%Y-%m-%dT%H:%M:%SZ"
            )

    # Ensure the Gold output directory exists.
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    # Save the Gold DataFrame to parquet.
    df.to_parquet(GOLD_FILE, index=False)

    # Prepare an Excel-friendly DataFrame by converting timezone-aware datetimes
    # to naive datetimes (no timezone) for Excel compatibility.
    excel_df = df.copy()
    for col in excel_df.columns:
        if pd.api.types.is_datetime64tz_dtype(excel_df[col]):
            excel_df[col] = excel_df[col].dt.tz_convert("UTC").dt.tz_localize(None)
        elif pd.api.types.is_datetime64_any_dtype(excel_df[col]):
            try:
                excel_df[col] = excel_df[col].dt.tz_localize(None)
            except Exception:
                excel_df[col] = pd.to_datetime(
                    excel_df[col], errors="coerce"
                ).dt.tz_localize(None)
        else:
            pass

    # Save the main Gold Excel file.
    excel_df.to_excel(GOLD_EXCEL, index=False)

    # Generate aggregated report by analyst and save to Excel.
    df_by_analyst = df.groupby("assignee_name").agg(
        issue_count=("issue_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
    ).reset_index()
    df_by_analyst.to_excel(REPORT_ANALYST, index=False)

    # Generate aggregated report by issue type and save to Excel.
    df_by_type = df.groupby("issue_type").agg(
        issue_count=("issue_id", "count"),
        avg_resolution_hours=("resolution_hours", "mean"),
    ).reset_index()
    df_by_type.to_excel(REPORT_TYPE, index=False)

    # Generate SLA distribution report (using 'atendido'/'violado') and save to Excel.
    df_distribution = (
        df.groupby("is_sla_met")
        .agg(issue_count=("issue_id", "count"))
        .reset_index()
    )
    # Compute percentage column for distribution report.
    df_distribution["percentage"] = (
        df_distribution["issue_count"] / df_distribution["issue_count"].sum() * 100
    ).round(2)
    df_distribution.to_excel(REPORT_DISTRIBUTION, index=False)

    # Return a dictionary of generated output file paths.
    return {
        "gold_file": GOLD_FILE,
        "gold_excel": GOLD_EXCEL,
        "gold_sla_by_analyst": REPORT_ANALYST,
        "gold_sla_by_issue_type": REPORT_TYPE,
        "gold_sla_distribution": REPORT_DISTRIBUTION,
    }


# Allow running the Gold build directly for testing.
if __name__ == "__main__":
    build_gold()