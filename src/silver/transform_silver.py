"""Silver layer.

Reads bronze parquet, normalizes and cleans data, saves silver parquet.
Removes timezone before writing Excel (Excel does not support tz-aware datetimes).
"""

from __future__ import annotations

# Bring in Path to build file paths.
from pathlib import Path

# Bring in pandas for data manipulation.
import pandas as pd

# Project-relative paths for Bronze input and Silver output.
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
BRONZE_FILE = PROJECT_ROOT / "data" / "bronze" / "bronze_issues.parquet"
SILVER_DIR = PROJECT_ROOT / "data" / "silver"
SILVER_FILE = SILVER_DIR / "silver_issues.parquet"
SILVER_EXCEL = SILVER_DIR / "silver_issues.xlsx"


# Run the Silver layer: read Bronze, normalize, save Silver.
def run_silver() -> Path:
    # Read the Bronze parquet file into a DataFrame.
    df = pd.read_parquet(BRONZE_FILE)

    # Map possible variant column names to the standardized names.
    rename_map = {
        "issuesid": "issue_id",
        "issuesissue_type": "issue_type",
        "issuesstatus": "status",
        "issuespriority": "priority",
        "resp_id": "assignee_id",
        "resp_name": "assignee_name",
        "resp_email": "assignee_email",
        "timestamps_created_at": "created_at",
        "timestamps_resolved_at": "resolved_at",
    }
    # Rename columns if any of the variant names exist.
    df = df.rename(columns=rename_map)

    # Ensure date columns are parsed as timezone-aware UTC datetimes.
    for col in ["created_at", "resolved_at", "extracted_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    # Make text columns more readable (Title Case) without changing data meaning.
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(lambda x: x.title() if isinstance(x, str) else x)

    # Create Silver directory and save the canonical Silver parquet file.
    SILVER_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(SILVER_FILE, index=False)

    # Prepare an Excel-friendly copy by removing timezone info from datetimes.
    excel_df = df.copy()
    for col in ["created_at", "resolved_at", "extracted_at"]:
        if col in excel_df.columns and pd.api.types.is_datetime64_any_dtype(excel_df[col]):
            excel_df[col] = excel_df[col].dt.tz_localize(None)

    # If you want an Excel file for manual inspection, uncomment the line below.
    # excel_df.to_excel(SILVER_EXCEL, index=False)

    print(f"Silver file generated at: {SILVER_FILE}")
    return SILVER_FILE


# Allow running this module directly for testing.
if __name__ == "__main__":
    run_silver()