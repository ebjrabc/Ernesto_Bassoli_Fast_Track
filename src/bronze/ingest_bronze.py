"""Bronze layer.

Reads raw Jira JSON and writes a parquet file with selected fields.
"""

from __future__ import annotations

# Bring in JSON reader to parse the input file.
import json
# Bring in Path to build file paths safely.
from pathlib import Path
# Bring in typing helpers for clarity.
from typing import Any, Dict, List

# Bring in pandas to build and save tables.
import pandas as pd

# Define project-relative paths (no hardcoded absolute paths).
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
RESOURCES_DIR = PROJECT_ROOT / "resources"
# Input JSON file path (place your jira_issues_raw.json here).
INPUT_PATH = RESOURCES_DIR / "jira_issues_raw.json"
# Bronze output folder and file path.
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
OUTPUT_FILE = BRONZE_DIR / "bronze_issues.parquet"


# Read the JSON file and return a Python dictionary.
def read_json_file(path: Path = INPUT_PATH) -> Dict[str, Any]:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


# Build a pandas DataFrame with the selected fields in snake_case.
def build_bronze_dataframe(payload: Dict[str, Any]) -> pd.DataFrame:
    # Get top-level project info (may be empty).
    project = payload.get("project", {})
    # Get list of issues (each issue is a dict).
    issues: List[Dict[str, Any]] = payload.get("issues", [])
    # Prepare a list to collect rows.
    rows: List[Dict[str, Any]] = []

    # Loop over each issue and extract the desired fields.
    for issue in issues:
        # Timestamps may be a list or dict; handle both cases.
        timestamps = issue.get("timestamps", {})
        if isinstance(timestamps, list) and timestamps:
            ts = timestamps[0]
        elif isinstance(timestamps, dict):
            ts = timestamps
        else:
            ts = {}

        # Assignee may be a list or dict; handle both cases.
        assignee = issue.get("assignee", {})
        if isinstance(assignee, list) and assignee:
            assignee_info = assignee[0]
        elif isinstance(assignee, dict):
            assignee_info = assignee
        else:
            assignee_info = {}

        # Build a single row with standardized column names.
        row = {
            "project_id": project.get("project_id"),
            "project_name": project.get("project_name"),
            "extracted_at": project.get("extracted_at"),
            "issue_id": issue.get("id"),
            "issue_type": issue.get("issue_type"),
            "status": issue.get("status"),
            "priority": issue.get("priority"),
            "assignee_id": assignee_info.get("id"),
            "assignee_name": assignee_info.get("name"),
            "assignee_email": assignee_info.get("email"),
            "created_at": ts.get("created_at"),
            "resolved_at": ts.get("resolved_at"),
        }
        # Add the row to the list.
        rows.append(row)

    # Convert the list of rows into a pandas DataFrame.
    df = pd.DataFrame(rows)

    # Convert date-like columns to timezone-aware UTC datetimes.
    for col in ["extracted_at", "created_at", "resolved_at"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)

    return df


# Run the Bronze layer: read JSON, build DataFrame, save Parquet, return path.
def run_bronze() -> Path:
    payload = read_json_file()
    df = build_bronze_dataframe(payload)
    BRONZE_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"Bronze file generated at: {OUTPUT_FILE}")
    return OUTPUT_FILE


# Allow running this module directly for testing.
if __name__ == "__main__":
    run_bronze()