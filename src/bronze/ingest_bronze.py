# Indicate that future type annotations are enabled.
from __future__ import annotations

# Import json to parse the downloaded JSON file.
import json
# Import os to access environment variables.
import os
# Import re to parse .gitignore fallback lines.
import re
# Import sys to exit the process on fatal errors.
import sys
# Import time to implement retry backoff delays.
import time
# Import Path to build and manipulate filesystem paths.
from pathlib import Path
# Import typing helpers for annotations.
from typing import Any, Dict, List, Optional

# Define the project root as the parent of the src folder.
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
# Define the path to .gitignore used as an optional insecure fallback.
GITIGNORE_PATH = PROJECT_ROOT / ".gitignore"
# Define the resources directory and ensure it exists.
RESOURCES_DIR = PROJECT_ROOT / "resources"
RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
# Define the expected input JSON path inside resources.
INPUT_PATH = RESOURCES_DIR / "jira_issues_raw.json"
# Define the bronze output directory and ensure it exists.
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
BRONZE_DIR.mkdir(parents=True, exist_ok=True)
# Define the bronze parquet output file path.
OUTPUT_FILE = BRONZE_DIR / "bronze_issues.parquet"

# Compile a regex pattern to parse KEY=VALUE lines from .gitignore fallback.
# Use a raw single-quoted string so internal optional quotes do not break syntax.
_GITIGNORE_KV_PATTERN = re.compile(r'^\s*([A-Z0-9_]+)\s*=\s*("?)(.+?)\2\s*$', re.IGNORECASE)


# Parse KEY=VALUE pairs from a .gitignore file as an insecure fallback.
def parse_credentials_from_gitignore(path: Path) -> Dict[str, str]:
    # Initialize an empty dictionary to hold parsed credentials.
    creds: Dict[str, str] = {}
    # If the .gitignore file does not exist, return an empty dict.
    if not path.exists():
        return creds
    # Open the .gitignore file for reading.
    with path.open("r", encoding="utf-8") as f:
        # Iterate over each line in the file.
        for line in f:
            # Strip whitespace from the line.
            line = line.strip()
            # Skip empty lines, comments, and merge markers.
            if (
                not line
                or line.startswith("#")
                or line.startswith("<<<<<<<")
                or line.startswith("=======")
                or line.startswith(">>>>>>>")
            ):
                continue
            # Attempt to match the KEY=VALUE pattern.
            m = _GITIGNORE_KV_PATTERN.match(line)
            if m:
                # Store the parsed key (uppercased) and value in the dict.
                creds[m.group(1).upper()] = m.group(3)
    # Return the parsed credentials dictionary.
    return creds


# Retrieve Azure credentials from environment variables or .gitignore fallback.
def get_azure_credentials() -> Dict[str, Optional[str]]:
    # Read expected environment variables into a dictionary.
    creds = {
        "ACCOUNT_URL": os.getenv("ACCOUNT_URL"),
        "CONTAINER_NAME": os.getenv("CONTAINER_NAME"),
        "BLOB_NAME": os.getenv("BLOB_NAME"),
        "AZURE_TENANT_ID": os.getenv("AZURE_TENANT_ID"),
        "AZURE_CLIENT_ID": os.getenv("AZURE_CLIENT_ID"),
        "AZURE_CLIENT_SECRET": os.getenv("AZURE_CLIENT_SECRET"),
    }
    # Identify which keys are missing or empty.
    missing = [k for k, v in creds.items() if not v]
    # If any keys are missing, attempt to fill them from .gitignore fallback.
    if missing:
        fallback = parse_credentials_from_gitignore(GITIGNORE_PATH)
        for k in missing:
            if fallback.get(k):
                creds[k] = fallback[k]
    # Return the credentials dictionary (may contain None values).
    return creds


# Build a bronze DataFrame from the raw JSON payload using pandas.
def build_bronze_dataframe(payload: Dict[str, Any], pandas_module) -> "pandas.DataFrame":
    # Extract the project object from the payload or use an empty dict.
    project = payload.get("project", {}) or {}
    # Extract the issues list from the payload or use an empty list.
    issues: List[Dict[str, Any]] = payload.get("issues", []) or []
    # Initialize a list to collect row dictionaries.
    rows: List[Dict[str, Any]] = []
    # Iterate over each issue in the issues list.
    for issue in issues:
        # Extract timestamps which may be a list or a dict.
        timestamps = issue.get("timestamps", {}) or {}
        ts = timestamps[0] if isinstance(timestamps, list) and timestamps else (
            timestamps if isinstance(timestamps, dict) else {}
        )
        # Extract assignee which may be a list or a dict.
        assignee = issue.get("assignee", {}) or {}
        assignee_info = assignee[0] if isinstance(assignee, list) and assignee else (
            assignee if isinstance(assignee, dict) else {}
        )
        # Build a row dictionary with the desired fields in snake_case.
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
        # Append the row dictionary to the rows list.
        rows.append(row)
    # Create a pandas DataFrame from the collected rows.
    df = pandas_module.DataFrame(rows)
    # Convert date-like columns to timezone-aware UTC datetimes when present.
    for col in ["extracted_at", "created_at", "resolved_at"]:
        if col in df.columns:
            df[col] = pandas_module.to_datetime(df[col], errors="coerce", utc=True)
    # Return the constructed DataFrame.
    return df


# Run the Bronze ingestion: download JSON from Azure and write a parquet file.
def run_bronze(timeout: int = 30, max_retries: int = 3, backoff_factor: float = 2.0) -> Path:
    # Attempt to import pandas and Azure SDK modules; assume entrypoint installed them.
    try:
        import pandas as pd  # type: ignore
        from azure.identity import ClientSecretCredential  # type: ignore
        from azure.storage.blob import BlobClient  # type: ignore
    except Exception as exc:
        # If imports fail, print an error and exit with code 1.
        print(f"[ERROR] Required libraries not available in Bronze layer: {exc}")
        sys.exit(1)

    # Retrieve Azure credentials from environment or fallback.
    creds = get_azure_credentials()
    # Define the list of required credential keys.
    required_keys = [
        "ACCOUNT_URL",
        "CONTAINER_NAME",
        "BLOB_NAME",
        "AZURE_TENANT_ID",
        "AZURE_CLIENT_ID",
        "AZURE_CLIENT_SECRET",
    ]
    # Identify any missing credential keys.
    missing_keys = [k for k in required_keys if not creds.get(k)]
    # If any required credentials are missing, print an error and exit.
    if missing_keys:
        print(
            f"[ERROR] Azure credentials missing: {missing_keys}. "
            "Set environment variables or provide them via .gitignore fallback."
        )
        sys.exit(1)

    # Internal helper to download a blob to a local path with retries and backoff.
    def fetch_blob_to_local_with_retries(
        account_url: str,
        container_name: str,
        blob_name: str,
        tenant_id: str,
        client_id: str,
        client_secret: str,
        dest_path: Path,
        timeout: int = 30,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ) -> None:
        # Create a ClientSecretCredential for service principal authentication.
        credential = ClientSecretCredential(
            tenant_id=tenant_id, client_id=client_id, client_secret=client_secret
        )
        # Create a BlobClient for the specific blob to download.
        blob_client = BlobClient(
            account_url=account_url,
            container_name=container_name,
            blob_name=blob_name,
            credential=credential,
        )
        # Initialize the attempt counter.
        attempt = 0
        # Loop until max_retries attempts are exhausted.
        while attempt < max_retries:
            attempt += 1
            try:
                # Download the blob; StorageStreamDownloader is not a context manager.
                stream = blob_client.download_blob(timeout=timeout)
                # Read all bytes from the stream.
                data = stream.readall()
                # Write the bytes to the destination path.
                dest_path.write_bytes(data)
                # Return on successful download.
                return
            except Exception as exc:
                # If an attempt fails, and more attempts remain, wait with exponential backoff.
                if attempt < max_retries:
                    wait = backoff_factor ** attempt
                    time.sleep(wait)
                else:
                    # If all attempts fail, raise a ConnectionError.
                    raise ConnectionError(
                        f"Failed to download blob '{blob_name}' after {max_retries} attempts."
                    ) from exc

    # Attempt to download the JSON blob from Azure to the local resources path.
    try:
        fetch_blob_to_local_with_retries(
            account_url=creds["ACCOUNT_URL"],
            container_name=creds["CONTAINER_NAME"],
            blob_name=creds["BLOB_NAME"],
            tenant_id=creds["AZURE_TENANT_ID"],
            client_id=creds["AZURE_CLIENT_ID"],
            client_secret=creds["AZURE_CLIENT_SECRET"],
            dest_path=INPUT_PATH,
            timeout=timeout,
            max_retries=max_retries,
            backoff_factor=backoff_factor,
        )
    except Exception as exc:
        # On download failure, print an error and exit.
        print(f"[ERROR] Failed to download blob: {exc}")
        sys.exit(1)

    # Attempt to read the downloaded JSON file into a Python object.
    try:
        with INPUT_PATH.open("r", encoding="utf-8") as f:
            payload = json.load(f)
    except Exception as exc:
        # If reading fails, print an error and exit.
        print(f"[ERROR] Failed to read downloaded JSON: {exc}")
        sys.exit(1)

    # Build the bronze DataFrame from the raw JSON payload.
    df = build_bronze_dataframe(payload, pd)
    # Attempt to write the DataFrame to a parquet file using pyarrow engine.
    try:
        df.to_parquet(OUTPUT_FILE, index=False, engine="pyarrow")
    except Exception as exc:
        # If writing fails, print an error and exit.
        print(f"[ERROR] Failed to write bronze parquet: {exc}")
        sys.exit(1)

    # Return the path to the written bronze parquet file.
    return OUTPUT_FILE


# Allow running the Bronze ingestion directly for local testing.
if __name__ == "__main__":
    run_bronze()