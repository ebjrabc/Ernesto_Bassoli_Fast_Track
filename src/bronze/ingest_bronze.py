"""Bronze layer.

Reads raw Jira JSON from Azure Blob Storage (Service Principal) or local fallback,
then writes a parquet file with selected fields. Includes robust logging,
retries with exponential backoff, configurable timeouts, and a smoke test.
"""

from __future__ import annotations

# Bring in JSON reader to parse the input file.
import json
# Bring in OS to read environment variables for credentials and flags.
import os
# Bring in time for backoff sleep.
import time
# Bring in logging to record events to console and file.
import logging
# Bring in sys for console handler stream selection.
import sys
# Bring in Path to build file paths safely.
from pathlib import Path
# Bring in typing helpers for clarity.
from typing import Any, Dict, List, Optional

# Bring in pandas to build and save tables.
import pandas as pd

# Try to import Azure libraries; if missing, we will still allow local-only execution.
try:
    # Credential class for Service Principal authentication.
    from azure.identity import ClientSecretCredential
    # BlobClient to download a blob from Azure Storage.
    from azure.storage.blob import BlobClient
except Exception:
    ClientSecretCredential = None  # type: ignore
    BlobClient = None  # type: ignore

# Try to load .env for local development, but do not fail if python-dotenv is not installed.
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore

    dotenv_path = find_dotenv()
    if dotenv_path:
        load_dotenv(dotenv_path)
except Exception:
    # No-op: dotenv not installed or .env not found. Environment variables must be provided externally.
    pass

# -------------------------
# Configuration and paths
# -------------------------

# Project root (three levels up from this file).
PROJECT_ROOT = Path(__file__).parent.parent.parent.resolve()
# Directory where input resources (including downloaded blob) will be stored.
RESOURCES_DIR = PROJECT_ROOT / "resources"
# Ensure resources directory exists.
RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
# Local path where the JSON will be saved or read from.
INPUT_PATH = RESOURCES_DIR / "jira_issues_raw.json"
# Bronze output folder and file path.
BRONZE_DIR = PROJECT_ROOT / "data" / "bronze"
OUTPUT_FILE = BRONZE_DIR / "bronze_issues.parquet"
OUTPUT_XLSX = BRONZE_DIR / "bronze_issues.xlsx"
# Logs directory and file path.
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)
LOG_FILE = LOGS_DIR / "ingest.log"

# Read verbosity flag from environment (VERBOSE=1 for more logs).
VERBOSE = os.getenv("VERBOSE", "0") in ("1", "true", "True")

# -------------------------
# Logging setup
# -------------------------

# Create a logger for this module.
logger = logging.getLogger("bronze_ingest")
# Set level to DEBUG if verbose, else INFO.
logger.setLevel(logging.DEBUG if VERBOSE else logging.INFO)

# Create console handler to print to stdout.
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setLevel(logging.DEBUG if VERBOSE else logging.INFO)
# Create file handler to write logs to a file with timestamps.
file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
file_handler.setLevel(logging.DEBUG)

# Simple log format with timestamp, level and message.
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

# Avoid adding multiple handlers if this module is reloaded.
if not logger.handlers:
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

# -------------------------
# Azure environment reader
# -------------------------

# Read Azure-related environment variables.
def _read_azure_env() -> Dict[str, Optional[str]]:
    # Storage account URL (e.g., https://<account>.blob.core.windows.net).
    account_url = os.getenv("ACCOUNT_URL")
    # Container name where the blob is stored.
    container_name = os.getenv("CONTAINER_NAME")
    # Blob name (file name) inside the container.
    blob_name = os.getenv("BLOB_NAME")
    # Tenant id for the Service Principal.
    tenant_id = os.getenv("AZURE_TENANT_ID")
    # Client id for the Service Principal.
    client_id = os.getenv("AZURE_CLIENT_ID")
    # Client secret for the Service Principal.
    client_secret = os.getenv("AZURE_CLIENT_SECRET")
    return {
        "account_url": account_url,
        "container_name": container_name,
        "blob_name": blob_name,
        "tenant_id": tenant_id,
        "client_id": client_id,
        "client_secret": client_secret,
    }

# -------------------------
# Azure download with retries
# -------------------------

# Download the JSON blob from Azure Blob Storage using Service Principal credentials.
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
    # If azure packages are not available, raise an informative error.
    if ClientSecretCredential is None or BlobClient is None:
        logger.error("Azure SDK packages not installed. Install 'azure-identity' and 'azure-storage-blob'.")
        raise ImportError("Missing Azure SDK packages: azure-identity, azure-storage-blob")

    # Create credential object using Service Principal.
    credential = ClientSecretCredential(tenant_id=tenant_id, client_id=client_id, client_secret=client_secret)

    # Create a BlobClient for the target blob.
    blob_client = BlobClient(account_url=account_url, container_name=container_name, blob_name=blob_name, credential=credential)

    attempt = 0
    while attempt < max_retries:
        attempt += 1
        try:
            logger.info("Attempt %d/%d: downloading blob '%s' from container '%s'.", attempt, max_retries, blob_name, container_name)
            # CORREÇÃO: download_blob() retorna um downloader que não implementa context manager.
            stream = blob_client.download_blob(timeout=timeout)
            data = stream.readall()
            dest_path.write_bytes(data)
            logger.info("Blob successfully downloaded to %s", dest_path)
            return
        except Exception as exc:
            # Log the exception with details and timestamp.
            logger.warning("Download attempt %d failed (%s: %s)", attempt, type(exc).__name__, exc)
            if attempt < max_retries:
                wait = backoff_factor ** attempt
                logger.info("Waiting %.1f seconds before next attempt...", wait)
                time.sleep(wait)
            else:
                logger.error("All %d download attempts failed.", max_retries)
                # Raise a ConnectionError to be handled by caller.
                raise ConnectionError(f"Failed to download blob '{blob_name}' after {max_retries} attempts.") from exc

# -------------------------
# Read JSON (Azure or local fallback)
# -------------------------

# Read the JSON file and return a Python dictionary.
def read_json_file(path: Path = INPUT_PATH, timeout: int = 30, max_retries: int = 3) -> Dict[str, Any]:
    # Read Azure env vars.
    env = _read_azure_env()
    # Check if all Azure variables are present.
    azure_ready = all(
        [
            env.get("account_url"),
            env.get("container_name"),
            env.get("blob_name"),
            env.get("tenant_id"),
            env.get("client_id"),
            env.get("client_secret"),
        ]
    )

    # If Azure env vars are present, attempt to download with retries.
    if azure_ready:
        logger.info("Azure credentials detected in environment. Attempting to download from Azure Blob Storage.")
        try:
            fetch_blob_to_local_with_retries(
                account_url=env["account_url"],  # type: ignore[arg-type]
                container_name=env["container_name"],  # type: ignore[arg-type]
                blob_name=env["blob_name"],  # type: ignore[arg-type]
                tenant_id=env["tenant_id"],  # type: ignore[arg-type]
                client_id=env["client_id"],  # type: ignore[arg-type]
                client_secret=env["client_secret"],  # type: ignore[arg-type]
                dest_path=path,
                timeout=timeout,
                max_retries=max_retries,
                backoff_factor=2.0,
            )
            logger.info("Using JSON downloaded from Azure Blob Storage.")
        except Exception as exc:
            # If download fails, log and try fallback to local file.
            logger.warning("Azure download failed: %s", exc)
            if path.exists():
                logger.info("Local file found at %s. Using local file as fallback.", path)
            else:
                # No fallback available: log error and raise FileNotFoundError.
                logger.error("Azure download failed and local file not found at %s.", path)
                raise FileNotFoundError(f"Could not obtain input JSON from Azure and local file not found at {path}") from exc
    else:
        # Azure not configured: use local file if present.
        missing_keys = [
            k
            for k, v in {
                "ACCOUNT_URL": env.get("account_url"),
                "CONTAINER_NAME": env.get("container_name"),
                "BLOB_NAME": env.get("blob_name"),
                "AZURE_TENANT_ID": env.get("tenant_id"),
                "AZURE_CLIENT_ID": env.get("client_id"),
                "AZURE_CLIENT_SECRET": env.get("client_secret"),
            }.items()
            if not v
        ]
        if missing_keys:
            logger.warning(
                "Azure credentials missing: %s. Set environment variables or provide them via .env for development.",
                missing_keys,
            )
        logger.info("Azure credentials not provided or incomplete. Looking for local file at %s", path)
        if not path.exists():
            logger.error("Local input file not found at %s and Azure credentials not set.", path)
            raise FileNotFoundError(f"Input JSON not found: {path}. Set Azure env vars or place the file at resources/")

    # At this point the file exists locally (downloaded or pre-existing).
    logger.info("Reading JSON file from %s", path)
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)

# -------------------------
# Build Bronze DataFrame
# -------------------------

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

# -------------------------
# Smoke test / sanity check
# -------------------------

# Simple smoke test to validate expected columns exist and have reasonable types.
def smoke_test(df: pd.DataFrame) -> bool:
    # Required columns for Bronze output.
    required = {"issue_id", "issue_type", "assignee_name", "priority", "created_at", "resolved_at"}
    missing = required - set(df.columns)
    if missing:
        logger.error("Smoke test failed: missing required columns: %s", ", ".join(sorted(missing)))
        return False

    # Check that created_at is datetime-like.
    if not pd.api.types.is_datetime64_any_dtype(df["created_at"]):
        logger.warning("Smoke test: 'created_at' is not datetime dtype.")
    # Check that issue_id has values.
    if df["issue_id"].isna().all():
        logger.warning("Smoke test: all issue_id values are null.")
    logger.info("Smoke test passed (basic checks).")
    return True

# -------------------------
# Run Bronze pipeline
# -------------------------

# Run the Bronze layer: read JSON, build DataFrame, save Parquet, return path.
def run_bronze(timeout: int = 30, max_retries: int = 3) -> Path:
    logger.info("Starting Bronze ingestion. timeout=%s max_retries=%s", timeout, max_retries)

    try:
        payload = read_json_file(path=INPUT_PATH, timeout=timeout, max_retries=max_retries)
    except FileNotFoundError as exc:
        logger.error("Fatal error: %s", exc)
        raise RuntimeError(f"Input JSON not available: {exc}") from exc

    df = build_bronze_dataframe(payload)

    ok = smoke_test(df)
    if not ok:
        logger.warning("Smoke test reported issues. Check logs and input data for invalid records.")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Salva em Parquet
    df.to_parquet(OUTPUT_FILE, index=False)

# Run the Bronze layer: read JSON, build DataFrame, save Parquet, return path.
def run_bronze(timeout: int = 30, max_retries: int = 3) -> Path:
    logger.info("Starting Bronze ingestion. timeout=%s max_retries=%s", timeout, max_retries)

    try:
        payload = read_json_file(path=INPUT_PATH, timeout=timeout, max_retries=max_retries)
    except FileNotFoundError as exc:
        logger.error("Fatal error: %s", exc)
        raise RuntimeError(f"Input JSON not available: {exc}") from exc

    df = build_bronze_dataframe(payload)

    ok = smoke_test(df)
    if not ok:
        logger.warning("Smoke test reported issues. Check logs and input data for invalid records.")

    BRONZE_DIR.mkdir(parents=True, exist_ok=True)

    # Salva em Parquet (mantém timezone-aware, que é suportado)
    df.to_parquet(OUTPUT_FILE, index=False)

    # Converter colunas datetime para timezone-unaware antes de salvar em Excel
    for col in ["extracted_at", "created_at", "resolved_at"]:
        if col in df.columns and pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)   # REMOVE timezone

    # Salva também em Excel
    df.to_excel(OUTPUT_XLSX, index=False, engine="openpyxl")

    logger.info("Bronze files generated at: %s and %s", OUTPUT_FILE, OUTPUT_XLSX)

    env = _read_azure_env()
    azure_used = all(
        [env.get("account_url"), env.get("container_name"), env.get("blob_name"),
         env.get("tenant_id"), env.get("client_id"), env.get("client_secret")]
    ) and INPUT_PATH.exists()
    if azure_used:
        logger.info("Ingestion completed using Azure Blob Storage as source.")
    else:
        logger.info("Ingestion completed using local file as source.")

    # Só retorna no final
    return OUTPUT_FILE

    # Logs de confirmação
    logger.info("Bronze files generated at: %s and %s", OUTPUT_FILE, excel_file)

    env = _read_azure_env()
    azure_used = all(
        [env.get("account_url"), env.get("container_name"), env.get("blob_name"),
         env.get("tenant_id"), env.get("client_id"), env.get("client_secret")]
    ) and INPUT_PATH.exists()
    if azure_used:
        logger.info("Ingestion completed using Azure Blob Storage as source.")
    else:
        logger.info("Ingestion completed using local file as source.")

    # Só retorna no final
    return OUTPUT_FILE


# Allow running this module directly for testing.
if __name__ == "__main__":
    # Optional: read timeout and retries from environment for quick testing.
    try:
        t = int(os.getenv("INGEST_TIMEOUT", "30"))
    except ValueError:
        t = 30
    try:
        r = int(os.getenv("INGEST_RETRIES", "3"))
    except ValueError:
        r = 3
    # When run directly, let exceptions propagate so the developer sees full trace.
    run_bronze(timeout=t, max_retries=r)