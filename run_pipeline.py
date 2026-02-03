# Indicate that future type annotations are enabled for compatibility.
from __future__ import annotations

# Import subprocess to run pip install commands.
import subprocess
# Import sys to control process exit codes.
import sys
# Import traceback to capture error traces for logging.
import traceback
# Import time to measure execution duration.
import time
# Import re to parse requirements.txt lines.
import re
# Import importlib to attempt dynamic imports.
import importlib
# Import os to access os.devnull and environment.
import os
# Import redirect_stdout and redirect_stderr to suppress console output.
from contextlib import redirect_stdout, redirect_stderr
# Import Path to manipulate filesystem paths safely.
from pathlib import Path
# Import typing helpers for annotations.
from typing import List, Any

# Define the project root folder as the directory containing this file.
PROJECT_ROOT = Path(__file__).parent.resolve()
# Define the path to the requirements.txt file in the project root.
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"
# Define the resources directory where the JSON will be temporarily stored.
RESOURCES_DIR = PROJECT_ROOT / "resources"
# Define the expected input JSON path inside the resources directory.
INPUT_PATH = RESOURCES_DIR / "jira_issues_raw.json"
# Define the logs directory path.
LOG_DIR = PROJECT_ROOT / "logs"
# Define the log file path that will record one line per run.
LOG_FILE = LOG_DIR / "pipeline.log"


# Read requirements.txt and return a list of package tokens.
def parse_requirements(requirements_path: Path) -> List[str]:
    # Initialize an empty list to collect package tokens.
    pkgs: List[str] = []
    # If the requirements file does not exist, return an empty list.
    if not requirements_path.exists():
        return pkgs
    # Open the requirements file for reading using UTF-8 encoding.
    with requirements_path.open("r", encoding="utf-8") as f:
        # Iterate over each raw line in the file.
        for raw in f:
            # Strip leading and trailing whitespace from the line.
            line = raw.strip()
            # Skip empty lines and comment lines.
            if not line or line.startswith("#"):
                continue
            # Remove inline comments and semicolon parts, keep the first token.
            token = re.split(r"[;#]", line, maxsplit=1)[0].strip()
            # Remove version specifiers like ==, >=, <=, ~=, >, <.
            token = re.split(r"==|>=|<=|~=|>|<", token, maxsplit=1)[0].strip()
            # Remove extras like package[extra] and keep the base name.
            token = token.split("[", 1)[0].strip()
            # If a valid token remains, append it to the list.
            if token:
                pkgs.append(token)
    # Return the parsed package tokens.
    return pkgs


# Convert a pip package token to a heuristic import name.
def pkg_to_import_name(pkg_name: str) -> str:
    # Replace hyphens with dots to map packages like azure-identity -> azure.identity.
    return pkg_name.strip().replace("-", ".")


# Determine which modules from requirements cannot be imported.
def modules_missing_from_requirements(requirements_path: Path) -> List[str]:
    # Initialize an empty list for missing import names.
    missing: List[str] = []
    # Parse package tokens from the requirements file.
    pkgs = parse_requirements(requirements_path)
    # If no packages were found, return an empty list.
    if not pkgs:
        return missing
    # For each package token, attempt to import the heuristic module name.
    for pkg in pkgs:
        # Convert the package token to an import name.
        import_name = pkg_to_import_name(pkg)
        try:
            # Try to import the module dynamically.
            importlib.import_module(import_name)
        except Exception:
            # If import fails, record the import name as missing.
            missing.append(import_name)
    # Return the list of missing import names.
    return missing


# Install dependencies using pip and the same Python interpreter.
def install_requirements(requirements_path: Path) -> None:
    # If the requirements file does not exist, raise a FileNotFoundError.
    if not requirements_path.exists():
        raise FileNotFoundError(f"requirements.txt not found at: {requirements_path}")
    # Determine which modules appear missing based on requirements.
    missing = modules_missing_from_requirements(requirements_path)
    # If no modules are missing, do nothing.
    if not missing:
        return
    # Run pip install -r requirements.txt using the current Python interpreter.
    subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", str(requirements_path)])
    # Re-validate that imports now succeed after installation.
    still_missing = modules_missing_from_requirements(requirements_path)
    # If some modules are still missing, raise a RuntimeError.
    if still_missing:
        raise RuntimeError(f"After installation, still missing modules: {still_missing}")


# Ensure the logs directory exists and create a header if the log file is new.
def ensure_log_dir_and_header() -> None:
    # Create the logs directory and any missing parent directories.
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    # If the log file does not exist yet, create it and write a header line.
    if not LOG_FILE.exists():
        header = "timestamp_iso | duration_s | status | description\n"
        with LOG_FILE.open("w", encoding="utf-8") as f:
            f.write(header)


# Convert a multi-line text into a single truncated line for the log.
def single_line_and_truncate(text: str, max_len: int = 300) -> str:
    # Join all lines into a single line separated by spaces.
    single = " ".join(text.splitlines())
    # If the single-line text is within the limit, return it unchanged.
    if len(single) <= max_len:
        return single
    # Otherwise, truncate and append an ellipsis.
    return single[: max_len - 3].rstrip() + "..."


# Append a single log line with timestamp, duration, status, and description.
def append_log_line(timestamp_iso: str, duration_s: float, status: str, description: str) -> None:
    # Ensure the log directory and header exist before appending.
    ensure_log_dir_and_header()
    # Prepare a single-line, truncated description for the log.
    single_line_desc = single_line_and_truncate(description, max_len=300)
    # Format the log line according to the agreed schema.
    line = f"{timestamp_iso} | {duration_s:.3f}s | {status} | {single_line_desc}\n"
    # Open the log file in append mode and write the line.
    with LOG_FILE.open("a", encoding="utf-8") as f:
        f.write(line)


# Create a short summary for path-like or collection objects for the log.
def summarize_path(obj: Any) -> str:
    # If the object is a string, try to treat it as a path and return the filename.
    try:
        if isinstance(obj, str):
            p = Path(obj)
            if p.exists():
                return p.name
            return obj
        # If the object is a Path instance, return its name.
        from pathlib import Path as _P
        if isinstance(obj, _P):
            return obj.name
    except Exception:
        # If any error occurs, fall back to generic handling.
        pass
    # If the object is a dict, return a compact summary with item count.
    if isinstance(obj, dict):
        return f"dict({len(obj)} items)"
    # If the object is a sequence or set, return its type and length.
    if isinstance(obj, (list, tuple, set)):
        return f"{type(obj).__name__}({len(obj)})"
    # Otherwise, convert the object to string.
    return str(obj)


# Main orchestration function that installs dependencies and runs the pipeline.
def main() -> None:
    # Print a minimal start message to the console.
    print("Starting process")
    # Record the start time for duration measurement.
    start_time = time.perf_counter()
    # Create an ISO-like timestamp for the log entry.
    timestamp_iso = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    # Default status is ERROR until the pipeline completes successfully.
    status = "ERROR"
    # Default description if nothing else is set.
    description = "Unknown error"
    # Flag indicating whether the run completed (success or error).
    completed = False

    # Open the system null device and redirect stdout/stderr to suppress details.
    devnull_path = os.devnull
    with open(devnull_path, "w", encoding="utf-8") as devnull, redirect_stdout(devnull), redirect_stderr(devnull):
        # Attempt to install requirements from requirements.txt.
        try:
            install_requirements(REQUIREMENTS_FILE)
        except Exception as exc:
            # Capture the last line of the traceback for the log description.
            tb = traceback.format_exc()
            duration = time.perf_counter() - start_time
            description = (
                f"Failed to install requirements: {exc} | "
                f"{tb.splitlines()[-1] if tb else ''}"
            )
            append_log_line(timestamp_iso, duration, "ERROR", description)
            # Attempt to remove the input JSON if it exists as cleanup.
            try:
                if INPUT_PATH.exists():
                    INPUT_PATH.unlink()
            except Exception:
                pass
            # Mark as completed with error and exit the suppressed context.
            completed = True
            status = "ERROR"
        # If installation succeeded, continue to imports and execution.
        if not completed:
            # Attempt to import pipeline layer functions after installation.
            try:
                from src.bronze.ingest_bronze import run_bronze  # noqa: E402
                from src.silver.transform_silver import run_silver  # noqa: E402
                from src.gold.build_gold import build_gold  # noqa: E402
            except Exception as exc:
                # Log import failure with a short description and cleanup.
                tb = traceback.format_exc()
                duration = time.perf_counter() - start_time
                description = (
                    f"Failed to import pipeline modules: {exc} | "
                    f"{tb.splitlines()[-1] if tb else ''}"
                )
                append_log_line(timestamp_iso, duration, "ERROR", description)
                try:
                    if INPUT_PATH.exists():
                        INPUT_PATH.unlink()
                except Exception:
                    pass
                completed = True
                status = "ERROR"
        # If imports succeeded, run the pipeline layers.
        if not completed:
            try:
                # Run the Bronze ingestion layer.
                bronze_path = run_bronze()
                # Run the Silver transformation layer.
                silver_path = run_silver()
                # Run the Gold reporting layer.
                outputs = build_gold()

                # Summarize outputs for the concise log entry.
                bronze_name = summarize_path(bronze_path)
                silver_name = summarize_path(silver_path)
                gold_summary = summarize_path(outputs)
                # Mark the run as successful and prepare a short description.
                status = "SUCCESS"
                description = (
                    f"Completed: Bronze={bronze_name}; Silver={silver_name}; "
                    f"Gold={gold_summary}"
                )
                completed = True
            except Exception as exc:
                # On any exception, capture a short description including the last traceback line.
                tb = traceback.format_exc()
                description = f"{exc} | {tb.splitlines()[-1] if tb else ''}"
                status = "ERROR"
                completed = True
            finally:
                # Attempt to remove the input JSON file if it exists, ignoring errors.
                try:
                    if INPUT_PATH.exists():
                        INPUT_PATH.unlink()
                except Exception:
                    pass

    # After suppressed execution, compute total duration and append the log line.
    duration = time.perf_counter() - start_time
    append_log_line(timestamp_iso, duration, status, description)

    # Print a minimal end message to the console indicating success or error.
    if status == "SUCCESS":
        print("Finalizing process — completed successfully. For details consult the log.")
        sys.exit(0)
    else:
        print("Finalizing process — completed with errors. For details consult the log.")
        sys.exit(1)


# Execute main when the script is run directly.
if __name__ == "__main__":
    main()