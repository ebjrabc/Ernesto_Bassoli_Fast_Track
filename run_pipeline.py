"""Pipeline entry point.

Python Data Engineering Challenge - JIRA
This script ensures dependencies, defines global variables, and runs
the Bronze -> Silver -> Gold pipeline.
"""

from __future__ import annotations

# Bring in a utility to check if a Python package can be imported.
import importlib.util
# Bring in a tool to run system commands (we use it to run pip install).
import subprocess
# Bring in access to the current Python executable path.
import sys
# Bring in a modern path helper to build file paths safely.
from pathlib import Path

# List of external packages required by the project.
REQUIRED_PACKAGES = [
    "pandas",
    "numpy",
    "pyarrow",
    "openpyxl",
    "requests",
]

# Define the project root folder (the folder that contains this file).
PROJECT_ROOT = Path(__file__).parent.resolve()
# Define where input resources should be placed (relative to project root).
RESOURCES_DIR = PROJECT_ROOT / "resources"
# Define the expected input JSON file path.
INPUT_PATH = RESOURCES_DIR / "jira_issues_raw.json"

# Define where processed data will be stored (relative to project root).
DATA_DIR = PROJECT_ROOT / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

# Path to the requirements.txt file that we will write/update.
REQUIREMENTS_PATH = PROJECT_ROOT / "requirements.txt"


# Write a requirements.txt file listing the required packages.
def write_requirements_file(packages: list[str], path: Path) -> None:
    path.write_text("\n".join(packages), encoding="utf-8")


# Check if a package is importable; if not, install it using pip.
def ensure_package_installed(package: str) -> None:
    if importlib.util.find_spec(package) is None:
        print(f"Installing missing dependency: {package}")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package])


# Ensure all required packages are present and write requirements.txt.
def ensure_dependencies() -> None:
    write_requirements_file(REQUIRED_PACKAGES, REQUIREMENTS_PATH)
    for pkg in REQUIRED_PACKAGES:
        ensure_package_installed(pkg)


# Run dependency check/install before importing modules that need them.
ensure_dependencies()

# Import pipeline layer functions after dependencies are ensured.
from src.bronze.ingest_bronze import run_bronze  # noqa: E402
from src.silver.transform_silver import run_silver  # noqa: E402
from src.gold.build_gold import build_gold  # noqa: E402


# Main orchestration function that runs the three pipeline layers in order.
def main() -> None:
    # If the input JSON file is missing, stop and show a clear error.
    if not INPUT_PATH.exists():
        raise FileNotFoundError(
            f"Input JSON not found: {INPUT_PATH}\n"
            "Place the file at project-root/resources/jira_issues_raw.json"
        )

    # Run Bronze layer: ingest raw JSON and produce bronze parquet.
    bronze_path = run_bronze()
    # Run Silver layer: clean/normalize bronze and produce silver parquet.
    silver_path = run_silver()
    # Run Gold layer: compute SLA, produce gold outputs and reports.
    outputs = build_gold()

    # Print a short summary of generated files for the user.
    print("Pipeline finished successfully ✅")
    print("Generated outputs:")
    print(f"- Bronze: {bronze_path}")
    print(f"- Silver: {silver_path}")
    for name, path in outputs.items():
        print(f"- {name}: {path}")


# Only run main when this file is executed directly (not when imported).
if __name__ == "__main__":
    main()