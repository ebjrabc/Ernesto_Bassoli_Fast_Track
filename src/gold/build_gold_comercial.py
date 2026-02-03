"""Gold layer with vectorized SLA calculation."""

import pandas as pd
from pathlib import Path
from src.sla_calculation import (
    calculate_resolution_hours_vectorized,
    get_sla_expected_vectorized,
    check_sla_compliance_vectorized,
)

# Paths
SILVER_FILE = Path("data/silver/silver_issues.parquet")
GOLD_DIR = Path("data/gold")
GOLD_FILE = GOLD_DIR / "gold_sla_issues.parquet"
GOLD_EXCEL = GOLD_DIR / "gold_sla_issues.xlsx"


def build_gold() -> None:
    """Build Gold layer with vectorized SLA calculation."""
    # 1️⃣ Load Silver data
    df = pd.read_parquet(SILVER_FILE)

    # 2️⃣ Clean: keep only resolved issues with resolved_at
    df = df[df["status"].isin(["Done", "Resolved"])].copy()
    df = df[df["resolved_at"].notna()].copy()

    # 3️⃣ Vectorized SLA calculations
    df["resolution_hours"] = calculate_resolution_hours_vectorized(df)
    df["sla_expected_hours"] = get_sla_expected_vectorized(df)
    df["is_sla_met"] = check_sla_compliance_vectorized(df)

    # 4️⃣ Save outputs
    GOLD_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(GOLD_FILE, index=False)

    for col in df.select_dtypes(include="datetime64[ns, UTC]").columns:
        df[col] = df[col].dt.tz_localize(None)
    df.to_excel(GOLD_EXCEL, index=False)

    print(f"Gold file generated at: {GOLD_FILE}")
    print(f"Gold Excel generated at: {GOLD_EXCEL}")


if __name__ == "__main__":
    build_gold()