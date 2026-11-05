"""
BrightCart Daily Sales Pipeline: pipeline tasks.

The same logic as notebooks/02_pipeline_build.ipynb, organised into four
functions. Each function is one meaningful checkpoint of the pipeline and
will become one Airflow task:

    check_input_files  >>  clean_orders  >>  build_city_summary  >>  run_quality_check

Rules for every function:
    * It receives the day to process as run_date ("YYYYMMDD").
    * It reads its inputs from files and writes its outputs to files.
      Functions never pass DataFrames to each other, because Airflow runs
      each task separately.
    * Outputs are replaced on every run, never appended, so running the same
      day twice gives the same result (idempotent).
    * Nothing is ever written to data/raw.

Run it manually, without Airflow, from the project folder:

    python scripts/pipeline_tasks.py              # every day found in data/raw
    python scripts/pipeline_tasks.py 20261105     # one day

If this works and every quality check passes, the pipeline is ready to be
orchestrated. The Airflow DAG imports the four task functions from this file.
"""

from datetime import datetime, timezone
from pathlib import Path
import json
import sys

import numpy as np
import pandas as pd


# ===========================================================================
# Configuration
# ===========================================================================

# This file lives in /scripts, so the project root is one level up.
# Building paths from the file location makes the project portable:
# it works on any computer and from any folder you run it from.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = PROJECT_ROOT / "data"

VALID_STATUSES = ["COMPLETED", "CANCELLED", "PENDING", "REFUNDED"]
REVENUE_STATUS = "COMPLETED"
INPUT_FILES = ["customers.csv", "orders.csv"]

# Freshness: the curated outputs must have been written by THIS run.
# If a file is older than this, the report on disk is a leftover from an
# earlier run and must not be published as today's result.
FRESHNESS_MAX_AGE_MINUTES = 60


def get_paths(run_date):
    """Return every folder and file used for one day of data."""
    return {
        "raw": DATA_DIR / "raw" / run_date,
        "cleaned": DATA_DIR / "cleaned" / run_date,
        "curated": DATA_DIR / "curated" / run_date,
        "audit": DATA_DIR / "audit" / f"{run_date}.json",
    }


# ===========================================================================
# Task 1: check_input_files
# ===========================================================================

def check_input_files(run_date):
    """Confirm that both source files for the day exist and are not empty.

    Fails early with a clear message, so a missing file is not confused
    with a bug in the cleaning code.
    """
    paths = get_paths(run_date)
    missing = [name for name in INPUT_FILES if not (paths["raw"] / name).exists()]
    if missing:
        raise FileNotFoundError(f"[{run_date}] Missing input file(s) in {paths['raw']}: {missing}")

    row_counts = {}
    for name in INPUT_FILES:
        rows = len(pd.read_csv(paths["raw"] / name, dtype=str))
        if rows == 0:
            raise ValueError(f"[{run_date}] Input file is empty: {name}")
        row_counts[name] = rows

    print(f"[{run_date}] Input files found: {row_counts}")
    return row_counts


# ===========================================================================
# Task 2: clean_orders
# ===========================================================================

def validate_customers(raw_customers):
    """Fix safe values and split customers into valid and rejected (lab Q1, Q6, Q18, Q19)."""
    customers = raw_customers.copy()

    customers["customer_name"] = customers["customer_name"].str.strip().replace("", np.nan)
    customers["city"] = customers["city"].str.strip().str.title()
    customers["customer_id"] = pd.to_numeric(customers["customer_id"], errors="coerce").astype("Int64")

    conditions = [
        customers["customer_id"].isna(),
        customers["customer_name"].isna(),
        customers["customer_id"].duplicated(keep="first"),
    ]
    reasons = ["missing_customer_id", "missing_customer_name", "duplicate_customer_id"]
    customers["rejection_reason"] = np.select(conditions, reasons, default="valid")

    valid = customers[customers["rejection_reason"] == "valid"].drop(columns="rejection_reason")
    rejected = customers[customers["rejection_reason"] != "valid"]
    return valid, rejected


def standardise_orders(raw_orders):
    """Apply the safe fixes to orders and convert types on purpose (lab Q6)."""
    orders = raw_orders.copy()

    orders["status"] = orders["status"].str.strip().str.upper()
    orders["product_name"] = orders["product_name"].str.strip()
    orders["unit_price_gbp"] = orders["unit_price_gbp"].str.replace("£", "", regex=False)

    orders["quantity"] = pd.to_numeric(orders["quantity"], errors="coerce")
    orders["unit_price_gbp"] = pd.to_numeric(orders["unit_price_gbp"], errors="coerce")
    orders["customer_id"] = pd.to_numeric(orders["customer_id"], errors="coerce").astype("Int64")

    iso_dates = pd.to_datetime(orders["order_date"], format="%Y-%m-%d", errors="coerce")
    uk_dates = pd.to_datetime(orders["order_date"], format="%d/%m/%Y", errors="coerce")
    orders["order_date"] = iso_dates.fillna(uk_dates)
    return orders


def validate_orders(orders, known_customer_ids):
    """Split orders into valid and rejected, each rejected row with one reason (lab Q2, Q14, Q19).

    When a row has several problems, the first reason in the list wins.
    """
    conditions = [
        orders["order_id"].duplicated(keep="first"),
        orders["customer_id"].isna(),
        ~orders["customer_id"].isin(known_customer_ids),
        orders["quantity"].isna() | (orders["quantity"] <= 0),
        orders["unit_price_gbp"].isna(),
        orders["unit_price_gbp"] <= 0,
        ~orders["status"].isin(VALID_STATUSES),
        orders["order_date"].isna(),
    ]
    reasons = [
        "duplicate_order_id",
        "missing_customer_id",
        "unknown_customer",
        "invalid_quantity",
        "missing_price",
        "invalid_price",
        "invalid_status",
        "invalid_date",
    ]
    orders = orders.copy()
    orders["rejection_reason"] = np.select(conditions, reasons, default="valid")

    valid = orders[orders["rejection_reason"] == "valid"].drop(columns="rejection_reason").copy()
    rejected = orders[orders["rejection_reason"] != "valid"].copy()
    return valid, rejected


def enrich_orders(valid_orders, valid_customers):
    """Join customers to orders safely and add the revenue column (lab Q7, Q9, Q18)."""
    enriched = valid_orders.merge(
        valid_customers[["customer_id", "customer_name", "city"]],
        on="customer_id",
        how="left",
        validate="many_to_one",
    )
    if len(enriched) != len(valid_orders):
        raise ValueError("The join changed the number of orders")
    if enriched["city"].isna().any():
        raise ValueError("Some valid orders have no city after the join")

    enriched["line_revenue_gbp"] = (enriched["quantity"] * enriched["unit_price_gbp"]).round(2)
    return enriched


def clean_orders(run_date):
    """Read the raw files, validate customers and orders, and write the cleaned zone.

    Writes to data/cleaned/<run_date>/:
        customers_valid.csv, customers_rejected.csv,
        orders_valid.csv, orders_rejected.csv
    """
    paths = get_paths(run_date)

    # Read everything as text: CSV files do not guarantee types
    raw_customers = pd.read_csv(paths["raw"] / "customers.csv", dtype=str)
    raw_orders = pd.read_csv(paths["raw"] / "orders.csv", dtype=str)

    valid_customers, rejected_customers = validate_customers(raw_customers)
    orders = standardise_orders(raw_orders)
    valid_orders, rejected_orders = validate_orders(orders, set(valid_customers["customer_id"]))
    enriched = enrich_orders(valid_orders, valid_customers)

    # Replace the outputs (idempotent). Rejected rows are saved, never dropped.
    paths["cleaned"].mkdir(parents=True, exist_ok=True)
    valid_customers.to_csv(paths["cleaned"] / "customers_valid.csv", index=False)
    rejected_customers.to_csv(paths["cleaned"] / "customers_rejected.csv", index=False)
    enriched.to_csv(paths["cleaned"] / "orders_valid.csv", index=False)
    rejected_orders.to_csv(paths["cleaned"] / "orders_rejected.csv", index=False)

    counts = {
        "customers_source": len(raw_customers),
        "customers_valid": len(valid_customers),
        "customers_rejected": len(rejected_customers),
        "orders_source": len(raw_orders),
        "orders_valid": len(enriched),
        "orders_rejected": len(rejected_orders),
    }
    print(f"[{run_date}] Cleaned: {counts}")
    return counts


# ===========================================================================
# Task 3: build_city_summary
# ===========================================================================

def build_city_summary(run_date):
    """Build the curated report and the audit record from the cleaned files.

    Writes:
        data/curated/<run_date>/city_sales.csv
        data/curated/<run_date>/daily_summary.csv
        data/audit/<run_date>.json
    """
    paths = get_paths(run_date)

    raw_customers = pd.read_csv(paths["raw"] / "customers.csv", dtype=str)
    raw_orders = pd.read_csv(paths["raw"] / "orders.csv", dtype=str)
    valid_customers = pd.read_csv(paths["cleaned"] / "customers_valid.csv")
    rejected_customers = pd.read_csv(paths["cleaned"] / "customers_rejected.csv")
    orders = pd.read_csv(paths["cleaned"] / "orders_valid.csv")
    rejected_orders = pd.read_csv(paths["cleaned"] / "orders_rejected.csv")

    # Business rule: only completed orders count as revenue (lab Q10)
    completed = orders[orders["status"] == REVENUE_STATUS]

    city_sales = (
        completed
        .groupby("city", as_index=False)
        .agg(
            completed_orders=("order_id", "count"),
            completed_revenue_gbp=("line_revenue_gbp", "sum"),
        )
        .sort_values("completed_revenue_gbp", ascending=False)
    )
    city_sales["completed_revenue_gbp"] = city_sales["completed_revenue_gbp"].round(2)
    city_sales.insert(0, "run_date", run_date)

    top_order = completed.loc[completed["line_revenue_gbp"].idxmax()]
    completed_revenue = round(float(completed["line_revenue_gbp"].sum()), 2)

    daily_summary = pd.DataFrame([{
        "run_date": run_date,
        "completed_orders": len(completed),
        "completed_revenue_gbp": completed_revenue,
        "top_order_id": int(top_order["order_id"]),
        "top_order_revenue_gbp": float(top_order["line_revenue_gbp"]),
        "rejected_orders": len(rejected_orders),
    }])

    paths["curated"].mkdir(parents=True, exist_ok=True)
    city_sales.to_csv(paths["curated"] / "city_sales.csv", index=False)
    daily_summary.to_csv(paths["curated"] / "daily_summary.csv", index=False)

    # Audit record (lab Q13, Q15, Q20)
    audit = {
        "run_date": run_date,
        "run_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "customers": {
            "source": len(raw_customers),
            "valid": len(valid_customers),
            "rejected": len(rejected_customers),
            "rejected_by_reason": rejected_customers["rejection_reason"].value_counts().to_dict(),
        },
        "orders": {
            "source": len(raw_orders),
            "valid": len(orders),
            "rejected": len(rejected_orders),
            "rejected_by_reason": rejected_orders["rejection_reason"].value_counts().to_dict(),
        },
        "output": {
            "completed_orders": int(len(completed)),
            "completed_revenue_gbp": completed_revenue,
            "cities_in_report": int(len(city_sales)),
        },
        "status": "pending_quality_check",
    }
    for entity in ["customers", "orders"]:
        block = audit[entity]
        block["reconciles"] = block["source"] == block["valid"] + block["rejected"]

    paths["audit"].parent.mkdir(parents=True, exist_ok=True)
    paths["audit"].write_text(json.dumps(audit, indent=2))

    print(f"[{run_date}] Completed revenue: £{completed_revenue} from {len(completed)} orders")
    return audit["output"]


# ===========================================================================
# Task 4: run_quality_check
# ===========================================================================

def run_quality_check(run_date):
    """Publishing gate: verify the outputs agree before declaring success.

    Reads the files back from disk and checks them. If any check fails, the
    audit is marked as failed and an error is raised, so Airflow marks the
    task as failed and the completion message never runs.
    """
    paths = get_paths(run_date)

    required_files = [
        paths["cleaned"] / "orders_valid.csv",
        paths["cleaned"] / "orders_rejected.csv",
        paths["curated"] / "city_sales.csv",
        paths["curated"] / "daily_summary.csv",
        paths["audit"],
    ]
    missing = [str(p.relative_to(PROJECT_ROOT)) for p in required_files if not p.exists()]
    if missing:
        raise FileNotFoundError(f"[{run_date}] Missing output file(s): {missing}")

    orders = pd.read_csv(paths["cleaned"] / "orders_valid.csv")
    city_sales = pd.read_csv(paths["curated"] / "city_sales.csv")
    audit = json.loads(paths["audit"].read_text())

    cleaned_revenue = round(float(orders.loc[orders["status"] == REVENUE_STATUS, "line_revenue_gbp"].sum()), 2)
    report_revenue = round(float(city_sales["completed_revenue_gbp"].sum()), 2)
    audit_revenue = round(float(audit["output"]["completed_revenue_gbp"]), 2)

    # Freshness (Airflow lab Q14, Q15): how old is each curated output?
    now = datetime.now()
    output_age_minutes = {}
    for path in [paths["curated"] / "city_sales.csv", paths["curated"] / "daily_summary.csv"]:
        modified = datetime.fromtimestamp(path.stat().st_mtime)
        output_age_minutes[path.name] = round((now - modified).total_seconds() / 60, 1)
    outputs_are_fresh = all(age <= FRESHNESS_MAX_AGE_MINUTES for age in output_age_minutes.values())
    print(f"[{run_date}] Output age in minutes: {output_age_minutes} (limit {FRESHNESS_MAX_AGE_MINUTES})")

    checks = {
        "order counts reconcile": audit["orders"]["reconciles"],
        "customer counts reconcile": audit["customers"]["reconciles"],
        "no duplicate order_id in valid output": bool(orders["order_id"].is_unique),
        "no missing quantity or price in valid output": bool(orders[["quantity", "unit_price_gbp"]].notna().all().all()),
        "revenue: cleaned = city report": cleaned_revenue == report_revenue,
        "revenue: city report = audit": report_revenue == audit_revenue,
        f"outputs are fresh (written in the last {FRESHNESS_MAX_AGE_MINUTES} min)": outputs_are_fresh,
    }

    for name, passed in checks.items():
        print(f"[{run_date}] {'PASS' if passed else 'FAIL'}  {name}")

    audit["output_age_minutes"] = output_age_minutes
    audit["quality_checks"] = {name: bool(passed) for name, passed in checks.items()}
    audit["status"] = "success" if all(checks.values()) else "failed"
    paths["audit"].write_text(json.dumps(audit, indent=2))

    if audit["status"] != "success":
        failed = [name for name, passed in checks.items() if not passed]
        raise ValueError(f"[{run_date}] Quality checks failed: {failed}")

    print(f"[{run_date}] All quality checks passed")
    return audit["quality_checks"]


# ===========================================================================
# Manual run (without Airflow)
# ===========================================================================

TASKS = [check_input_files, clean_orders, build_city_summary, run_quality_check]


def run_day(run_date):
    """Run the four tasks in the same order the Airflow DAG will use."""
    print(f"\n========== BrightCart pipeline: {run_date} ==========")
    for task in TASKS:
        print(f"\n> {task.__name__}")
        task(run_date)
    print(f"\nBrightCart pipeline completed successfully for {run_date}")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_dates = sys.argv[1:]
    else:
        run_dates = sorted(folder.name for folder in (DATA_DIR / "raw").iterdir() if folder.is_dir())

    for run_date in run_dates:
        run_day(run_date)
