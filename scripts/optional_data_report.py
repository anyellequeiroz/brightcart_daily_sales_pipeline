"""
OPTIONAL: automatic data quality report (not part of the Airflow DAG).

Generates an interactive HTML report that compares the RAW orders with the
CLEANED orders for one day, side by side, using Sweetviz. One line of code
builds the whole report: statistics, charts and missing values per column.

Install once:

    pip install sweetviz

Run from the project folder, after the pipeline has processed the day:

    python scripts/optional_data_report.py 20261105

Then open data/reports/20261105/orders_raw_vs_cleaned.html in the browser.
"""

import sys
from pathlib import Path

import pandas as pd
import sweetviz as sv

PROJECT_ROOT = Path(__file__).resolve().parents[1]

run_date = sys.argv[1] if len(sys.argv) > 1 else "20261105"

raw_file = PROJECT_ROOT / "data" / "raw" / run_date / "orders.csv"
clean_file = PROJECT_ROOT / "data" / "cleaned" / run_date / "orders_valid.csv"
report_dir = PROJECT_ROOT / "data" / "reports" / run_date

if not clean_file.exists():
    sys.exit(f"Run the pipeline for {run_date} first: python scripts/pipeline_tasks.py {run_date}")

# Raw is read with default types on purpose: the report shows how messy it is
raw_orders = pd.read_csv(raw_file)
clean_orders = pd.read_csv(clean_file)

# The automatic part: one call compares both DataFrames column by column
report = sv.compare([raw_orders, "Raw orders"], [clean_orders, "Cleaned orders"])

report_dir.mkdir(parents=True, exist_ok=True)
report_file = report_dir / "orders_raw_vs_cleaned.html"
report.show_html(str(report_file), open_browser=False, layout="vertical")

print(f"Report saved to {report_file.relative_to(PROJECT_ROOT)}")
