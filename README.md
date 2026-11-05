# BrightCart Daily Sales Pipeline

A small batch pipeline that turns BrightCart's daily customer and order files into a **trusted daily sales report**, orchestrated with Apache Airflow.

BrightCart is a small UK online retailer. Every day it receives two files: a full snapshot of its customers and the day's orders. The pipeline validates every row, keeps rejected rows with a reason, builds the report, and records audit evidence that the numbers can be trusted.

## Business rule

> An order counts as **completed revenue** only when its status is `COMPLETED`, its quantity and price are valid, and duplicate order IDs have been removed.

The report answers: completed revenue, revenue by city, completed order count, highest value completed order, and records rejected.

## Project structure

```
Week 07/
    data/
        raw/<run_date>/         customers.csv, orders.csv (as received, never modified)
        cleaned/<run_date>/     valid and rejected customers and orders
        curated/<run_date>/     city_sales.csv, daily_summary.csv
        audit/<run_date>.json   counts, rejection reasons, revenue, quality checks
    dags/
        brightcart_daily_sales_pipeline.py    Airflow DAG (orchestration only)
    scripts/
        pipeline_tasks.py                     all pipeline logic, also runnable manually
    notebooks/
        01_lab_build.ipynb                    Build lab, solved and commented
        02_pipeline_build.ipynb               the pipeline step by step on the real files
    README.md
    RECOVERY.md                               what to do when a run fails
```

`<run_date>` is the day in `YYYYMMDD` format, for example `20261105`. It is the same format as Airflow's `ds_nodash`.

## Setup

Requirements: **Python 3.12**, **pandas** and **Apache Airflow 3.3.2**.

1. Create and activate a virtual environment for the project.
2. Install Airflow 3.3.2 following the course setup guide (the install uses the official constraints file for Python 3.12).
3. Install pandas:

```bash
pip install pandas
```

4. Check:

```bash
airflow version
```

## How to run

### Option 1: manually, without Airflow

From the project folder:

```bash
python scripts/pipeline_tasks.py 20261105
```

Without a date, it processes every day found in `data/raw`:

```bash
python scripts/pipeline_tasks.py
```

### Option 2: with Airflow

From the project folder, tell Airflow to read the DAGs of this project, then start it:

```bash
export AIRFLOW__CORE__DAGS_FOLDER="$PWD/dags"
airflow standalone
```

1. Open `http://localhost:8080`. The user is `admin`; the password is in `$AIRFLOW_HOME/simple_auth_manager_passwords.json.generated`.
2. Open **brightcart_daily_sales_pipeline** and click **Trigger**.
3. In the form, set **run_date** to the day to process (for example `20261105`). Leave it empty to use the run's logical date.

### Run order

Both options run the same four functions, in this order:

| Task | What it does | Reads | Writes |
|---|---|---|---|
| `check_input_files` | Confirms both source files exist and are not empty | `raw/<run_date>/` | nothing |
| `clean_orders` | Validates customers and orders, joins them, calculates revenue | `raw/<run_date>/` | `cleaned/<run_date>/` |
| `build_city_summary` | Builds the report and the audit record | `cleaned/<run_date>/` | `curated/<run_date>/`, `audit/<run_date>.json` |
| `run_quality_check` | Reads every output back and checks it. Fails the run if anything disagrees | all outputs | updates the audit status |
| `publish_completion_message` | Airflow only: prints the success message | | |

DAG settings: daily schedule (`@daily`), `catchup=False`, 1 retry after 1 minute. The quality check has no retry: bad data gives the same result every time.

## Validation rules

### Customers

| Rule | Action | Reason saved |
|---|---|---|
| City with extra spaces or wrong capitals | **Fix** (trim, title case) | |
| Blank email | **Keep** (email is optional) | |
| Missing `customer_id` | Reject | `missing_customer_id` |
| Missing `customer_name` | Reject | `missing_customer_name` |
| Repeated `customer_id` | Keep the first, reject the others | `duplicate_customer_id` |

### Orders

Safe fixes applied first: status trimmed and upper cased, `£` removed from prices, product names trimmed, dates accepted in ISO format (year, month, day) or UK format (day/month/year).

Then each row is checked. When a row has several problems, the first reason in this list is saved:

| Order | Rule | Reason saved |
|---|---|---|
| 1 | Second or later occurrence of an `order_id` | `duplicate_order_id` |
| 2 | `customer_id` is empty | `missing_customer_id` |
| 3 | Customer not in the same day's valid customers | `unknown_customer` |
| 4 | Quantity is zero, negative or not a number | `invalid_quantity` |
| 5 | Price is empty | `missing_price` |
| 6 | Price is zero or negative | `invalid_price` |
| 7 | Status is not COMPLETED, CANCELLED, PENDING or REFUNDED | `invalid_status` |
| 8 | Date cannot be read | `invalid_date` |

Rejected rows are **never dropped silently**: they are saved in `orders_rejected.csv` and `customers_rejected.csv` with their reason. Cancelled, pending and refunded orders are valid, but they are not counted as revenue.

## Outputs

| File | Content |
|---|---|
| `cleaned/<run_date>/customers_valid.csv` | Valid customers after the fixes |
| `cleaned/<run_date>/customers_rejected.csv` | Rejected customers with `rejection_reason` |
| `cleaned/<run_date>/orders_valid.csv` | Valid orders with customer name, city and `line_revenue_gbp` |
| `cleaned/<run_date>/orders_rejected.csv` | Rejected orders with `rejection_reason` |
| `curated/<run_date>/city_sales.csv` | Completed orders and revenue per city |
| `curated/<run_date>/daily_summary.csv` | One row: completed orders, revenue, top order, rejected orders |
| `audit/<run_date>.json` | Evidence for the run (see below) |

## How to verify a run

Open `data/audit/<run_date>.json`. A trusted run has:

* `"status": "success"`
* `"reconciles": true` for both customers and orders, meaning `source = valid + rejected`
* every entry in `"quality_checks"` set to `true`

The quality checks are:

* order counts reconcile;
* customer counts reconcile;
* no duplicate `order_id` in the valid output;
* no missing quantity or price in the valid output;
* revenue in the cleaned data equals revenue in the city report;
* revenue in the city report equals revenue in the audit;
* the curated outputs are **fresh**: written in the last 60 minutes, so the report is from this run and not a leftover.

Reruns are safe: every output is replaced, never appended. Running the same day twice gives the same files and totals.

### Results for the sample data

| Day | Orders received | Valid | Rejected | Completed orders | Completed revenue |
|---|---|---|---|---|---|
| 20261103 | 58 | 41 | 17 | 32 | £3,489.39 |
| 20261104 | 63 | 50 | 13 | 36 | £3,191.84 |
| 20261105 | 61 | 48 | 13 | 39 | £4,011.28 |

## When something fails

See [RECOVERY.md](RECOVERY.md) for the recovery plan: missing files, failed quality checks, runs that stopped halfway, and backfilling missed days.

## Known limitations

* **Local files only.** Data is read from and written to the project folder. A production version would use cloud storage or a database.
* **Schedule time.** The DAG runs at midnight (UTC) for the new day, before that day's files exist. In production it would run later in the morning, or wait for the files with a sensor instead of failing.
* **Unknown customers are rejected.** Orders from customers not in the day's snapshot go to the rejected file. A production version might hold them for review and process them when the customer appears.
* **Full customer snapshot every day.** The pipeline uses the snapshot of the same day. It does not keep a history of customer changes (for example, a customer moving city).
* **Freshness is relative to the run.** The check confirms the outputs were written by this run. It does not check a business deadline such as "report ready by 06:00".
* **Pandas in memory.** Suitable for the current file sizes, not for very large volumes.
