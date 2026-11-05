# BrightCart Pipeline: Recovery Plan

What to do when a run fails or stops halfway.

## The golden rule

**Fix the cause first, then rerun.** Clearing or rerunning a task before fixing the cause only repeats the same failure.

Every recovery follows the same order:

1. Find the **first** task that failed (not the last red one).
2. Open its log and read the clearest error message.
3. Decide: is the cause **temporary** (will fix itself) or **deterministic** (needs a fix)?
4. Fix or restore the cause, and check the fix.
5. Rerun: retry, clear, or trigger a new run.
6. Check the downstream tasks and the audit file: `data/audit/<run_date>.json` must show `"status": "success"`.

## Why reruns are safe

The pipeline is **idempotent**: every output is replaced, never appended. Running the same day twice produces the same files and the same totals. The raw files in `data/raw/` are never modified, so any day can always be rebuilt from them.

## Where to look

| What you see | Most likely cause | Where to look |
|---|---|---|
| DAG does not appear in the UI | Syntax error or failed import in the DAG file, or Airflow is reading another DAG folder | Import errors banner in the UI; the value of `AIRFLOW__CORE__DAGS_FOLDER` |
| `ModuleNotFoundError: pipeline_tasks` | The `scripts` folder is not next to `dags` | Project structure: `dags/` and `scripts/` must be in the same project folder |
| `check_input_files` failed | Source file missing, late or in the wrong folder | Its log: the message names the missing file and the folder |
| `clean_orders` failed | Unexpected file content (missing column, broken join) | Its log: the error names the column or the join problem |
| `run_quality_check` failed | The outputs disagree, or they are stale | Its log: each check is printed as PASS or FAIL |
| A task shows `upstream_failed` | An earlier task failed | Go back up the chain to the first failed task |
| A task shows `up_for_retry` | It failed once and Airflow will try again in 1 minute | Wait for the retry before acting |

## Scenario 1: the source file did not arrive

**What you see**

* `check_input_files` goes to `up_for_retry`, then `failed`.
* The other four tasks are `upstream_failed`.
* Log: `FileNotFoundError: [20261106] Missing input file(s) in .../data/raw/20261106: ['customers.csv', 'orders.csv']`

**Temporary or deterministic?** Temporary if the file is only late (the automatic retry may solve it). Deterministic if the file is missing, misnamed or in the wrong folder.

**Recovery**

1. Check the folder named in the log. Is the folder name the right date format (`YYYYMMDD`)? Are the files named exactly `customers.csv` and `orders.csv`?
2. Get the files from the source and place them in `data/raw/<run_date>/`. Never create or edit the data by hand.
3. In the Airflow UI, open the failed run, select `check_input_files` and click **Clear** including **downstream** tasks. The same run starts again from that task.
4. Confirm all five tasks are green and the audit shows `"status": "success"`.

## Scenario 2: the quality check failed

**What you see**

* `check_input_files`, `clean_orders` and `build_city_summary` are green.
* `run_quality_check` is `failed` (it has no retry on purpose: retrying bad data gives the same result).
* `publish_completion_message` is `upstream_failed`, so the report is **not** announced.
* Log: lines such as `FAIL  revenue: city report = audit`.
* The audit file shows `"status": "failed"` and which checks failed.

**Recovery**

1. Read which check failed:
   * **counts do not reconcile**: rows were lost or created. Look at the validation code in `clean_orders`.
   * **revenue does not match**: a curated or audit file was edited by hand, or the summary code changed. Never edit outputs by hand.
   * **duplicate order_id or missing values in valid output**: a validation rule is not working.
   * **outputs are not fresh**: the report on disk is from an earlier run. `build_city_summary` did not really write new files.
2. Fix the code or restore the files from the pipeline (not by hand).
3. Test manually first: `python scripts/pipeline_tasks.py <run_date>`.
4. In the UI, clear `clean_orders` **with downstream** tasks, so every output is rebuilt from the raw files.

## Scenario 3: the run stopped halfway

**What you see**

* For example, `clean_orders` succeeded but `build_city_summary` failed (out of disk space, machine restarted, a crash).
* `data/cleaned/<run_date>/` exists, but `data/curated/<run_date>/` is missing or old.
* The audit is missing or shows `"status": "pending_quality_check"`.

**Recovery**

1. Read the log of the failed task and fix the cause.
2. Clear the failed task **with downstream** tasks. There is no need to delete the partial files first: they are replaced.
3. If you are unsure which files are trustworthy, clear from `clean_orders` and rebuild the whole day.
4. The run is only finished when `run_quality_check` is green and the audit shows `"status": "success"`.

## Scenario 4: several days were missed (backfill)

**What you see**

The pipeline was paused or broken for some days, for example 20261103 to 20261105.

**Recovery**

1. Check that the raw files exist for every missing day.
2. Check which days already have outputs in `data/audit/`.
3. Process the days **in order**, one at a time:
   * in the UI, **Trigger** with the `run_date` parameter (`20261103`, then `20261104`, then `20261105`); or
   * manually: `python scripts/pipeline_tasks.py 20261103 20261104 20261105`
4. Validate each day: its audit must show `"status": "success"`.
5. Because outputs are replaced, reprocessing a day that already existed does not duplicate anything.

## Retry, clear or new run?

| Action | What it does | Use it when |
|---|---|---|
| **Retry** | Airflow tries the same task again automatically (1 retry, after 1 minute) | The cause may be temporary, such as a file arriving a little late |
| **Clear** | Resets selected tasks inside an existing run so they run again | You fixed the cause and want to finish the same run |
| **Trigger new run** | Creates a separate run | You want to process a specific day with `run_date`, or test after a change |

## Daily operational checklist

A green DAG only proves the code finished. Every morning, also check:

* [ ] The DAG run for the day is `success`, and no task is `failed` or `upstream_failed`.
* [ ] Both source files exist in `data/raw/<run_date>/`.
* [ ] The outputs exist in `data/cleaned/<run_date>/` and `data/curated/<run_date>/`.
* [ ] The outputs are fresh (the freshness check passed).
* [ ] Counts reconcile: `source = valid + rejected` for customers and orders.
* [ ] The rejected count is in the usual range. A sudden jump means a problem in the source system.
* [ ] Completed revenue looks plausible compared with previous days.
