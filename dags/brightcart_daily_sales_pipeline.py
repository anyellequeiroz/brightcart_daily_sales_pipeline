"""
BrightCart Daily Sales Pipeline: Airflow DAG.

This file only describes ORCHESTRATION: schedule, retries, task order.
All business logic lives in scripts/pipeline_tasks.py, so the same code runs
manually (python scripts/pipeline_tasks.py) and under Airflow.

    check_input_files >> clean_orders >> build_city_summary >> run_quality_check >> publish_completion_message

Which day is processed?
    * Scheduled runs use the run's logical date (ds_nodash, e.g. 20261105).
    * Manual runs can choose a day with the "run_date" parameter in the
      trigger form, e.g. 20261103. Leave it empty to use the logical date.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

# The DAG lives in /dags and the functions in /scripts.
# We add /scripts to the Python path, built from this file's location,
# so it works on any computer without hard coded paths.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT / "scripts"))

from pipeline_tasks import (  # noqa: E402
    check_input_files,
    clean_orders,
    build_city_summary,
    run_quality_check,
)

# One retry with a short delay: helps with temporary problems (a file arriving
# a little late), but does not fix bugs or bad data.
DEFAULT_ARGS = {
    "owner": "brightcart",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

# Use the run_date parameter if one was given, otherwise the run's logical date
RUN_DATE = "{{ params.run_date if params.run_date else ds_nodash }}"

with DAG(
    dag_id="brightcart_daily_sales_pipeline",
    description="Validate BrightCart daily files, build the sales report and audit",
    start_date=datetime(2026, 11, 1),
    schedule="@daily",
    catchup=False,
    default_args=DEFAULT_ARGS,
    params={
        "run_date": Param(
            "",
            type="string",
            description="Day to process as YYYYMMDD (e.g. 20261105). Leave empty to use the logical date.",
        ),
    },
    tags=["brightcart", "mini project"],
) as dag:

    check_files = PythonOperator(
        task_id="check_input_files",
        python_callable=check_input_files,
        op_kwargs={"run_date": RUN_DATE},
    )

    clean = PythonOperator(
        task_id="clean_orders",
        python_callable=clean_orders,
        op_kwargs={"run_date": RUN_DATE},
    )

    build_summary = PythonOperator(
        task_id="build_city_summary",
        python_callable=build_city_summary,
        op_kwargs={"run_date": RUN_DATE},
    )

    # Publishing gate: if any check fails, this task fails and the
    # completion message below never runs
    quality_check = PythonOperator(
        task_id="run_quality_check",
        python_callable=run_quality_check,
        op_kwargs={"run_date": RUN_DATE},
        retries=0,  # a failed check is a data problem, retrying gives the same result
    )

    publish_message = BashOperator(
        task_id="publish_completion_message",
        bash_command=f'echo "BrightCart pipeline completed successfully for {RUN_DATE}"',
    )

    # The arrows define the ORDER only. Tasks share results through files,
    # not by passing DataFrames.
    check_files >> clean >> build_summary >> quality_check >> publish_message
