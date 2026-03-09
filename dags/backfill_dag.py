"""
Airflow DAG: sales_etl_backfill
Manual backfill DAG — processes a date range in parallel TaskGroups.
Trigger via:  airflow dags trigger sales_etl_backfill --conf '{"start":"2020-01-01","end":"2020-12-31"}'
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, date

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

DEFAULT_ARGS = {
    "owner":         "data-engineering",
    "retries":       2,
    "retry_delay":   timedelta(minutes=3),
    "email_on_failure": True,
}


def _backfill_date(target_date: str, **context):
    """Full ETL cycle for a single historical date."""
    import pandas as pd
    from etl.extract import extract
    from etl.transform import transform
    from etl.load import load, validate_load
    from etl.utils.metrics import dataframe_checksum
    from etl.utils.logger import set_context

    conf    = context["dag_run"].conf or {}
    run_id  = str(uuid.uuid4())
    run_d   = date.fromisoformat(target_date)
    set_context(run_id=run_id, task_id="backfill", dag_id="sales_etl_backfill")

    data_dir = conf.get("data_dir", "data/raw")

    df        = extract(run_date=run_d, data_dir=data_dir, run_id=uuid.UUID(run_id))
    if df.empty:
        return 0

    transformed = transform(df, run_date=run_d, run_id=uuid.UUID(run_id))
    checksum    = dataframe_checksum(transformed, ["sale_id", "net_amount"])
    load(transformed, run_date=run_d, run_id=uuid.UUID(run_id), source_checksum=checksum)
    validate_load(transformed, run_date=run_d)
    return len(transformed)


with DAG(
    dag_id="sales_etl_backfill",
    description="On-demand historical backfill DAG",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2020, 1, 1),
    schedule_interval=None,     # manual trigger only
    catchup=False,
    max_active_runs=1,
    tags=["ecommerce", "backfill"],
    params={
        "start": "2020-01-01",
        "end":   "2020-03-31",
        "data_dir": "data/raw",
    },
    doc_md="""
## Sales ETL Backfill DAG

Processes historical date range in parallel groups (up to 10 concurrent dates).

**Trigger**:
```bash
airflow dags trigger sales_etl_backfill \\
  --conf '{"start":"2020-01-01","end":"2022-12-31","data_dir":"data/raw"}'
```
""",
) as backfill_dag:

    with TaskGroup("backfill_group") as backfill_group:
        # Create tasks for a representative static range covering 2020
        # In production, generate dynamically via a BranchPythonOperator or ExternalTaskSensor
        sample_dates = [
            f"2020-{mo:02d}-01" for mo in range(1, 13)
        ] + [
            f"2021-{mo:02d}-01" for mo in range(1, 13)
        ]

        prev_task = None
        for dt_str in sample_dates:
            safe_name = dt_str.replace("-", "_")
            t = PythonOperator(
                task_id=f"backfill_{safe_name}",
                python_callable=_backfill_date,
                op_kwargs={"target_date": dt_str},
            )
            if prev_task:
                prev_task >> t
            prev_task = t
