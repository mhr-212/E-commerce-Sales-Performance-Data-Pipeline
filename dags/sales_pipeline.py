"""
Airflow DAG: sales_etl
Daily ETL pipeline — extract → validate_extract → transform → load → validate_load → refresh_mart

Schedule: @daily (UTC midnight)
Catchup:  True (to support backfills from start_date)
SLA:      2 hours per full run
Retries:  3 with exponential back-off on transform and load tasks
"""
from __future__ import annotations

import uuid
from datetime import date, datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

# ---------------------------------------------------------------------------
# Default arguments — applied to all tasks unless overridden
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":             "data-engineering",
    "depends_on_past":   False,
    "email":             ["data-alerts@example.com"],
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":   timedelta(minutes=30),
    "execution_timeout": timedelta(hours=1),
    "sla":               timedelta(hours=2),
}

# ---------------------------------------------------------------------------
# Task callables — thin wrappers that inject context and delegate to etl/
# ---------------------------------------------------------------------------

def _make_run_id(context) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, f"sales_etl_{context['ds']}"))


def extract_callable(**context):
    from etl.utils.logger import set_context
    from etl.extract import extract

    run_id  = _make_run_id(context)
    run_date = date.fromisoformat(context["ds"])
    set_context(run_id=run_id, task_id="extract", dag_id="sales_etl")

    data_dir = Variable.get("SALES_DATA_DIR", default_var="data/raw")
    df = extract(
        run_date=run_date,
        data_dir=data_dir,
        run_id=uuid.UUID(run_id),
        dag_id="sales_etl",
        task_id="extract",
    )
    # Push row count to XCom for downstream validation
    context["ti"].xcom_push(key="extract_row_count", value=len(df))
    context["ti"].xcom_push(key="run_id", value=run_id)
    # Pickle-friendly: store as parquet in /tmp
    df.to_parquet(f"/tmp/extract_{context['ds']}.parquet", index=False)
    return len(df)


def validate_extract_callable(**context):
    import pandas as pd
    from etl.utils.logger import set_context

    run_id = context["ti"].xcom_pull(task_ids="extract", key="run_id")
    set_context(run_id=run_id, task_id="validate_extract", dag_id="sales_etl")

    df = pd.read_parquet(f"/tmp/extract_{context['ds']}.parquet")
    assert not df.empty, "Extract produced 0 rows — aborting pipeline."
    assert df["sale_id"].nunique() == len(df), "Duplicate sale_ids detected after extract!"
    assert (df["net_amount"] >= 0).all(), "Negative net_amounts found in extracted data!"

    import logging
    logging.getLogger("airflow").info(
        f"[validate_extract] ✅ {len(df):,} rows, {df['sale_id'].nunique():,} unique sale_ids"
    )


def transform_callable(**context):
    import pandas as pd
    from etl.transform import transform
    from etl.utils.logger import set_context

    run_id   = context["ti"].xcom_pull(task_ids="extract", key="run_id")
    run_date = date.fromisoformat(context["ds"])
    set_context(run_id=run_id, task_id="transform", dag_id="sales_etl")

    df = pd.read_parquet(f"/tmp/extract_{context['ds']}.parquet")

    # Load dim slices from DB for surrogate key resolution
    products_df  = _load_dim("dim_product",  "product_id", "product_sk")
    customers_df = _load_dim("dim_customer", "customer_id", "customer_sk")

    transformed = transform(
        df,
        run_date=run_date,
        products_df=products_df,
        customers_df=customers_df,
        run_id=uuid.UUID(run_id),
        dag_id="sales_etl",
        task_id="transform",
    )
    transformed.to_parquet(f"/tmp/transform_{context['ds']}.parquet", index=False)
    return len(transformed)


def load_callable(**context):
    import pandas as pd
    from etl.load import load
    from etl.utils.logger import set_context
    from etl.utils.metrics import dataframe_checksum

    run_id   = context["ti"].xcom_pull(task_ids="extract", key="run_id")
    run_date = date.fromisoformat(context["ds"])
    set_context(run_id=run_id, task_id="load", dag_id="sales_etl")

    df = pd.read_parquet(f"/tmp/transform_{context['ds']}.parquet")
    checksum = dataframe_checksum(df, ["sale_id", "net_amount"])

    result = load(
        df,
        run_date=run_date,
        run_id=uuid.UUID(run_id),
        dag_id="sales_etl",
        task_id="load",
        source_checksum=checksum,
    )
    context["ti"].xcom_push(key="rows_inserted", value=result["rows_inserted"])
    context["ti"].xcom_push(key="target_checksum", value=result["target_checksum"])
    return result["rows_inserted"]


def validate_load_callable(**context):
    import pandas as pd
    from etl.load import validate_load
    from etl.utils.logger import set_context

    run_id   = context["ti"].xcom_pull(task_ids="extract", key="run_id")
    run_date = date.fromisoformat(context["ds"])
    set_context(run_id=run_id, task_id="validate_load", dag_id="sales_etl")

    df = pd.read_parquet(f"/tmp/transform_{context['ds']}.parquet")
    validate_load(df, run_date=run_date)


def refresh_mart_callable(**context):
    from etl.utils.db import execute_sql
    from etl.utils.logger import set_context

    run_id = context["ti"].xcom_pull(task_ids="extract", key="run_id")
    set_context(run_id=run_id, task_id="refresh_mart", dag_id="sales_etl")
    execute_sql("REFRESH MATERIALIZED VIEW CONCURRENTLY mart_daily_sales")

    import logging
    logging.getLogger("airflow").info("[refresh_mart] ✅ mart_daily_sales refreshed")


def cleanup_callable(**context):
    """Remove temp parquet files from /tmp."""
    import os
    ds = context["ds"]
    for stage in ("extract", "transform"):
        fpath = f"/tmp/{stage}_{ds}.parquet"
        try:
            os.remove(fpath)
        except FileNotFoundError:
            pass


def _load_dim(table: str, nk_col: str, sk_col: str):
    """Load a dimension table's NK→SK mapping from PostgreSQL."""
    import pandas as pd
    from etl.utils.db import get_engine
    return pd.read_sql(
        f"SELECT {nk_col}, {sk_col} FROM {table} WHERE is_active = TRUE",
        get_engine(),
    )


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="sales_etl",
    description="Daily e-commerce sales ETL: extract → transform → load → validate",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2020, 1, 1),
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=3,
    tags=["ecommerce", "sales", "etl", "production"],
    doc_md="""
## Sales ETL DAG

**Flow**: extract → validate_extract → transform → load → validate_load → refresh_mart → cleanup

| Task | Retries | SLA |
|------|---------|-----|
| extract | 3 | 30 min |
| transform | 3 | 45 min |
| load | 3 | 30 min |
| validate_load | 0 | 10 min |
| refresh_mart | 1 | 15 min |

All metrics are recorded in `pipeline_audit_log`.
""",
) as dag:

    start = EmptyOperator(task_id="start")

    extract_task = PythonOperator(
        task_id="extract",
        python_callable=extract_callable,
        execution_timeout=timedelta(minutes=30),
    )

    validate_extract_task = PythonOperator(
        task_id="validate_extract",
        python_callable=validate_extract_callable,
        retries=0,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_callable,
        execution_timeout=timedelta(minutes=45),
    )

    load_task = PythonOperator(
        task_id="load",
        python_callable=load_callable,
        execution_timeout=timedelta(minutes=30),
    )

    validate_load_task = PythonOperator(
        task_id="validate_load",
        python_callable=validate_load_callable,
        retries=0,
        execution_timeout=timedelta(minutes=10),
    )

    refresh_mart_task = PythonOperator(
        task_id="refresh_mart",
        python_callable=refresh_mart_callable,
        retries=1,
        execution_timeout=timedelta(minutes=15),
    )

    cleanup_task = PythonOperator(
        task_id="cleanup",
        python_callable=cleanup_callable,
        trigger_rule="all_done",   # runs even if upstream failed
    )

    end = EmptyOperator(task_id="end")

    # Task dependency graph
    (
        start
        >> extract_task
        >> validate_extract_task
        >> transform_task
        >> load_task
        >> validate_load_task
        >> refresh_mart_task
        >> cleanup_task
        >> end
    )
