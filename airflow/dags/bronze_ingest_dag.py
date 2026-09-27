from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from apexmart_common import (
    TABLES, get_pg_conn, get_ch_client, get_watermark, set_watermark, LOOKBACK_MINUTES,
)

default_args = {"owner": "data-eng", "retries": 3, "retry_delay": timedelta(minutes=2)}


def extract_table(table_name: str, **context):
    cfg = TABLES[table_name]
    run_id = context["run_id"]

    watermark = get_watermark(table_name)
    watermark_start = watermark - timedelta(minutes=LOOKBACK_MINUTES)
    window_end = context["data_interval_end"]

    cols = cfg["columns"]
    col_list = ", ".join(cols)
    sql = f"""
        SELECT {col_list} FROM {cfg['pg_table']}
        WHERE updated_at > %s AND updated_at <= %s
        ORDER BY updated_at
    """

    with get_pg_conn() as pg_conn:
        with pg_conn.cursor() as cur:
            cur.execute(sql, (watermark_start, window_end))
            rows = cur.fetchall()

    ch = get_ch_client()
    max_updated_at = None
    if rows:
        # append the two metadata columns your bronze schema actually has
        payload = [list(r) + [run_id, window_end] for r in rows]
        ch.insert(
            f"bronze.{table_name}",
            payload,
            column_names=cols + ["_ingestion_run_id", "_source_watermark"],
        )
        updated_at_idx = cols.index("updated_at")
        max_updated_at = max(r[updated_at_idx] for r in rows)

    new_watermark = max_updated_at if max_updated_at else window_end
    set_watermark(table_name, new_watermark, run_id)
    print(f"{table_name}: extracted {len(rows)} rows, watermark -> {new_watermark}")


with DAG(
    dag_id="bronze_ingest",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["bronze"],
) as dag:
    for table_name in TABLES:
        PythonOperator(
            task_id=f"extract_{table_name}",
            python_callable=extract_table,
            op_kwargs={"table_name": table_name},
        )