"""
Shared helpers for the ApexMart pipeline DAG.
Keeps connection + watermark logic in one place so DAG tasks stay readable.
"""
import os
import uuid
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
import clickhouse_connect

# ---------------- Source Postgres (ERP) connection ----------------
def get_pg_conn():
    return psycopg2.connect(
        host=os.getenv("SOURCE_DB_HOST", "host.docker.internal"),
        port=int(os.getenv("SOURCE_DB_PORT", "5433")),
        dbname=os.getenv("SOURCE_DB_NAME", "apexmart_erp"),
        user=os.getenv("SOURCE_DB_USER", "erp"),
        password=os.getenv("SOURCE_DB_PASSWORD", "erp_password"),
    )

# ---------------- ClickHouse connection ----------------
def get_ch_client():
    return clickhouse_connect.get_client(
        host="clickhouse",
        port=8123,
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", "clickhouse"),
        send_receive_timeout=600,   # allow up to 10 minutes for heavy queries
    )

# Lookback buffer: re-extract a few minutes before the last watermark to
# safely catch rows whose updated_at was still being written when the
# previous run's query executed. Silver dedupes, so overlap is harmless.
LOOKBACK_MINUTES = 5

def get_watermark(table_name: str) -> datetime:
    """Return the last successful watermark for a table, or epoch if none exists."""
    client = get_ch_client()
    result = client.query(
        "SELECT last_watermark FROM meta.ingestion_watermarks FINAL WHERE table_name = {table:String}",
        parameters={"table": table_name},
    )
    if result.result_rows:
        return result.result_rows[0][0]
    # No watermark yet -> first run -> extract everything since the epoch.
    return datetime(1970, 1, 1, tzinfo=timezone.utc)

def set_watermark(table_name: str, new_watermark: datetime, run_id: str):
    """Advance the watermark. Only call this AFTER the Bronze write succeeds."""
    client = get_ch_client()
    client.insert(
        "meta.ingestion_watermarks",
        [[table_name, new_watermark, run_id, datetime.now(timezone.utc)]],
        column_names=["table_name", "last_watermark", "last_run_id", "last_success_at"],
    )

def new_run_id() -> str:
    return str(uuid.uuid4())
    # ---------------- Source table -> column list config ----------------
TABLES = {
    "categories": {"pg_table": "erp.categories",
        "columns": ["category_id","category_name","created_at","updated_at"]},
    "suppliers": {"pg_table": "erp.suppliers",
        "columns": ["supplier_id","supplier_code","supplier_name","city","state","country","active","created_at","updated_at"]},
    "customers": {"pg_table": "erp.customers",
        "columns": ["customer_id","customer_code","first_name","last_name","email","phone","city","state","customer_segment","active","created_at","updated_at"]},
    "products": {"pg_table": "erp.products",
        "columns": ["product_id","sku","product_name","category_id","supplier_id","unit_cost","unit_price","reorder_level","active","created_at","updated_at"]},
    "stores": {"pg_table": "erp.stores",
        "columns": ["store_id","store_code","store_name","city","state","opened_on","active","created_at","updated_at"]},
    "warehouses": {"pg_table": "erp.warehouses",
        "columns": ["warehouse_id","warehouse_code","warehouse_name","city","state","capacity_units","active","created_at","updated_at"]},
    "employees": {"pg_table": "erp.employees",
        "columns": ["employee_id","employee_code","first_name","last_name","role","store_id","hired_on","active","created_at","updated_at"]},
    "sales_orders": {"pg_table": "erp.sales_orders",
        "columns": ["order_id","order_number","customer_id","store_id","order_ts","status","payment_status","total_amount","created_at","updated_at"]},
    "sales_order_items": {"pg_table": "erp.sales_order_items",
        "columns": ["order_item_id","order_id","product_id","quantity","unit_price","discount_amount","line_amount","created_at","updated_at"]},
    "purchase_orders": {"pg_table": "erp.purchase_orders",
        "columns": ["purchase_order_id","po_number","supplier_id","warehouse_id","order_ts","status","total_amount","created_at","updated_at"]},
    "purchase_order_items": {"pg_table": "erp.purchase_order_items",
        "columns": ["purchase_item_id","purchase_order_id","product_id","quantity","unit_cost","line_amount","created_at","updated_at"]},
    "payments": {"pg_table": "erp.payments",
        "columns": ["payment_id","order_id","payment_ts","payment_method","amount","status","created_at","updated_at"]},
    "inventory_movements": {"pg_table": "erp.inventory_movements",
        "columns": ["movement_id","product_id","warehouse_id","movement_ts","movement_type","quantity","reference_type","reference_id","created_at","updated_at"]},
}