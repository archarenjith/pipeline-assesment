from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from apexmart_common import get_ch_client

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}

SILVER_SQL = """
TRUNCATE TABLE silver.categories;
INSERT INTO silver.categories
SELECT category_id, argMax(category_name, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.categories GROUP BY category_id;

TRUNCATE TABLE silver.suppliers;
INSERT INTO silver.suppliers
SELECT supplier_id, argMax(supplier_code, updated_at), argMax(supplier_name, updated_at),
       argMax(city, updated_at), argMax(state, updated_at), argMax(country, updated_at),
       argMax(active, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.suppliers GROUP BY supplier_id;

TRUNCATE TABLE silver.customers;
INSERT INTO silver.customers
SELECT customer_id, argMax(customer_code, updated_at), argMax(first_name, updated_at),
       argMax(last_name, updated_at), argMax(email, updated_at), argMax(phone, updated_at),
       argMax(city, updated_at), argMax(state, updated_at), argMax(customer_segment, updated_at),
       argMax(active, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.customers GROUP BY customer_id;

TRUNCATE TABLE silver.products;
INSERT INTO silver.products
SELECT product_id, argMax(sku, updated_at), argMax(product_name, updated_at),
       argMax(category_id, updated_at), argMax(supplier_id, updated_at),
       argMax(unit_cost, updated_at), argMax(unit_price, updated_at),
       argMax(reorder_level, updated_at), argMax(active, updated_at),
       argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.products GROUP BY product_id;

TRUNCATE TABLE silver.stores;
INSERT INTO silver.stores
SELECT store_id, argMax(store_code, updated_at), argMax(store_name, updated_at),
       argMax(city, updated_at), argMax(state, updated_at), argMax(opened_on, updated_at),
       argMax(active, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.stores GROUP BY store_id;

TRUNCATE TABLE silver.warehouses;
INSERT INTO silver.warehouses
SELECT warehouse_id, argMax(warehouse_code, updated_at), argMax(warehouse_name, updated_at),
       argMax(city, updated_at), argMax(state, updated_at), argMax(capacity_units, updated_at),
       argMax(active, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.warehouses GROUP BY warehouse_id;

TRUNCATE TABLE silver.employees;
INSERT INTO silver.employees
SELECT employee_id, argMax(employee_code, updated_at), argMax(first_name, updated_at),
       argMax(last_name, updated_at), argMax(role, updated_at), argMax(store_id, updated_at),
       argMax(hired_on, updated_at), argMax(active, updated_at),
       argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.employees GROUP BY employee_id;

TRUNCATE TABLE silver.sales_orders;
INSERT INTO silver.sales_orders
SELECT
    o.order_id, o.order_number, o.customer_id, o.store_id, o.order_ts, o.status,
    o.payment_status, o.total_amount, o.created_at, o._uat,
    if(dateDiff('hour', o.order_ts, o.created_at) > 2, 1, 0)                 AS dq_late_arrival,
    if(abs(o.total_amount - coalesce(item_totals.sum_line, 0)) > 0.01, 1, 0) AS dq_total_mismatch,
    if(c.customer_id = 0, 1, 0)                                             AS dq_orphan_customer,
    if(s.store_id = 0, 1, 0)                                                AS dq_orphan_store
FROM
(
    SELECT order_id,
           argMax(order_number, updated_at)   AS order_number,
           argMax(customer_id, updated_at)     AS customer_id,
           argMax(store_id, updated_at)        AS store_id,
           argMax(order_ts, updated_at)        AS order_ts,
           argMax(status, updated_at)          AS status,
           argMax(payment_status, updated_at)  AS payment_status,
           argMax(total_amount, updated_at)    AS total_amount,
           argMax(created_at, updated_at)      AS created_at,
           max(updated_at)                     AS _uat
    FROM bronze.sales_orders GROUP BY order_id
) o
LEFT JOIN (
    SELECT order_id, sum(line_amount) AS sum_line
    FROM (
        SELECT order_item_id, order_id, argMax(line_amount, updated_at) AS line_amount
        FROM bronze.sales_order_items GROUP BY order_item_id, order_id
    ) GROUP BY order_id
) item_totals ON item_totals.order_id = o.order_id
LEFT JOIN silver.customers c ON c.customer_id = o.customer_id
LEFT JOIN silver.stores s ON s.store_id = o.store_id;

TRUNCATE TABLE silver.sales_order_items;
INSERT INTO silver.sales_order_items
SELECT
    i.order_item_id, i.order_id, i.product_id, i.quantity, i.unit_price,
    i.discount_amount, i.line_amount, i.created_at, i._uat,
    if(o.order_id = 0, 1, 0)   AS dq_orphan_order,
    if(p.product_id = 0, 1, 0) AS dq_orphan_product
FROM
(
    SELECT order_item_id,
           argMax(order_id, updated_at) AS order_id,
           argMax(product_id, updated_at) AS product_id,
           argMax(quantity, updated_at) AS quantity,
           argMax(unit_price, updated_at) AS unit_price,
           argMax(discount_amount, updated_at) AS discount_amount,
           argMax(line_amount, updated_at) AS line_amount,
           argMax(created_at, updated_at) AS created_at,
           max(updated_at) AS _uat
    FROM bronze.sales_order_items GROUP BY order_item_id
) i
LEFT JOIN silver.sales_orders o ON o.order_id = i.order_id
LEFT JOIN silver.products p ON p.product_id = i.product_id;

TRUNCATE TABLE silver.purchase_orders;
INSERT INTO silver.purchase_orders
SELECT purchase_order_id, argMax(po_number, updated_at), argMax(supplier_id, updated_at),
       argMax(warehouse_id, updated_at), argMax(order_ts, updated_at), argMax(status, updated_at),
       argMax(total_amount, updated_at), argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.purchase_orders GROUP BY purchase_order_id;

TRUNCATE TABLE silver.purchase_order_items;
INSERT INTO silver.purchase_order_items
SELECT purchase_item_id, argMax(purchase_order_id, updated_at), argMax(product_id, updated_at),
       argMax(quantity, updated_at), argMax(unit_cost, updated_at), argMax(line_amount, updated_at),
       argMax(created_at, updated_at), max(updated_at) AS _uat
FROM bronze.purchase_order_items GROUP BY purchase_item_id;

TRUNCATE TABLE silver.payments;
INSERT INTO silver.payments
SELECT
    py.payment_id, py.order_id, py.payment_ts, py.payment_method, py.amount, py.status,
    py.created_at, py._uat,
    if(o.order_id = 0, 1, 0) AS dq_orphan_order,
    if(py.status = 'SUCCESS' AND abs(py.amount - o.total_amount) > 0.01, 1, 0) AS dq_amount_mismatch
FROM
(
    SELECT payment_id,
           argMax(order_id, updated_at) AS order_id,
           argMax(payment_ts, updated_at) AS payment_ts,
           argMax(payment_method, updated_at) AS payment_method,
           argMax(amount, updated_at) AS amount,
           argMax(status, updated_at) AS status,
           argMax(created_at, updated_at) AS created_at,
           max(updated_at) AS _uat
    FROM bronze.payments GROUP BY payment_id
) py
LEFT JOIN silver.sales_orders o ON o.order_id = py.order_id;

TRUNCATE TABLE silver.inventory_movements;
INSERT INTO silver.inventory_movements
SELECT
    m.movement_id, m.product_id, m.warehouse_id, m.movement_ts, m.movement_type, m.quantity,
    m.reference_type, m.reference_id, m.created_at, m._uat,
    if(m.reference_type = 'ANOMALY' OR (m.movement_type = 'ADJUSTMENT' AND abs(m.quantity) > 200), 1, 0) AS dq_anomaly,
    if(p.product_id = 0, 1, 0) AS dq_orphan_product,
    if(m.reference_type = 'SEED', 1, 0) AS dq_seed_noise
FROM
(
    SELECT movement_id,
           argMax(product_id, updated_at) AS product_id,
           argMax(warehouse_id, updated_at) AS warehouse_id,
           argMax(movement_ts, updated_at) AS movement_ts,
           argMax(movement_type, updated_at) AS movement_type,
           argMax(quantity, updated_at) AS quantity,
           argMax(reference_type, updated_at) AS reference_type,
           argMax(reference_id, updated_at) AS reference_id,
           argMax(created_at, updated_at) AS created_at,
           max(updated_at) AS _uat
    FROM bronze.inventory_movements GROUP BY movement_id
) m
LEFT JOIN silver.products p ON p.product_id = m.product_id;
"""


def run_silver_transform(**context):
    ch = get_ch_client()
    ch.command("SET mutations_sync = 1")
    statements = [s.strip() for s in SILVER_SQL.split(";") if s.strip()]
    for stmt in statements:
        ch.command(stmt)
    print(f"Silver transform complete: {len(statements)} statements executed")


with DAG(
    dag_id="silver_transform",
    default_args=default_args,
    schedule_interval=timedelta(minutes=10),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["silver"],
) as dag:
    PythonOperator(task_id="transform_silver", python_callable=run_silver_transform)