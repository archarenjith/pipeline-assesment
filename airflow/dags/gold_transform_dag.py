from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from apexmart_common import get_ch_client

default_args = {"owner": "data-eng", "retries": 2, "retry_delay": timedelta(minutes=2)}

GOLD_SQL = """
TRUNCATE TABLE gold.dim_customers;
INSERT INTO gold.dim_customers
SELECT customer_id, customer_code, concat(first_name, ' ', last_name), email, city, state,
       customer_segment, active
FROM silver.customers;

TRUNCATE TABLE gold.dim_products;
INSERT INTO gold.dim_products
SELECT p.product_id, p.sku, p.product_name, c.category_name, s.supplier_name,
       p.unit_cost, p.unit_price, p.reorder_level, p.active
FROM silver.products p
LEFT JOIN silver.categories c ON c.category_id = p.category_id
LEFT JOIN silver.suppliers s ON s.supplier_id = p.supplier_id;

TRUNCATE TABLE gold.dim_stores;
INSERT INTO gold.dim_stores
SELECT store_id, store_code, store_name, city, state, opened_on, active FROM silver.stores;

TRUNCATE TABLE gold.dim_warehouses;
INSERT INTO gold.dim_warehouses
SELECT warehouse_id, warehouse_code, warehouse_name, city, state, capacity_units, active
FROM silver.warehouses;

TRUNCATE TABLE gold.dim_employees;
INSERT INTO gold.dim_employees
SELECT employee_id, employee_code, concat(first_name, ' ', last_name), role, store_id, hired_on, active
FROM silver.employees;

TRUNCATE TABLE gold.dim_date;
INSERT INTO gold.dim_date
SELECT
    d AS date_key,
    toYear(d), toQuarter(d), toMonth(d), formatDateTime(d, '%M'),
    toWeek(d), toDayOfWeek(d), formatDateTime(d, '%W'),
    if(toDayOfWeek(d) IN (6,7), 1, 0)
FROM (
    SELECT toDate('2021-01-01') + number AS d
    FROM numbers(3650)
) AS dates;

ALTER TABLE gold.fact_sales DELETE
WHERE order_date >= today() - 7 AND order_date <= today();

INSERT INTO gold.fact_sales
SELECT
    toDate(o.order_ts) AS order_date,
    o.order_id, i.order_item_id, o.customer_id, o.store_id, i.product_id,
    o.status, o.payment_status, i.quantity, i.unit_price, i.discount_amount,
    i.line_amount, o.order_ts, o.dq_late_arrival
FROM silver.sales_order_items i
INNER JOIN silver.sales_orders o ON o.order_id = i.order_id
WHERE toDate(o.order_ts) >= today() - 7 AND toDate(o.order_ts) <= today();

ALTER TABLE gold.fact_payments DELETE
WHERE payment_date >= today() - 7 AND payment_date <= today();

INSERT INTO gold.fact_payments
SELECT toDate(payment_ts), payment_id, order_id, payment_method, amount, status, payment_ts
FROM silver.payments
WHERE toDate(payment_ts) >= today() - 7 AND toDate(payment_ts) <= today();

ALTER TABLE gold.fact_inventory_movements DELETE
WHERE movement_date >= today() - 7 AND movement_date <= today();

INSERT INTO gold.fact_inventory_movements
SELECT toDate(movement_ts), movement_id, product_id, warehouse_id, movement_type, quantity,
       reference_type, movement_ts, dq_anomaly, dq_seed_noise
FROM silver.inventory_movements
WHERE toDate(movement_ts) >= today() - 7 AND toDate(movement_ts) <= today();

TRUNCATE TABLE gold.fact_inventory_daily_balance;
INSERT INTO gold.fact_inventory_daily_balance
SELECT
    movement_date, product_id, warehouse_id, daily_net_qty,
    sum(daily_net_qty) OVER (
        PARTITION BY product_id, warehouse_id ORDER BY movement_date
        ROWS UNBOUNDED PRECEDING
    ) AS running_balance
FROM (
    SELECT movement_date, product_id, warehouse_id, sum(quantity) AS daily_net_qty
    FROM (
        SELECT toDate(movement_ts) AS movement_date, product_id, warehouse_id, quantity
        FROM silver.inventory_movements
    )
    GROUP BY movement_date, product_id, warehouse_id
);
"""


def run_gold_transform(**context):
    ch = get_ch_client()
    ch.command("SET mutations_sync = 1")
    statements = [s.strip() for s in GOLD_SQL.split(";") if s.strip()]
    for stmt in statements:
        ch.command(stmt)
    print(f"Gold transform complete: {len(statements)} statements executed")


with DAG(
    dag_id="gold_transform",
    default_args=default_args,
    schedule_interval=timedelta(minutes=15),
    start_date=datetime(2026, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    tags=["gold"],
) as dag:
    PythonOperator(task_id="transform_gold", python_callable=run_gold_transform)