SET mutations_sync = 1;
-- ---------- Dimensions (full rebuild every run) ----------

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