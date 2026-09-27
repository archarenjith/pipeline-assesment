CREATE DATABASE IF NOT EXISTS silver;

-- ---------- Master data (latest-per-PK, no extra DQ flags needed) ----------

CREATE TABLE IF NOT EXISTS silver.categories
(
    category_id Int64, category_name String,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY category_id;

CREATE TABLE IF NOT EXISTS silver.suppliers
(
    supplier_id Int64, supplier_code String, supplier_name String,
    city String, state String, country String, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY supplier_id;

CREATE TABLE IF NOT EXISTS silver.customers
(
    customer_id Int64, customer_code String, first_name String, last_name String,
    email String, phone Nullable(String), city String, state String,
    customer_segment String, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS silver.products
(
    product_id Int64, sku String, product_name String, category_id Int64, supplier_id Int64,
    unit_cost Decimal(12,2), unit_price Decimal(12,2), reorder_level Int32, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS silver.stores
(
    store_id Int64, store_code String, store_name String, city String, state String,
    opened_on Date, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY store_id;

CREATE TABLE IF NOT EXISTS silver.warehouses
(
    warehouse_id Int64, warehouse_code String, warehouse_name String, city String, state String,
    capacity_units Int32, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY warehouse_id;

CREATE TABLE IF NOT EXISTS silver.employees
(
    employee_id Int64, employee_code String, first_name String, last_name String, role String,
    store_id Nullable(Int64), hired_on Date, active UInt8,
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY employee_id;

-- ---------- Transactional data (+ DQ flags) ----------

CREATE TABLE IF NOT EXISTS silver.sales_orders
(
    order_id        Int64,
    order_number    String,
    customer_id     Int64,
    store_id        Int64,
    order_ts        DateTime64(6,'UTC'),
    status          String,
    payment_status  String,
    total_amount    Decimal(14,2),
    created_at      DateTime64(6,'UTC'),
    updated_at      DateTime64(6,'UTC'),
    dq_late_arrival      UInt8,
    dq_total_mismatch    UInt8,
    dq_orphan_customer   UInt8,
    dq_orphan_store      UInt8
) ENGINE = MergeTree ORDER BY order_id;

CREATE TABLE IF NOT EXISTS silver.sales_order_items
(
    order_item_id     Int64,
    order_id          Int64,
    product_id        Int64,
    quantity          Int32,
    unit_price        Decimal(12,2),
    discount_amount   Decimal(12,2),
    line_amount       Decimal(14,2),
    created_at        DateTime64(6,'UTC'),
    updated_at        DateTime64(6,'UTC'),
    dq_orphan_order    UInt8,
    dq_orphan_product  UInt8
) ENGINE = MergeTree ORDER BY order_item_id;

CREATE TABLE IF NOT EXISTS silver.purchase_orders
(
    purchase_order_id Int64, po_number String, supplier_id Int64, warehouse_id Int64,
    order_ts DateTime64(6,'UTC'), status String, total_amount Decimal(14,2),
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY purchase_order_id;

CREATE TABLE IF NOT EXISTS silver.purchase_order_items
(
    purchase_item_id Int64, purchase_order_id Int64, product_id Int64,
    quantity Int32, unit_cost Decimal(12,2), line_amount Decimal(14,2),
    created_at DateTime64(6,'UTC'), updated_at DateTime64(6,'UTC')
) ENGINE = MergeTree ORDER BY purchase_item_id;

CREATE TABLE IF NOT EXISTS silver.payments
(
    payment_id       Int64,
    order_id         Int64,
    payment_ts       DateTime64(6,'UTC'),
    payment_method   String,
    amount           Decimal(14,2),
    status           String,
    created_at       DateTime64(6,'UTC'),
    updated_at       DateTime64(6,'UTC'),
    dq_orphan_order    UInt8,
    dq_amount_mismatch UInt8
) ENGINE = MergeTree ORDER BY payment_id;

CREATE TABLE IF NOT EXISTS silver.inventory_movements
(
    movement_id      Int64,
    product_id       Int64,
    warehouse_id     Int64,
    movement_ts      DateTime64(6,'UTC'),
    movement_type    String,
    quantity         Int32,
    reference_type   Nullable(String),
    reference_id     Nullable(Int64),
    created_at       DateTime64(6,'UTC'),
    updated_at       DateTime64(6,'UTC'),
    dq_anomaly        UInt8,
    dq_orphan_product UInt8,
    dq_seed_noise     UInt8
) ENGINE = MergeTree ORDER BY movement_id;