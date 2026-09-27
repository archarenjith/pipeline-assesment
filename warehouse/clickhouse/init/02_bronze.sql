-- Bronze = raw extraction log. One table per source table.
-- Append-oriented: every extraction run inserts rows here, including
-- re-extractions caused by lookback windows or retries.
-- ReplacingMergeTree(updated_at) means ClickHouse will eventually merge
-- away older physical duplicates in the background, keeping the version
-- with the highest updated_at for a given sort key. Until merges happen,
-- duplicates may exist physically -- Silver is responsible for resolving
-- that logically (see 03_silver.sql), so we never rely on merge timing
-- for correctness.

CREATE DATABASE IF NOT EXISTS bronze;

-- ---------------- categories ----------------
CREATE TABLE IF NOT EXISTS bronze.categories (
    category_id Int64,
    category_name String,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY category_id;

-- ---------------- suppliers ----------------
CREATE TABLE IF NOT EXISTS bronze.suppliers (
    supplier_id Int64,
    supplier_code String,
    supplier_name String,
    city String,
    state String,
    country String,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY supplier_id;

-- ---------------- customers ----------------
CREATE TABLE IF NOT EXISTS bronze.customers (
    customer_id Int64,
    customer_code String,
    first_name String,
    last_name String,
    email String,
    phone String,
    city String,
    state String,
    customer_segment String,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY customer_id;

-- ---------------- products ----------------
CREATE TABLE IF NOT EXISTS bronze.products (
    product_id Int64,
    sku String,
    product_name String,
    category_id Int64,
    supplier_id Int64,
    unit_cost Decimal(12,2),
    unit_price Decimal(12,2),
    reorder_level Int32,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY product_id;

-- ---------------- stores ----------------
CREATE TABLE IF NOT EXISTS bronze.stores (
    store_id Int64,
    store_code String,
    store_name String,
    city String,
    state String,
    opened_on Date,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY store_id;

-- ---------------- warehouses ----------------
CREATE TABLE IF NOT EXISTS bronze.warehouses (
    warehouse_id Int64,
    warehouse_code String,
    warehouse_name String,
    city String,
    state String,
    capacity_units Int32,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY warehouse_id;

-- ---------------- employees ----------------
CREATE TABLE IF NOT EXISTS bronze.employees (
    employee_id Int64,
    employee_code String,
    first_name String,
    last_name String,
    role String,
    store_id Nullable(Int64),
    hired_on Date,
    active UInt8,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY employee_id;

-- ---------------- sales_orders ----------------
CREATE TABLE IF NOT EXISTS bronze.sales_orders (
    order_id Int64,
    order_number String,
    customer_id Int64,
    store_id Int64,
    order_ts DateTime64(6),
    status String,
    payment_status String,
    total_amount Decimal(14,2),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_id
PARTITION BY toYYYYMM(order_ts);

-- ---------------- sales_order_items ----------------
CREATE TABLE IF NOT EXISTS bronze.sales_order_items (
    order_item_id Int64,
    order_id Int64,
    product_id Int64,
    quantity Int32,
    unit_price Decimal(12,2),
    discount_amount Decimal(12,2),
    line_amount Decimal(14,2),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY order_item_id;

-- ---------------- purchase_orders ----------------
CREATE TABLE IF NOT EXISTS bronze.purchase_orders (
    purchase_order_id Int64,
    po_number String,
    supplier_id Int64,
    warehouse_id Int64,
    order_ts DateTime64(6),
    status String,
    total_amount Decimal(14,2),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY purchase_order_id;

-- ---------------- purchase_order_items ----------------
CREATE TABLE IF NOT EXISTS bronze.purchase_order_items (
    purchase_item_id Int64,
    purchase_order_id Int64,
    product_id Int64,
    quantity Int32,
    unit_cost Decimal(12,2),
    line_amount Decimal(14,2),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY purchase_item_id;

-- ---------------- payments ----------------
CREATE TABLE IF NOT EXISTS bronze.payments (
    payment_id Int64,
    order_id Int64,
    payment_ts DateTime64(6),
    payment_method String,
    amount Decimal(14,2),
    status String,
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY payment_id;

-- ---------------- inventory_movements ----------------
CREATE TABLE IF NOT EXISTS bronze.inventory_movements (
    movement_id Int64,
    product_id Int64,
    warehouse_id Int64,
    movement_ts DateTime64(6),
    movement_type String,
    quantity Int32,
    reference_type Nullable(String),
    reference_id Nullable(Int64),
    created_at DateTime64(6),
    updated_at DateTime64(6),
    _ingested_at DateTime64(6) DEFAULT now(),
    _ingestion_run_id String,
    _source_watermark DateTime64(6)
) ENGINE = ReplacingMergeTree(updated_at)
ORDER BY movement_id;