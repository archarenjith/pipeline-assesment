CREATE DATABASE IF NOT EXISTS gold;

-- ---------- Dimensions ----------

CREATE TABLE IF NOT EXISTS gold.dim_customers
(
    customer_id      Int64,
    customer_code    String,
    customer_name    String,
    email            String,
    city             String,
    state            String,
    customer_segment String,
    active           UInt8
) ENGINE = MergeTree ORDER BY customer_id;

CREATE TABLE IF NOT EXISTS gold.dim_products
(
    product_id     Int64,
    sku            String,
    product_name   String,
    category_name  String,
    supplier_name  String,
    unit_cost      Decimal(12,2),
    unit_price     Decimal(12,2),
    reorder_level  Int32,
    active         UInt8
) ENGINE = MergeTree ORDER BY product_id;

CREATE TABLE IF NOT EXISTS gold.dim_stores
(
    store_id    Int64,
    store_code  String,
    store_name  String,
    city        String,
    state       String,
    opened_on   Date,
    active      UInt8
) ENGINE = MergeTree ORDER BY store_id;

CREATE TABLE IF NOT EXISTS gold.dim_warehouses
(
    warehouse_id    Int64,
    warehouse_code  String,
    warehouse_name  String,
    city            String,
    state           String,
    capacity_units  Int32,
    active          UInt8
) ENGINE = MergeTree ORDER BY warehouse_id;

CREATE TABLE IF NOT EXISTS gold.dim_employees
(
    employee_id    Int64,
    employee_code  String,
    employee_name  String,
    role           String,
    store_id       Nullable(Int64),
    hired_on       Date,
    active         UInt8
) ENGINE = MergeTree ORDER BY employee_id;

CREATE TABLE IF NOT EXISTS gold.dim_date
(
    date_key    Date,
    year        UInt16,
    quarter     UInt8,
    month       UInt8,
    month_name  String,
    week        UInt8,
    day_of_week UInt8,
    day_name    String,
    is_weekend  UInt8
) ENGINE = MergeTree ORDER BY date_key;

-- ---------- Facts ----------

CREATE TABLE IF NOT EXISTS gold.fact_sales
(
    order_date        Date,
    order_id          Int64,
    order_item_id     Int64,
    customer_id       Int64,
    store_id          Int64,
    product_id        Int64,
    order_status      String,
    payment_status    String,
    quantity          Int32,
    unit_price        Decimal(12,2),
    discount_amount   Decimal(12,2),
    line_amount       Decimal(14,2),
    order_ts          DateTime64(6,'UTC'),
    dq_late_arrival   UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(order_date)
ORDER BY (order_date, order_id, order_item_id);

CREATE TABLE IF NOT EXISTS gold.fact_payments
(
    payment_date      Date,
    payment_id        Int64,
    order_id          Int64,
    payment_method    String,
    amount            Decimal(14,2),
    status            String,
    payment_ts        DateTime64(6,'UTC')
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(payment_date)
ORDER BY (payment_date, payment_id);

CREATE TABLE IF NOT EXISTS gold.fact_inventory_movements
(
    movement_date    Date,
    movement_id      Int64,
    product_id       Int64,
    warehouse_id     Int64,
    movement_type    String,
    quantity         Int32,
    reference_type   Nullable(String),
    movement_ts      DateTime64(6,'UTC'),
    dq_anomaly       UInt8,
    dq_seed_noise    UInt8
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(movement_date)
ORDER BY (movement_date, movement_id);

CREATE TABLE IF NOT EXISTS gold.fact_inventory_daily_balance
(
    balance_date     Date,
    product_id       Int64,
    warehouse_id     Int64,
    daily_net_qty    Int64,
    running_balance  Int64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(balance_date)
ORDER BY (balance_date, product_id, warehouse_id);