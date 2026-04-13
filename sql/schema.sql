-- sql/schema.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Creates all tables required by the Sales ETL pipeline.
-- Designed to be idempotent (safe to run multiple times).
-- ──────────────────────────────────────────────────────────────────────────────

-- ── Extensions ────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";


-- ── Dimension: Customers ──────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id     VARCHAR(20)  NOT NULL,
    customer_name   VARCHAR(120),
    PRIMARY KEY (customer_id)
);


-- ── Dimension: Products / Categories ─────────────────────────────────────────
CREATE TABLE IF NOT EXISTS dim_products (
    product_key     SERIAL       PRIMARY KEY,
    category        VARCHAR(60)  NOT NULL,
    sub_category    VARCHAR(60)  NOT NULL,
    UNIQUE (category, sub_category)
);


-- ── Fact: Sales ───────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS sales_fact (
    -- Surrogate key (auto-generated on insert)
    id               SERIAL        PRIMARY KEY,

    -- Natural / business keys
    order_id         VARCHAR(20)   NOT NULL,
    customer_id      VARCHAR(20),
    customer_name    VARCHAR(120),

    -- Product / category
    category         VARCHAR(60),
    sub_category     VARCHAR(60),

    -- Pricing
    unit_price       NUMERIC(12,2) NOT NULL,
    quantity         INTEGER       NOT NULL DEFAULT 1,
    discount_pct     NUMERIC(5,2)  NOT NULL DEFAULT 0,
    discount_amount  NUMERIC(12,2),
    line_total       NUMERIC(12,2),

    -- Transaction metadata
    payment_method   VARCHAR(30),
    order_date       DATE,
    order_year       SMALLINT,
    order_month      SMALLINT,
    order_quarter    SMALLINT,
    order_year_month VARCHAR(7),   -- 'YYYY-MM'

    -- Fulfillment
    shipping_status  VARCHAR(20),
    is_returned      BOOLEAN       DEFAULT FALSE,

    -- Geography
    region           VARCHAR(20),
    state            VARCHAR(50),

    -- Audit
    loaded_at        TIMESTAMP     DEFAULT NOW()
);


-- ── Indexes (improve query performance for common analytical patterns) ─────────
CREATE INDEX IF NOT EXISTS idx_sales_order_date
    ON sales_fact (order_date);

CREATE INDEX IF NOT EXISTS idx_sales_customer_id
    ON sales_fact (customer_id);

CREATE INDEX IF NOT EXISTS idx_sales_category
    ON sales_fact (category);

CREATE INDEX IF NOT EXISTS idx_sales_year_month
    ON sales_fact (order_year_month);

CREATE INDEX IF NOT EXISTS idx_sales_region
    ON sales_fact (region);

CREATE INDEX IF NOT EXISTS idx_sales_shipping_status
    ON sales_fact (shipping_status);


-- ── Materialised summary table (optional; refreshed by queries.sql) ───────────
CREATE TABLE IF NOT EXISTS monthly_revenue_summary (
    order_year_month VARCHAR(7)    PRIMARY KEY,
    total_orders     INTEGER,
    total_revenue    NUMERIC(14,2),
    avg_order_value  NUMERIC(10,2),
    total_returns    INTEGER,
    return_rate_pct  NUMERIC(5,2),
    last_updated     TIMESTAMP     DEFAULT NOW()
);
