-- sql/queries.sql
-- ──────────────────────────────────────────────────────────────────────────────
-- Analytical queries for the Sales ETL pipeline.
-- Run against the sales_fact table after the pipeline has loaded data.
-- ──────────────────────────────────────────────────────────────────────────────


-- ════════════════════════════════════════════════════════════════════════════
-- 1. REVENUE OVERVIEW
-- ════════════════════════════════════════════════════════════════════════════

-- 1a. Total revenue, orders, and average order value (all time)
SELECT
    COUNT(DISTINCT order_id)              AS total_orders,
    SUM(line_total)                       AS total_revenue,
    ROUND(AVG(line_total), 2)             AS avg_order_value,
    SUM(discount_amount)                  AS total_discounts_given,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS total_returns
FROM sales_fact;


-- 1b. Total revenue by category (sorted highest first)
SELECT
    category,
    COUNT(DISTINCT order_id)              AS total_orders,
    SUM(quantity)                         AS units_sold,
    ROUND(SUM(line_total), 2)             AS total_revenue,
    ROUND(AVG(unit_price), 2)             AS avg_unit_price,
    ROUND(AVG(discount_pct), 2)           AS avg_discount_pct,
    ROUND(SUM(line_total) * 100.0
          / SUM(SUM(line_total)) OVER (), 2) AS revenue_share_pct
FROM sales_fact
GROUP BY category
ORDER BY total_revenue DESC;


-- 1c. Revenue by sub-category within each category
SELECT
    category,
    sub_category,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS total_revenue,
    RANK() OVER (
        PARTITION BY category
        ORDER BY SUM(line_total) DESC
    )                                      AS rank_within_category
FROM sales_fact
GROUP BY category, sub_category
ORDER BY category, rank_within_category;


-- ════════════════════════════════════════════════════════════════════════════
-- 2. MONTHLY & QUARTERLY TRENDS
-- ════════════════════════════════════════════════════════════════════════════

-- 2a. Monthly revenue trend
SELECT
    order_year_month,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS monthly_revenue,
    ROUND(AVG(line_total), 2)             AS avg_order_value,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM sales_fact
GROUP BY order_year_month
ORDER BY order_year_month;


-- 2b. Month-over-month revenue growth (%)
WITH monthly AS (
    SELECT
        order_year_month,
        SUM(line_total) AS revenue
    FROM sales_fact
    GROUP BY order_year_month
)
SELECT
    order_year_month,
    ROUND(revenue, 2)                     AS revenue,
    LAG(revenue) OVER (ORDER BY order_year_month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY order_year_month))
        * 100.0
        / NULLIF(LAG(revenue) OVER (ORDER BY order_year_month), 0),
    2)                                    AS mom_growth_pct
FROM monthly
ORDER BY order_year_month;


-- 2c. Quarterly revenue by year
SELECT
    order_year,
    order_quarter,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS quarterly_revenue
FROM sales_fact
GROUP BY order_year, order_quarter
ORDER BY order_year, order_quarter;


-- ════════════════════════════════════════════════════════════════════════════
-- 3. CUSTOMER ANALYSIS
-- ════════════════════════════════════════════════════════════════════════════

-- 3a. Top 20 customers by lifetime value
SELECT
    customer_id,
    customer_name,
    COUNT(DISTINCT order_id)              AS total_orders,
    SUM(quantity)                         AS total_units,
    ROUND(SUM(line_total), 2)             AS lifetime_value,
    ROUND(AVG(line_total), 2)             AS avg_order_value,
    MIN(order_date)                       AS first_order_date,
    MAX(order_date)                       AS last_order_date
FROM sales_fact
WHERE customer_id IS NOT NULL
GROUP BY customer_id, customer_name
ORDER BY lifetime_value DESC
LIMIT 20;


-- 3b. Customer purchase frequency distribution
WITH cust_orders AS (
    SELECT customer_id, COUNT(DISTINCT order_id) AS order_count
    FROM sales_fact
    WHERE customer_id IS NOT NULL
    GROUP BY customer_id
)
SELECT
    order_count                           AS orders_placed,
    COUNT(customer_id)                    AS number_of_customers,
    ROUND(COUNT(customer_id) * 100.0
          / SUM(COUNT(customer_id)) OVER (), 2) AS pct_of_customers
FROM cust_orders
GROUP BY order_count
ORDER BY order_count;


-- ════════════════════════════════════════════════════════════════════════════
-- 4. GEOGRAPHIC ANALYSIS
-- ════════════════════════════════════════════════════════════════════════════

-- 4a. Revenue by region
SELECT
    region,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS total_revenue,
    ROUND(AVG(line_total), 2)             AS avg_order_value
FROM sales_fact
GROUP BY region
ORDER BY total_revenue DESC;


-- 4b. Top 10 states by revenue
SELECT
    state,
    region,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS total_revenue
FROM sales_fact
GROUP BY state, region
ORDER BY total_revenue DESC
LIMIT 10;


-- ════════════════════════════════════════════════════════════════════════════
-- 5. PAYMENT & SHIPPING ANALYSIS
-- ════════════════════════════════════════════════════════════════════════════

-- 5a. Revenue by payment method
SELECT
    payment_method,
    COUNT(DISTINCT order_id)              AS total_orders,
    ROUND(SUM(line_total), 2)             AS total_revenue,
    ROUND(AVG(line_total), 2)             AS avg_order_value
FROM sales_fact
GROUP BY payment_method
ORDER BY total_revenue DESC;


-- 5b. Shipping status breakdown with return rate
SELECT
    shipping_status,
    COUNT(*)                              AS total_orders,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS pct_of_orders
FROM sales_fact
GROUP BY shipping_status
ORDER BY total_orders DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 6. DISCOUNT ANALYSIS
-- ════════════════════════════════════════════════════════════════════════════

-- 6a. Revenue impact of discounts by category
SELECT
    category,
    ROUND(SUM(unit_price * quantity), 2)  AS gross_revenue,
    ROUND(SUM(discount_amount), 2)        AS total_discounts,
    ROUND(SUM(line_total), 2)             AS net_revenue,
    ROUND(SUM(discount_amount) * 100.0
          / NULLIF(SUM(unit_price * quantity), 0), 2) AS effective_discount_rate_pct
FROM sales_fact
GROUP BY category
ORDER BY total_discounts DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- 7. REFRESH MONTHLY SUMMARY TABLE
-- ════════════════════════════════════════════════════════════════════════════

INSERT INTO monthly_revenue_summary
    (order_year_month, total_orders, total_revenue,
     avg_order_value, total_returns, return_rate_pct, last_updated)
SELECT
    order_year_month,
    COUNT(DISTINCT order_id),
    ROUND(SUM(line_total), 2),
    ROUND(AVG(line_total), 2),
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END),
    ROUND(SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) * 100.0
          / NULLIF(COUNT(*), 0), 2),
    NOW()
FROM sales_fact
GROUP BY order_year_month
ON CONFLICT (order_year_month) DO UPDATE SET
    total_orders     = EXCLUDED.total_orders,
    total_revenue    = EXCLUDED.total_revenue,
    avg_order_value  = EXCLUDED.avg_order_value,
    total_returns    = EXCLUDED.total_returns,
    return_rate_pct  = EXCLUDED.return_rate_pct,
    last_updated     = NOW();
