-- ═══════════════════════════════════════════════════════════════
-- Analytical queries on the star schema
-- These demonstrate the kinds of business insights the pipeline
-- enables — run them after COPY is complete.
-- ═══════════════════════════════════════════════════════════════

SET search_path TO ecommerce;


-- ─────────────────────────────────────────────────────────────
-- Q1: Revenue by product category by quarter
--     Classic slice-and-dice across two dimensions
-- ─────────────────────────────────────────────────────────────

SELECT
    p.product_category_name       AS category,
    d.year,
    d.quarter,
    COUNT(f.order_id)             AS orders,
    ROUND(SUM(f.order_value), 2)  AS total_revenue,
    ROUND(AVG(f.order_value), 2)  AS avg_order_value
FROM fact_orders f
JOIN dim_product p ON f.primary_product_id = p.product_id
JOIN dim_date    d ON f.order_date = d.date_key
GROUP BY p.product_category_name, d.year, d.quarter
ORDER BY d.year, d.quarter, total_revenue DESC
LIMIT 30;


-- ─────────────────────────────────────────────────────────────
-- Q2: Average delivery delay by seller state
--     Operational insight — which regions have logistics issues?
-- ─────────────────────────────────────────────────────────────

SELECT
    s.seller_state,
    COUNT(f.order_id)                          AS total_orders,
    ROUND(AVG(f.delivery_delay_days), 2)       AS avg_delay_days,
    ROUND(AVG(f.shipping_time_days), 2)        AS avg_shipping_days,
    ROUND(
        SUM(CASE WHEN f.is_late_delivery THEN 1 ELSE 0 END)::DECIMAL
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                          AS late_delivery_pct
FROM fact_orders f
JOIN dim_seller s ON f.primary_seller_id = s.seller_id
GROUP BY s.seller_state
HAVING COUNT(f.order_id) >= 50
ORDER BY avg_delay_days DESC;


-- ─────────────────────────────────────────────────────────────
-- Q3: Customer cohort retention by first-order month
--     How many customers from each cohort ordered again?
-- ─────────────────────────────────────────────────────────────

WITH cohorts AS (
    SELECT
        c.customer_key,
        DATE_TRUNC('month', c.first_order_date)::DATE AS cohort_month,
        c.total_orders
    FROM dim_customer c
    WHERE c.first_order_date IS NOT NULL
)
SELECT
    cohort_month,
    COUNT(*)                                        AS cohort_size,
    SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END) AS repeat_customers,
    ROUND(
        SUM(CASE WHEN total_orders > 1 THEN 1 ELSE 0 END)::DECIMAL
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                               AS repeat_rate_pct
FROM cohorts
GROUP BY cohort_month
ORDER BY cohort_month;


-- ─────────────────────────────────────────────────────────────
-- Q4: Top 10 sellers by revenue with review quality
--     Who drives the most value and are customers happy?
-- ─────────────────────────────────────────────────────────────

SELECT
    s.seller_id,
    s.seller_city,
    s.seller_state,
    s.total_orders,
    ROUND(s.total_revenue, 2)       AS total_revenue,
    ROUND(s.avg_review_score, 2)    AS avg_review,
    s.late_deliveries,
    ROUND(s.late_delivery_rate * 100, 2) AS late_pct
FROM dim_seller s
ORDER BY s.total_revenue DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────
-- Q5: Weekend vs weekday order patterns
--     Does purchasing behaviour shift on weekends?
-- ─────────────────────────────────────────────────────────────

SELECT
    CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END AS day_type,
    COUNT(f.order_id)                   AS total_orders,
    ROUND(AVG(f.order_value), 2)        AS avg_order_value,
    ROUND(AVG(f.review_score), 2)       AS avg_review,
    ROUND(AVG(f.total_items), 2)        AS avg_items_per_order,
    ROUND(
        SUM(CASE WHEN f.primary_payment_type = 'credit_card' THEN 1 ELSE 0 END)::DECIMAL
        / NULLIF(COUNT(*), 0) * 100, 2
    )                                   AS credit_card_pct
FROM fact_orders f
JOIN dim_date d ON f.order_date = d.date_key
GROUP BY day_type;


-- ─────────────────────────────────────────────────────────────
-- Q6: Monthly revenue trend with MoM growth
--     Time-series view for dashboarding
-- ─────────────────────────────────────────────────────────────

WITH monthly AS (
    SELECT
        d.year,
        d.month,
        ROUND(SUM(f.order_value), 2) AS revenue
    FROM fact_orders f
    JOIN dim_date d ON f.order_date = d.date_key
    GROUP BY d.year, d.month
)
SELECT
    year,
    month,
    revenue,
    LAG(revenue) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        (revenue - LAG(revenue) OVER (ORDER BY year, month))
        / NULLIF(LAG(revenue) OVER (ORDER BY year, month), 0) * 100, 2
    ) AS mom_growth_pct
FROM monthly
ORDER BY year, month;
