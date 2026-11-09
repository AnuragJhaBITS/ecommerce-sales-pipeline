-- ═══════════════════════════════════════════════════════════════
-- Star Schema DDL for Amazon Redshift
-- Database: dev  |  Schema: ecommerce
--
-- Fact table:  fact_orders (one row per delivered order)
-- Dimensions:  dim_date, dim_customer, dim_product, dim_seller
--
-- Distribution & sort keys chosen for typical analytical
-- query patterns (time-series, category roll-ups, joins).
-- ═══════════════════════════════════════════════════════════════

CREATE SCHEMA IF NOT EXISTS ecommerce;
SET search_path TO ecommerce;


-- ─────────────────────── dim_date ────────────────────────────
-- Pre-populated calendar dimension (2016-01-01 to 2019-12-31)
-- Covers the full range of the Brazilian e-commerce dataset

CREATE TABLE IF NOT EXISTS dim_date (
    date_key        DATE        PRIMARY KEY,
    day_of_week     SMALLINT    NOT NULL,   -- 0=Mon .. 6=Sun
    day_name        VARCHAR(10) NOT NULL,
    day_of_month    SMALLINT    NOT NULL,
    week_of_year    SMALLINT    NOT NULL,
    month           SMALLINT    NOT NULL,
    month_name      VARCHAR(10) NOT NULL,
    quarter         SMALLINT    NOT NULL,
    year            SMALLINT    NOT NULL,
    is_weekend      BOOLEAN     NOT NULL
)
DISTSTYLE ALL
SORTKEY (date_key);


-- ─────────────────────── dim_customer ────────────────────────

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key        VARCHAR(64)  PRIMARY KEY,  -- customer_unique_id
    customer_city       VARCHAR(100),
    customer_state      VARCHAR(2),
    total_orders        INT,
    lifetime_value      DECIMAL(12,2),
    avg_review_score    DECIMAL(3,2),
    first_order_date    TIMESTAMP,
    last_order_date     TIMESTAMP,
    customer_tenure_days INT
)
DISTSTYLE KEY
DISTKEY (customer_key)
SORTKEY (customer_state, customer_city);


-- ─────────────────────── dim_product ─────────────────────────

CREATE TABLE IF NOT EXISTS dim_product (
    product_id              VARCHAR(64) PRIMARY KEY,
    product_category_name   VARCHAR(100),
    product_weight_g        DECIMAL(10,2),
    product_length_cm       DECIMAL(10,2),
    product_height_cm       DECIMAL(10,2),
    product_width_cm        DECIMAL(10,2),
    product_photos_qty      SMALLINT
)
DISTSTYLE ALL
SORTKEY (product_category_name);


-- ─────────────────────── dim_seller ──────────────────────────

CREATE TABLE IF NOT EXISTS dim_seller (
    seller_id               VARCHAR(64)  PRIMARY KEY,
    seller_city             VARCHAR(100),
    seller_state            VARCHAR(2),
    total_orders            INT,
    total_revenue           DECIMAL(14,2),
    avg_delivery_delay      DECIMAL(6,2),
    avg_review_score        DECIMAL(3,2),
    late_deliveries         INT,
    late_delivery_rate      DECIMAL(5,4)
)
DISTSTYLE ALL
SORTKEY (seller_state, seller_city);


-- ─────────────────────── fact_orders ─────────────────────────
-- Grain: one row per delivered order

CREATE TABLE IF NOT EXISTS fact_orders (
    order_id                VARCHAR(64)   PRIMARY KEY,
    customer_id             VARCHAR(64)   REFERENCES dim_customer(customer_key),
    primary_product_id      VARCHAR(64)   REFERENCES dim_product(product_id),
    primary_seller_id       VARCHAR(64)   REFERENCES dim_seller(seller_id),
    order_date              DATE          REFERENCES dim_date(date_key),

    -- Measures
    total_item_price        DECIMAL(12,2),
    total_freight           DECIMAL(12,2),
    order_value             DECIMAL(12,2),
    total_payment_value     DECIMAL(12,2),
    distinct_products       SMALLINT,
    total_items             SMALLINT,

    -- Delivery metrics
    delivery_delay_days     INT,
    is_late_delivery        BOOLEAN,
    processing_time_days    INT,
    shipping_time_days      INT,

    -- Payment info
    primary_payment_type    VARCHAR(30),
    max_installments        SMALLINT,

    -- Review
    review_score            SMALLINT,

    -- Partition keys (denormalized for fast filtering)
    order_year              SMALLINT,
    order_month             SMALLINT
)
DISTSTYLE KEY
DISTKEY (customer_id)
SORTKEY (order_date);


-- ═══════════════════════════════════════════════════════════════
-- Populate dim_date with a generate_series (Redshift syntax)
-- ═══════════════════════════════════════════════════════════════

INSERT INTO dim_date
SELECT
    d::DATE                                          AS date_key,
    EXTRACT(DOW FROM d)::SMALLINT                    AS day_of_week,
    TO_CHAR(d, 'Day')                                AS day_name,
    EXTRACT(DAY FROM d)::SMALLINT                    AS day_of_month,
    EXTRACT(WEEK FROM d)::SMALLINT                   AS week_of_year,
    EXTRACT(MONTH FROM d)::SMALLINT                  AS month,
    TO_CHAR(d, 'Month')                              AS month_name,
    EXTRACT(QUARTER FROM d)::SMALLINT                AS quarter,
    EXTRACT(YEAR FROM d)::SMALLINT                   AS year,
    CASE WHEN EXTRACT(DOW FROM d) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM (
    SELECT ('2016-01-01'::DATE + n)::DATE AS d
    FROM (SELECT ROW_NUMBER() OVER () - 1 AS n FROM stl_scan LIMIT 1461) seq
) calendar
WHERE d <= '2019-12-31';
