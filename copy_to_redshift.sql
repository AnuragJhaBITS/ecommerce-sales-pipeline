-- ═══════════════════════════════════════════════════════════════
-- COPY commands — load processed Parquet from S3 into Redshift
--
-- Replace:
--   <BUCKET>   → your S3 bucket name
--   <IAM_ROLE> → your Redshift IAM role ARN with S3 read access
-- ═══════════════════════════════════════════════════════════════

SET search_path TO ecommerce;


-- ─── Dimension tables first (fact table has FK references) ───

COPY dim_customer
FROM 's3://<BUCKET>/processed/dim_customer_ltv/'
IAM_ROLE '<IAM_ROLE>'
FORMAT AS PARQUET;

COPY dim_product
FROM 's3://<BUCKET>/processed/dim_products/'
IAM_ROLE '<IAM_ROLE>'
FORMAT AS PARQUET;

COPY dim_seller
FROM 's3://<BUCKET>/processed/dim_seller_performance/'
IAM_ROLE '<IAM_ROLE>'
FORMAT AS PARQUET;


-- ─── Fact table ──────────────────────────────────────────────

COPY fact_orders
FROM 's3://<BUCKET>/processed/fact_orders/'
IAM_ROLE '<IAM_ROLE>'
FORMAT AS PARQUET;


-- ─── Verify row counts ──────────────────────────────────────

SELECT 'dim_date'     AS tbl, COUNT(*) AS rows FROM dim_date
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_product',  COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_seller',   COUNT(*) FROM dim_seller
UNION ALL
SELECT 'fact_orders',  COUNT(*) FROM fact_orders
ORDER BY tbl;
