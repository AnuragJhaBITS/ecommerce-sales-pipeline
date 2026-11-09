"""
transform.py — PySpark ETL job for the e-commerce pipeline

Reads raw CSVs from S3 (or local), cleans and joins them,
engineers derived metrics, and writes analytics-ready Parquet
partitioned by year/month to the processed layer.

Run locally:
    spark-submit src/etl/transform.py --input ./data/ --output ./output/

Run on AWS Glue / EMR:
    spark-submit src/etl/transform.py \
        --input s3://your-bucket/raw/ \
        --output s3://your-bucket/processed/
"""

import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, FloatType,
    IntegerType, TimestampType,
)


# ─────────────────────────────── Schemas ──────────────────────
# Explicit schemas prevent type-inference surprises on messy CSVs

ORDERS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("order_status", StringType()),
    StructField("order_purchase_timestamp", TimestampType()),
    StructField("order_approved_at", TimestampType()),
    StructField("order_delivered_carrier_date", TimestampType()),
    StructField("order_delivered_customer_date", TimestampType()),
    StructField("order_estimated_delivery_date", TimestampType()),
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_item_id", IntegerType()),
    StructField("product_id", StringType()),
    StructField("seller_id", StringType()),
    StructField("shipping_limit_date", TimestampType()),
    StructField("price", FloatType()),
    StructField("freight_value", FloatType()),
])

PAYMENTS_SCHEMA = StructType([
    StructField("order_id", StringType()),
    StructField("payment_sequential", IntegerType()),
    StructField("payment_type", StringType()),
    StructField("payment_installments", IntegerType()),
    StructField("payment_value", FloatType()),
])

PRODUCTS_SCHEMA = StructType([
    StructField("product_id", StringType()),
    StructField("product_category_name", StringType()),
    StructField("product_name_lenght", IntegerType()),
    StructField("product_description_lenght", IntegerType()),
    StructField("product_photos_qty", IntegerType()),
    StructField("product_weight_g", FloatType()),
    StructField("product_length_cm", FloatType()),
    StructField("product_height_cm", FloatType()),
    StructField("product_width_cm", FloatType()),
])

CUSTOMERS_SCHEMA = StructType([
    StructField("customer_id", StringType()),
    StructField("customer_unique_id", StringType()),
    StructField("customer_zip_code_prefix", StringType()),
    StructField("customer_city", StringType()),
    StructField("customer_state", StringType()),
])

SELLERS_SCHEMA = StructType([
    StructField("seller_id", StringType()),
    StructField("seller_zip_code_prefix", StringType()),
    StructField("seller_city", StringType()),
    StructField("seller_state", StringType()),
])

REVIEWS_SCHEMA = StructType([
    StructField("review_id", StringType()),
    StructField("order_id", StringType()),
    StructField("review_score", IntegerType()),
    StructField("review_comment_title", StringType()),
    StructField("review_comment_message", StringType()),
    StructField("review_creation_date", TimestampType()),
    StructField("review_answer_timestamp", TimestampType()),
])


# ─────────────────────────── Read helpers ─────────────────────

def read_csv(spark: SparkSession, base_path: str, entity: str, filename: str, schema: StructType) -> DataFrame:
    """Read a single entity CSV with explicit schema."""
    path = f"{base_path.rstrip('/')}/{entity}/{filename}"
    return (
        spark.read
        .option("header", "true")
        .option("quote", '"')
        .option("escape", '"')
        .schema(schema)
        .csv(path)
    )


# ────────────────────────── Transformations ───────────────────

def clean_orders(orders: DataFrame) -> DataFrame:
    """Keep only delivered orders, drop nulls on critical columns."""
    return (
        orders
        .filter(F.col("order_status") == "delivered")
        .filter(F.col("order_purchase_timestamp").isNotNull())
        .filter(F.col("order_delivered_customer_date").isNotNull())
        .dropDuplicates(["order_id"])
    )


def build_order_metrics(orders: DataFrame, items: DataFrame, payments: DataFrame, reviews: DataFrame) -> DataFrame:
    """
    Join orders with items, payments, and reviews to produce
    one row per order with aggregated metrics.
    """
    # Aggregate item-level data to order level
    items_agg = (
        items
        .groupBy("order_id")
        .agg(
            F.sum("price").alias("total_item_price"),
            F.sum("freight_value").alias("total_freight"),
            F.countDistinct("product_id").alias("distinct_products"),
            F.count("order_item_id").alias("total_items"),
            F.first("product_id").alias("primary_product_id"),
            F.first("seller_id").alias("primary_seller_id"),
        )
    )

    # Aggregate payment data to order level
    payments_agg = (
        payments
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.max("payment_installments").alias("max_installments"),
            F.first("payment_type").alias("primary_payment_type"),
        )
    )

    # Take the latest review per order
    reviews_dedup = (
        reviews
        .dropDuplicates(["order_id"])
        .select("order_id", "review_score")
    )

    # Join everything together
    enriched = (
        orders
        .join(items_agg, "order_id", "inner")
        .join(payments_agg, "order_id", "left")
        .join(reviews_dedup, "order_id", "left")
    )

    # Engineer derived metrics
    enriched = (
        enriched
        .withColumn("order_value", F.col("total_item_price") + F.col("total_freight"))
        .withColumn(
            "delivery_delay_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_estimated_delivery_date"),
            ),
        )
        .withColumn("is_late_delivery", F.when(F.col("delivery_delay_days") > 0, True).otherwise(False))
        .withColumn(
            "processing_time_days",
            F.datediff(F.col("order_approved_at"), F.col("order_purchase_timestamp")),
        )
        .withColumn(
            "shipping_time_days",
            F.datediff(
                F.col("order_delivered_customer_date"),
                F.col("order_delivered_carrier_date"),
            ),
        )
        .withColumn("order_year", F.year("order_purchase_timestamp"))
        .withColumn("order_month", F.month("order_purchase_timestamp"))
    )

    return enriched


def build_customer_lifetime(enriched: DataFrame, customers: DataFrame) -> DataFrame:
    """Compute customer-level lifetime metrics."""
    clv = (
        enriched
        .join(customers, "customer_id", "inner")
        .groupBy("customer_unique_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_value").alias("lifetime_value"),
            F.avg("review_score").alias("avg_review_score"),
            F.min("order_purchase_timestamp").alias("first_order_date"),
            F.max("order_purchase_timestamp").alias("last_order_date"),
            F.first("customer_city").alias("customer_city"),
            F.first("customer_state").alias("customer_state"),
        )
        .withColumn(
            "customer_tenure_days",
            F.datediff(F.col("last_order_date"), F.col("first_order_date")),
        )
    )
    return clv


def build_seller_performance(enriched: DataFrame, sellers: DataFrame) -> DataFrame:
    """Compute seller-level performance metrics."""
    perf = (
        enriched
        .join(
            sellers,
            enriched["primary_seller_id"] == sellers["seller_id"],
            "inner",
        )
        .groupBy("seller_id")
        .agg(
            F.count("order_id").alias("total_orders"),
            F.sum("order_value").alias("total_revenue"),
            F.avg("delivery_delay_days").alias("avg_delivery_delay"),
            F.avg("review_score").alias("avg_review_score"),
            F.sum(F.when(F.col("is_late_delivery"), 1).otherwise(0)).alias("late_deliveries"),
            F.first("seller_city").alias("seller_city"),
            F.first("seller_state").alias("seller_state"),
        )
        .withColumn(
            "late_delivery_rate",
            F.round(F.col("late_deliveries") / F.col("total_orders"), 4),
        )
    )
    return perf


# ───────────────────────────── Main ETL ───────────────────────

def run(input_path: str, output_path: str) -> None:
    spark = (
        SparkSession.builder
        .appName("EcommerceETL")
        .config("spark.sql.parquet.compression.codec", "snappy")
        .getOrCreate()
    )

    print("=" * 60)
    print("  E-Commerce ETL Pipeline — starting")
    print("=" * 60)

    # ── Read raw data ──
    print("\n[1/5] Reading raw CSVs...")
    orders    = read_csv(spark, input_path, "orders",     "olist_orders_dataset.csv",          ORDERS_SCHEMA)
    items     = read_csv(spark, input_path, "order_items","olist_order_items_dataset.csv",      ORDER_ITEMS_SCHEMA)
    payments  = read_csv(spark, input_path, "payments",   "olist_order_payments_dataset.csv",   PAYMENTS_SCHEMA)
    products  = read_csv(spark, input_path, "products",   "olist_products_dataset.csv",         PRODUCTS_SCHEMA)
    customers = read_csv(spark, input_path, "customers",  "olist_customers_dataset.csv",        CUSTOMERS_SCHEMA)
    sellers   = read_csv(spark, input_path, "sellers",    "olist_sellers_dataset.csv",           SELLERS_SCHEMA)
    reviews   = read_csv(spark, input_path, "reviews",    "olist_order_reviews_dataset.csv",     REVIEWS_SCHEMA)

    print(f"    orders:    {orders.count():,} rows")
    print(f"    items:     {items.count():,} rows")
    print(f"    payments:  {payments.count():,} rows")
    print(f"    customers: {customers.count():,} rows")
    print(f"    products:  {products.count():,} rows")
    print(f"    sellers:   {sellers.count():,} rows")
    print(f"    reviews:   {reviews.count():,} rows")

    # ── Clean ──
    print("\n[2/5] Cleaning orders (delivered only, dedup)...")
    orders_clean = clean_orders(orders)
    print(f"    {orders_clean.count():,} delivered orders retained")

    # ── Transform: order-level enrichment ──
    print("\n[3/5] Building enriched order metrics...")
    enriched = build_order_metrics(orders_clean, items, payments, reviews)

    # ── Transform: customer lifetime value ──
    print("\n[4/5] Computing customer lifetime value...")
    customer_ltv = build_customer_lifetime(enriched, customers)

    # ── Transform: seller performance ──
    print("        Computing seller performance...")
    seller_perf = build_seller_performance(enriched, sellers)

    # ── Write to processed layer ──
    print(f"\n[5/5] Writing Parquet to {output_path}...")

    out = output_path.rstrip("/")

    enriched.write.mode("overwrite") \
        .partitionBy("order_year", "order_month") \
        .parquet(f"{out}/fact_orders/")

    customer_ltv.write.mode("overwrite").parquet(f"{out}/dim_customer_ltv/")
    seller_perf.write.mode("overwrite").parquet(f"{out}/dim_seller_performance/")

    # Also write dimension tables as-is for Redshift COPY
    products.write.mode("overwrite").parquet(f"{out}/dim_products/")
    customers.dropDuplicates(["customer_unique_id"]).write.mode("overwrite").parquet(f"{out}/dim_customers/")
    sellers.write.mode("overwrite").parquet(f"{out}/dim_sellers/")

    print("\n" + "=" * 60)
    print("  ETL complete")
    print(f"  Output: {out}/")
    print("=" * 60)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="E-Commerce PySpark ETL")
    parser.add_argument("--input", required=True, help="Path to raw/ (local or s3://)")
    parser.add_argument("--output", required=True, help="Path to processed/ (local or s3://)")
    args = parser.parse_args()
    run(args.input, args.output)
