# E-Commerce Sales Analytics Pipeline

An end-to-end batch data pipeline that ingests 100K+ e-commerce transactions into an S3 data lake, transforms them with PySpark, and loads a star-schema warehouse in Amazon Redshift for business analytics.

## Architecture

```
┌────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌──────────────────┐     ┌────────────┐
│   Kaggle   │────▶│   S3 Data Lake   │────▶│   AWS Glue       │────▶│   S3 Processed   │────▶│  Redshift  │
│  (8 CSVs)  │     │   raw/ layer     │     │  Crawler + ETL   │     │  Parquet (Y/M)   │     │ Star Schema│
└────────────┘     └──────────────────┘     └──────────────────┘     └──────────────────┘     └────────────┘
                                                    │
                                              PySpark ETL
                                            ┌───────┴───────┐
                                            │  • Clean       │
                                            │  • Join 7 tbls │
                                            │  • Engineer 5+ │
                                            │    metrics     │
                                            │  • Partition   │
                                            └────────────────┘
```

## Star Schema Design

```
                    ┌──────────────┐
                    │   dim_date   │
                    ├──────────────┤
                    │ date_key (PK)│
                    │ day_of_week  │
                    │ month        │
                    │ quarter      │
                    │ year         │
                    │ is_weekend   │
                    └──────┬───────┘
                           │
┌──────────────┐   ┌──────┴───────────┐   ┌───────────────┐
│ dim_customer │   │   fact_orders    │   │  dim_product   │
├──────────────┤   ├──────────────────┤   ├───────────────┤
│ customer_key │◀──│ customer_id (FK) │   │ product_id(PK)│
│ city, state  │   │ product_id  (FK) │──▶│ category      │
│ lifetime_val │   │ seller_id   (FK) │   │ weight, dims  │
│ total_orders │   │ order_date  (FK) │   └───────────────┘
│ avg_review   │   │──────────────────│
│ tenure_days  │   │ order_value      │   ┌───────────────┐
└──────────────┘   │ total_freight    │   │  dim_seller   │
                   │ delivery_delay   │   ├───────────────┤
                   │ is_late_delivery │   │ seller_id (PK)│
                   │ review_score     │──▶│ city, state   │
                   │ payment_type     │   │ total_revenue │
                   │ shipping_time    │   │ avg_review    │
                   └──────────────────┘   │ late_del_rate │
                                          └───────────────┘
```

## Project Structure

```
ecommerce-sales-pipeline/
├── README.md
├── requirements.txt
├── src/
│   ├── config.py                    # AWS resource names (edit this first)
│   ├── setup/
│   │   ├── upload_to_s3.py          # Upload raw CSVs to S3
│   │   └── glue_setup.py           # Create Glue database + crawler
│   └── etl/
│       └── transform.py             # PySpark ETL job
└── sql/
    ├── ddl/
    │   └── create_star_schema.sql   # Redshift table definitions
    ├── load/
    │   └── copy_to_redshift.sql     # COPY from S3 into Redshift
    └── analytics/
        └── analytical_queries.sql   # 6 business analytics queries
```

## Quick Start

### Prerequisites
- Python 3.9+, Java 8+ (for Spark)
- AWS account with Free Tier (Redshift Serverless, S3, Glue)
- Dataset: [Brazilian E-Commerce (Kaggle)](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce)

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure AWS
Edit `src/config.py` with your bucket name, Redshift endpoint, and IAM roles.

### 3. Upload raw data to S3
```bash
python src/setup/upload_to_s3.py --data-dir ./data/
```

### 4. Set up Glue Crawler
```bash
python src/setup/glue_setup.py
```

### 5. Run PySpark ETL (locally or on AWS)
```bash
# Local
spark-submit src/etl/transform.py --input ./data/ --output ./output/

# Against S3
spark-submit src/etl/transform.py \
    --input s3://your-bucket/raw/ \
    --output s3://your-bucket/processed/
```

### 6. Create Redshift schema & load data
Run the SQL files in order via Redshift Query Editor:
1. `sql/ddl/create_star_schema.sql`
2. `sql/load/copy_to_redshift.sql` (update bucket & IAM role)
3. `sql/analytics/analytical_queries.sql`

## Key Engineering Decisions

| Decision | Rationale |
|----------|-----------|
| Parquet over CSV for processed layer | Columnar format, ~70% compression, predicate pushdown |
| Partition by year/month | Enables partition pruning on time-range queries |
| Explicit PySpark schemas | Prevents type-inference errors on messy source data |
| Star schema over flat table | Enables fast joins, clean dimensional analysis, SCD-ready |
| DISTKEY on customer_id | Most analytical queries filter/group by customer |
| dim_date as DISTSTYLE ALL | Small table, replicated to every node for fast joins |

## Analytical Queries Included

1. **Revenue by category by quarter** — slice-and-dice across product and time dimensions
2. **Delivery delay by seller state** — operational logistics insight
3. **Customer cohort retention** — repeat purchase rates by signup month
4. **Top sellers by revenue + review quality** — value vs satisfaction analysis
5. **Weekend vs weekday patterns** — purchasing behavior segmentation
6. **Monthly revenue trend with MoM growth** — time-series with window functions

## Tech Stack

- **Storage:** Amazon S3 (data lake — raw + processed layers)
- **Catalog:** AWS Glue Data Catalog + Crawlers
- **ETL:** PySpark (local / Glue / EMR compatible)
- **Warehouse:** Amazon Redshift (star schema)
- **Languages:** Python, SQL
