# ──────────────────────────────────────────────────────────────
# config.py — Central configuration for the pipeline
# Update these values with your own AWS resource names
# ──────────────────────────────────────────────────────────────

# S3
S3_BUCKET         = "your-ecommerce-pipeline-bucket"
S3_RAW_PREFIX     = "raw/"
S3_PROCESSED_PREFIX = "processed/"

# Glue
GLUE_DATABASE     = "ecommerce_catalog"
GLUE_CRAWLER_NAME = "ecommerce-raw-crawler"
GLUE_IAM_ROLE     = "arn:aws:iam::YOUR_ACCOUNT_ID:role/GlueServiceRole"

# Redshift Serverless
REDSHIFT_HOST     = "your-workgroup.region.redshift-serverless.amazonaws.com"
REDSHIFT_PORT     = 5439
REDSHIFT_DB       = "dev"
REDSHIFT_USER     = "admin"
REDSHIFT_PASSWORD = "YourPassword123!"   # use env vars in production
REDSHIFT_SCHEMA   = "ecommerce"
REDSHIFT_IAM_ROLE = "arn:aws:iam::YOUR_ACCOUNT_ID:role/RedshiftS3ReadRole"
