"""
upload_to_s3.py — Upload raw Kaggle CSVs to S3 data lake (raw layer)

Usage:
    1. Download the Brazilian E-Commerce dataset from Kaggle:
       https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
    2. Extract CSVs into a local folder (e.g., ./data/)
    3. Run: python upload_to_s3.py --data-dir ./data/
"""

import os
import sys
import argparse
import boto3
from botocore.exceptions import ClientError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import S3_BUCKET, S3_RAW_PREFIX

# Map each CSV filename to its entity folder in the data lake
FILE_ENTITY_MAP = {
    "olist_orders_dataset.csv":                "orders",
    "olist_order_items_dataset.csv":           "order_items",
    "olist_order_payments_dataset.csv":        "payments",
    "olist_order_reviews_dataset.csv":         "reviews",
    "olist_products_dataset.csv":              "products",
    "olist_customers_dataset.csv":             "customers",
    "olist_sellers_dataset.csv":               "sellers",
    "olist_geolocation_dataset.csv":           "geolocation",
}


def upload_files(data_dir: str) -> None:
    s3 = boto3.client("s3")

    # Create bucket if it doesn't exist
    try:
        s3.head_bucket(Bucket=S3_BUCKET)
        print(f"Bucket '{S3_BUCKET}' exists.")
    except ClientError:
        print(f"Creating bucket '{S3_BUCKET}'...")
        s3.create_bucket(
            Bucket=S3_BUCKET,
            CreateBucketConfiguration={"LocationConstraint": boto3.session.Session().region_name},
        )

    uploaded = 0
    for filename, entity in FILE_ENTITY_MAP.items():
        local_path = os.path.join(data_dir, filename)
        if not os.path.isfile(local_path):
            print(f"  SKIP  {filename} — not found in {data_dir}")
            continue

        s3_key = f"{S3_RAW_PREFIX}{entity}/{filename}"
        size_mb = os.path.getsize(local_path) / (1024 * 1024)
        print(f"  UPLOAD  {filename} ({size_mb:.1f} MB) → s3://{S3_BUCKET}/{s3_key}")
        s3.upload_file(local_path, S3_BUCKET, s3_key)
        uploaded += 1

    print(f"\nDone — {uploaded}/{len(FILE_ENTITY_MAP)} files uploaded to s3://{S3_BUCKET}/{S3_RAW_PREFIX}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload raw CSVs to S3 data lake")
    parser.add_argument("--data-dir", required=True, help="Local folder with Kaggle CSVs")
    args = parser.parse_args()
    upload_files(args.data_dir)
