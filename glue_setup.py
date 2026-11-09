"""
glue_setup.py — Create a Glue database and crawler for the raw S3 data

The crawler auto-discovers schemas from the CSVs in your raw/ prefix
and registers them in the Glue Data Catalog.

Usage:
    python glue_setup.py
"""

import os
import sys
import time
import boto3
from botocore.exceptions import ClientError

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import S3_BUCKET, S3_RAW_PREFIX, GLUE_DATABASE, GLUE_CRAWLER_NAME, GLUE_IAM_ROLE

glue = boto3.client("glue")


def create_database() -> None:
    try:
        glue.get_database(Name=GLUE_DATABASE)
        print(f"Database '{GLUE_DATABASE}' already exists.")
    except ClientError:
        print(f"Creating Glue database '{GLUE_DATABASE}'...")
        glue.create_database(
            DatabaseInput={
                "Name": GLUE_DATABASE,
                "Description": "E-commerce raw data catalog — auto-discovered by crawler",
            }
        )


def create_crawler() -> None:
    try:
        glue.get_crawler(Name=GLUE_CRAWLER_NAME)
        print(f"Crawler '{GLUE_CRAWLER_NAME}' already exists.")
    except ClientError:
        print(f"Creating Glue crawler '{GLUE_CRAWLER_NAME}'...")
        glue.create_crawler(
            Name=GLUE_CRAWLER_NAME,
            Role=GLUE_IAM_ROLE,
            DatabaseName=GLUE_DATABASE,
            Targets={
                "S3Targets": [
                    {"Path": f"s3://{S3_BUCKET}/{S3_RAW_PREFIX}"}
                ]
            },
            SchemaChangePolicy={
                "UpdateBehavior": "UPDATE_IN_DATABASE",
                "DeleteBehavior": "LOG",
            },
            Description="Crawls raw e-commerce CSVs and catalogs their schemas",
        )


def run_crawler() -> None:
    print(f"Starting crawler '{GLUE_CRAWLER_NAME}'...")
    glue.start_crawler(Name=GLUE_CRAWLER_NAME)

    while True:
        resp = glue.get_crawler(Name=GLUE_CRAWLER_NAME)
        state = resp["Crawler"]["State"]
        print(f"  Crawler state: {state}")
        if state == "READY":
            break
        time.sleep(15)

    # Show discovered tables
    tables = glue.get_tables(DatabaseName=GLUE_DATABASE)["TableList"]
    print(f"\nCrawler finished — {len(tables)} tables discovered:")
    for t in tables:
        cols = len(t.get("StorageDescriptor", {}).get("Columns", []))
        print(f"  {t['Name']}  ({cols} columns)")


if __name__ == "__main__":
    create_database()
    create_crawler()
    run_crawler()
