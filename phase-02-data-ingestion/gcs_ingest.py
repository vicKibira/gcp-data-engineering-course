"""
Phase 02 — LAB 2: Data Ingestion to GCS Data Lake
Reads CSV from local/URL, partitions by date, writes to GCS raw zone.
"""
import os
import argparse
from datetime import datetime
from google.cloud import storage
import pandas as pd


def upload_partitioned(df: pd.DataFrame, bucket_name: str, prefix: str, date_col: str) -> list[str]:
    """Upload DataFrame partitioned by date to GCS."""
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    uploaded = []

    for date, group in df.groupby(df[date_col].dt.date):
        blob_path = f"{prefix}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.parquet"
        blob = bucket.blob(blob_path)

        parquet_bytes = group.to_parquet(index=False)
        blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

        print(f"  ✓ gs://{bucket_name}/{blob_path}  ({len(group):,} rows)")
        uploaded.append(f"gs://{bucket_name}/{blob_path}")

    return uploaded


def ingest(source: str, bucket_name: str, dataset_name: str, date_col: str) -> None:
    print(f"[ingestion] source={source}  bucket={bucket_name}  dataset={dataset_name}")

    # Load — CSV from local path or URL
    df = pd.read_csv(source, parse_dates=[date_col])
    print(f"  loaded {len(df):,} rows  |  columns: {list(df.columns)}")

    # Write raw parquet (Bronze layer)
    prefix = f"raw/{dataset_name}"
    paths = upload_partitioned(df, bucket_name, prefix, date_col)
    print(f"\n[ingestion] done — {len(paths)} partitions written to gs://{bucket_name}/{prefix}/")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV → GCS partitioned parquet")
    parser.add_argument("--source",       required=True,  help="Local CSV path or public URL")
    parser.add_argument("--bucket",       required=True,  help="GCS bucket name (no gs://)")
    parser.add_argument("--dataset",      default="sales", help="Dataset prefix inside raw/")
    parser.add_argument("--date-col",     default="order_date", help="Column to partition by")
    args = parser.parse_args()

    ingest(args.source, args.bucket, args.dataset, args.date_col)
