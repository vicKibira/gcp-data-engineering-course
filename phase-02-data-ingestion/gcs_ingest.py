"""
Phase 02 — LAB 2: Data Ingestion to GCS Data Lake
==================================================
Reads a CSV file (local path or public URL), partitions the data by date,
and writes Parquet files to the GCS Bronze (raw) zone using the Hive-style
partition layout:

  gs://BUCKET/raw/DATASET/year=YYYY/month=MM/day=DD/data.parquet

Why Parquet?
  - Columnar format — ~5-10x smaller than CSV for analytical queries
  - BigQuery and Spark can read it natively without conversion
  - Supports predicate pushdown (partition pruning by date)

Usage:
  python gcs_ingest.py \
      --source data/sales_2024.csv \
      --bucket my-project-datalake \
      --dataset sales \
      --date-col order_date
"""
import os
import argparse
from datetime import datetime
from google.cloud import storage
import pandas as pd


def upload_partitioned(
    df: pd.DataFrame,
    bucket_name: str,
    prefix: str,
    date_col: str,
) -> list[str]:
    """
    Partition a DataFrame by date and upload each partition as a Parquet file.

    Why partition by date?
    - Enables downstream tools (Spark, BigQuery) to skip irrelevant partitions
      when filtering by date — much faster than scanning the whole dataset.
    - Makes incremental ingestion easy: each day's data lands in its own folder
      so re-running a single day never touches other partitions.

    Args:
        df:          DataFrame with a datetime column to partition on.
        bucket_name: GCS bucket name (no gs:// prefix).
        prefix:      Base path inside the bucket, e.g. "raw/sales".
        date_col:    Column name containing the datetime values.

    Returns:
        List of gs:// URIs for the uploaded files.
    """
    client = storage.Client()  # uses ADC (Application Default Credentials)
    bucket = client.bucket(bucket_name)
    uploaded = []

    # Group rows by calendar date to produce one file per day
    for date, group in df.groupby(df[date_col].dt.date):
        # Hive-style path: year=2024/month=01/day=15/data.parquet
        # BigQuery and Spark understand this partitioning format natively
        blob_path = (
            f"{prefix}"
            f"/year={date.year}"
            f"/month={date.month:02d}"
            f"/day={date.day:02d}"
            f"/data.parquet"
        )
        blob = bucket.blob(blob_path)

        # Serialize to Parquet in memory — avoids a temp file on disk
        parquet_bytes = group.to_parquet(index=False)
        blob.upload_from_string(parquet_bytes, content_type="application/octet-stream")

        print(f"  ✓ gs://{bucket_name}/{blob_path}  ({len(group):,} rows)")
        uploaded.append(f"gs://{bucket_name}/{blob_path}")

    return uploaded


def ingest(
    source: str,
    bucket_name: str,
    dataset_name: str,
    date_col: str,
) -> None:
    """
    Full ingestion pipeline: load CSV → partition by date → upload to GCS.

    This produces the Bronze layer of the Medallion Architecture.
    The raw data is written as-is (no transforms) so we can always
    replay from source if downstream processing has bugs.

    Args:
        source:       Local file path or public URL to a CSV file.
        bucket_name:  Destination GCS bucket (no gs:// prefix).
        dataset_name: Logical dataset name used as the folder prefix.
        date_col:     Name of the column to partition by.
    """
    print(f"[ingestion] source={source}  bucket={bucket_name}  dataset={dataset_name}")

    # ── Load ──────────────────────────────────────────────────────────────────
    # parse_dates converts the date column to datetime64 so groupby works
    df = pd.read_csv(source, parse_dates=[date_col])
    print(f"  loaded {len(df):,} rows  |  columns: {list(df.columns)}")

    # ── Write raw Parquet (Bronze layer) ──────────────────────────────────────
    # prefix = "raw/sales" → partitioned under gs://BUCKET/raw/sales/year=.../
    prefix = f"raw/{dataset_name}"
    paths = upload_partitioned(df, bucket_name, prefix, date_col)

    print(
        f"\n[ingestion] done — {len(paths)} partitions written "
        f"to gs://{bucket_name}/{prefix}/"
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest CSV → GCS partitioned Parquet (Bronze layer)")
    parser.add_argument("--source",    required=True,       help="Local CSV path or public URL")
    parser.add_argument("--bucket",    required=True,       help="GCS bucket name (no gs://)")
    parser.add_argument("--dataset",   default="sales",     help="Dataset prefix inside raw/")
    parser.add_argument("--date-col",  default="order_date",help="Column to partition by (must be datetime-parseable)")
    args = parser.parse_args()

    ingest(args.source, args.bucket, args.dataset, args.date_col)
