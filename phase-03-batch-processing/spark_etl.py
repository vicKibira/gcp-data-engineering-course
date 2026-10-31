"""
Phase 03 — LAB 3: Batch ETL with PySpark on Dataproc
=====================================================
Reads raw Parquet files from the GCS Bronze zone, applies a series of
cleaning and enrichment transforms, then writes the result to the Silver
zone — also as Parquet.

This is the Bronze → Silver step of the Medallion Architecture:
  Bronze (raw/):     unchanged data as ingested — never modified
  Silver (processed/): validated, typed, deduplicated, enriched

Spark is the right tool here because:
  - Parquet files can be very large (hundreds of GB) — Spark distributes
    the read across all cluster workers in parallel
  - Adaptive Query Execution (AQE) automatically optimises join and
    sort strategies at runtime based on actual data sizes

Submit via submit_dataproc.sh or directly:
  gcloud dataproc jobs submit pyspark spark_etl.py \
      --cluster=course-cluster --region=us-central1 \
      -- --bucket=YOUR_BUCKET --dataset=sales --date=2024-01-15
"""
import argparse
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def build_spark(app_name: str) -> SparkSession:
    """
    Create (or retrieve) the SparkSession for this job.

    Configuration notes:
      adaptive.enabled — Adaptive Query Execution rewrites the physical plan
        after each shuffle stage using actual runtime statistics. On Dataproc
        this can cut shuffle sizes by 30-60% for skewed datasets.
      coalescePartitions — AQE merges small shuffle partitions into fewer,
        larger ones. Without this, a small daily batch might produce 200 tiny
        output files instead of the optimal 4-8.
    """
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, bucket: str, dataset: str, date: str) -> DataFrame:
    """
    Read all Bronze Parquet files for a given dataset and date.

    Path pattern uses a wildcard (*) to read every file in the day's
    partition folder. Spark pushes the partition filter down to the
    file system listing, so only the target day's files are opened.

    Args:
        spark:   Active SparkSession.
        bucket:  GCS bucket name (no gs:// prefix).
        dataset: Dataset name, e.g. "sales".
        date:    Processing date in YYYY-MM-DD format.

    Returns:
        Unmodified Bronze DataFrame.
    """
    year, month, day = date.split("-")
    path = f"gs://{bucket}/raw/{dataset}/year={year}/month={month}/day={day}/*.parquet"
    df = spark.read.parquet(path)
    print(f"[read]  {df.count():,} rows from {path}")
    return df


def transform_sales(df: DataFrame) -> DataFrame:
    """
    Apply the Bronze → Silver transformation rules to the sales DataFrame.

    Transform steps (order matters):
      1. Rename 'order_date' → 'event_date' (standardise schema across datasets)
      2. Cast numeric columns to explicit types (Parquet infers types from
         the writer; we enforce the contract here)
      3. Compute derived columns:
           unit_price   = total / quantity   (for per-unit analytics)
           revenue_band = categorical bucket (for BI dashboard filters)
           processed_at = audit timestamp
      4. dropDuplicates on event_id — if the publisher sent a retry, we keep
         only the first occurrence (deterministic dedup)
      5. dropna on critical fields — rows missing event_id, user_id,
         product_id, or total cannot be usefully analysed

    Args:
        df: Raw Bronze DataFrame.

    Returns:
        Cleaned and enriched Silver DataFrame.
    """
    return (
        df
        # ── Step 1: Standardise naming ────────────────────────────────────
        .withColumnRenamed("order_date", "event_date")

        # ── Step 2: Enforce types ─────────────────────────────────────────
        .withColumn("total",    F.col("total").cast(DoubleType()))
        .withColumn("quantity", F.col("quantity").cast("integer"))

        # ── Step 3: Derived columns ───────────────────────────────────────
        # unit_price: used to detect pricing anomalies and per-unit margin analysis
        .withColumn("unit_price", F.round(F.col("total") / F.col("quantity"), 2))

        # revenue_band: low-cardinality grouping column — partitions well in BI tools
        # Thresholds: < $50 = low, $50-$199 = medium, $200+ = high
        .withColumn("revenue_band",
            F.when(F.col("total") < 50,  "low")
             .when(F.col("total") < 200, "medium")
             .otherwise("high")
        )

        # processed_at: audit trail — tells analysts when a row entered Silver
        .withColumn("processed_at", F.current_timestamp())

        # ── Step 4: Deduplication ─────────────────────────────────────────
        # event_id is the natural primary key; Pub/Sub guarantees at-least-once
        # so duplicates are expected, especially after a publisher retry
        .dropDuplicates(["event_id"])

        # ── Step 5: Null filter ───────────────────────────────────────────
        # Rows missing these fields are not actionable — drop rather than
        # forward bad data to analytics consumers
        .dropna(subset=["event_id", "user_id", "product_id", "total"])
    )


def write_silver(df: DataFrame, bucket: str, dataset: str, date: str) -> None:
    """
    Write the Silver DataFrame to GCS under the processed/ zone.

    Partitioning strategy:
      repartition(4) — 4 output files per daily batch. This assumes a
      daily volume of ~1-4 GB compressed Parquet. Tune upward for larger
      datasets. Too few partitions = large files that slow downstream reads;
      too many = many small files that hurt BigQuery and Dataproc listing times.

    Overwrite mode is intentional: re-running the job for the same date
    produces the correct result (idempotency). The Bronze files are never
    touched.

    Args:
        df:      Cleaned Silver DataFrame.
        bucket:  GCS bucket name.
        dataset: Dataset name.
        date:    Processing date in YYYY-MM-DD format.
    """
    year, month, day = date.split("-")
    path = f"gs://{bucket}/processed/{dataset}/year={year}/month={month}/day={day}/"

    (
        df
        .repartition(4)      # consolidate to 4 files (tune for your data volume)
        .write
        .mode("overwrite")   # safe to re-run for the same date
        .parquet(path)
    )
    print(f"[write] silver layer → {path}")


def run(bucket: str, dataset: str, date: str) -> None:
    """
    Orchestrate the full Bronze → Silver ETL for a single day.

    Args:
        bucket:  GCS bucket name.
        dataset: Dataset name.
        date:    Processing date in YYYY-MM-DD format.
    """
    spark = build_spark(f"etl-{dataset}-{date}")

    # Read raw data for the target date
    raw_df    = read_raw(spark, bucket, dataset, date)

    # Apply all transforms
    silver_df = transform_sales(raw_df)

    # Print row counts before/after to quantify data quality (dedup + null drops)
    print(f"[transform] {raw_df.count():,} → {silver_df.count():,} rows after cleaning")
    silver_df.printSchema()   # verify schema looks correct
    silver_df.show(5, truncate=False)  # spot-check sample rows in the logs

    # Write Silver output
    write_silver(silver_df, bucket, dataset, date)

    spark.stop()
    print("[done]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="PySpark Bronze → Silver ETL")
    parser.add_argument("--bucket",  required=True,         help="GCS bucket name (no gs://)")
    parser.add_argument("--dataset", default="sales",       help="Dataset name under raw/ and processed/")
    parser.add_argument("--date",    required=True,         help="Processing date YYYY-MM-DD")
    args = parser.parse_args()

    run(args.bucket, args.dataset, args.date)
