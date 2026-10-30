"""
Phase 03 — LAB 3: Batch ETL with PySpark on Dataproc
Reads raw parquet from GCS, applies transformations, writes Silver layer.

Submit via:
    gcloud dataproc jobs submit pyspark spark_etl.py \
        --cluster=course-cluster \
        --region=us-central1 \
        -- --bucket=YOUR_BUCKET --dataset=sales --date=2024-01-15
"""
import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate()
    )


def read_raw(spark: SparkSession, bucket: str, dataset: str, date: str) -> "DataFrame":
    year, month, day = date.split("-")
    path = f"gs://{bucket}/raw/{dataset}/year={year}/month={month}/day={day}/*.parquet"
    df = spark.read.parquet(path)
    print(f"[read]  {df.count():,} rows from {path}")
    return df


def transform_sales(df: "DataFrame") -> "DataFrame":
    return (
        df
        # Standardise column names
        .withColumnRenamed("order_date", "event_date")
        # Cast and clean
        .withColumn("total",    F.col("total").cast(DoubleType()))
        .withColumn("quantity", F.col("quantity").cast("integer"))
        # Derived fields
        .withColumn("unit_price",    F.round(F.col("total") / F.col("quantity"), 2))
        .withColumn("revenue_band",  F.when(F.col("total") < 50,  "low")
                                      .when(F.col("total") < 200, "medium")
                                      .otherwise("high"))
        .withColumn("processed_at",  F.current_timestamp())
        # Drop duplicates on natural key
        .dropDuplicates(["event_id"])
        # Drop nulls on critical fields
        .dropna(subset=["event_id", "user_id", "product_id", "total"])
    )


def write_silver(df: "DataFrame", bucket: str, dataset: str, date: str) -> None:
    year, month, day = date.split("-")
    path = f"gs://{bucket}/processed/{dataset}/year={year}/month={month}/day={day}/"

    (
        df
        .repartition(4)          # reasonable partition count for daily batch
        .write
        .mode("overwrite")
        .parquet(path)
    )
    print(f"[write] silver layer → {path}")


def run(bucket: str, dataset: str, date: str) -> None:
    spark = build_spark(f"etl-{dataset}-{date}")

    raw_df    = read_raw(spark, bucket, dataset, date)
    silver_df = transform_sales(raw_df)

    print(f"[transform] {raw_df.count():,} → {silver_df.count():,} rows after cleaning")
    silver_df.printSchema()
    silver_df.show(5, truncate=False)

    write_silver(silver_df, bucket, dataset, date)
    spark.stop()
    print("[done]")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket",  required=True)
    parser.add_argument("--dataset", default="sales")
    parser.add_argument("--date",    required=True, help="YYYY-MM-DD")
    args = parser.parse_args()

    run(args.bucket, args.dataset, args.date)
