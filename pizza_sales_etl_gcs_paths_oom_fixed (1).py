"""
Pizza Sales ETL: a beginner-friendly PySpark script.
Dataset columns: pizza_id, order_id, pizza_name_id, quantity, order_date,
order_time, unit_price, total_price, pizza_size, pizza_category,
pizza_ingredients, pizza_name.

Change the file and output paths below. Then run this script.

This script shows: SparkSession, CSV ingestion/schema, lazy evaluation,
transformations/actions, quality/bad_sales, type casting, deduplication,
date features, DataFrames, Spark SQL, groupBy aggregations, window functions,
Parquet, Hive partitions, explain plan, AQE, shuffle settings, GCS,
optional BigQuery connector. Memory-conscious version: avoids repeated count()
actions and caching on the single-node cluster.
"""
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark import StorageLevel

COLS = ["pizza_id", "order_id", "pizza_name_id", "quantity", "order_date",
        "order_time", "unit_price", "total_price", "pizza_size",
        "pizza_category", "pizza_ingredients", "pizza_name"]


# Change these settings here. No command-line arguments are needed.
SOURCE_PATH = "gs://project-27c25bbf-b46d-4bff-abd-datalake/scripts/pizza_sales.csv"
OUTPUT_PATH = "gs://project-27c25bbf-b46d-4bff-abd-datalake/output/pizza_sales"
DATE_FORMAT = "M/d/yyyy"
WRITE_MODE = "overwrite"              # "overwrite" or "append"
SHUFFLE_PARTITIONS = 200
SAMPLE_FRACTION = 1.0                 # 1.0 = full dataset; e.g. 0.1 = 10% sample

# BigQuery is optional. Leave BQ_PROJECT_ID = None to skip it.
BQ_PROJECT_ID = None                  # e.g. "my-gcp-project"
BQ_DATASET = "pizza_sales"
BQ_TEMP_BUCKET = None                 # e.g. "my-gcp-temp-bucket"


def start_spark():
    # SparkSession is the Spark SQL/DataFrame entry point. AQE can adapt plans
    # after runtime statistics are available; compression uses Snappy for Parquet.
    return (SparkSession.builder.appName("PizzaSales_ETL_A_to_Z")
            .config("spark.sql.shuffle.partitions", SHUFFLE_PARTITIONS)
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "snappy")
            .getOrCreate())


def read_pizza_sales(spark, source, fraction):
    # Read raw_sales fields first; explicit casts in Silver avoid inconsistent inference.
    df = (spark.read.option("header", True).option("mode", "PERMISSIVE")
          .option("quote", '"').option("escape", '"').csv(source))
    df = df.toDF(*[c.strip() for c in df.columns])
    missing = [c for c in COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required pizza dataset columns {missing}; found {df.columns}")
    df = df.select(*COLS)
    if fraction <= 0 or fraction > 1:
        raise ValueError("--sample must be in (0,1]")
    if fraction < 1:
        df = df.sample(False, fraction, seed=17)
    return df


def save_pizza_data(df, path, mode="overwrite", partitions=None):
    # Parquet is columnar/compressed. Partition only by low/medium-cardinality
    # date keys, not identifiers such as order_id or pizza_id.
    w = df.write.mode(mode).format("parquet").option("compression", "snappy")
    if partitions:
        w = w.partitionBy(*partitions)
    w.save(path)


def clean_pizza_sales(raw_sales, date_format):
    # SAFE conversions: malformed values become null, then are quarantined.
    parsed = raw_sales.select(
        F.expr("try_cast(trim(pizza_id) as bigint)").alias("pizza_id"),
        F.expr("try_cast(trim(order_id) as bigint)").alias("order_id"),
        F.nullif(F.trim("pizza_name_id"), F.lit("")).alias("pizza_name_id"),
        F.expr("try_cast(trim(quantity) as int)").alias("quantity"),
        F.to_date(F.trim("order_date"), date_format).alias("order_date"),
        F.to_timestamp(F.concat_ws(" ", F.trim("order_date"), F.trim("order_time")),
                       date_format + " HH:mm:ss").alias("order_timestamp"),
        F.expr("try_cast(trim(unit_price) as decimal(10,2))").alias("unit_price"),
        F.expr("try_cast(trim(total_price) as decimal(12,2))").alias("total_price"),
        F.upper(F.nullif(F.trim("pizza_size"), F.lit(""))).alias("pizza_size"),
        F.initcap(F.nullif(F.trim("pizza_category"), F.lit(""))).alias("pizza_category"),
        F.nullif(F.trim("pizza_ingredients"), F.lit("")).alias("pizza_ingredients"),
        F.nullif(F.trim("pizza_name"), F.lit("")).alias("pizza_name")
    ).withColumn("order_time", F.date_format("order_timestamp", "HH:mm:ss"))

    bad = (F.col("pizza_id").isNull() | F.col("order_id").isNull() |
           F.col("pizza_name_id").isNull() | F.col("quantity").isNull() |
           (F.col("quantity") <= 0) | F.col("order_date").isNull() |
           F.col("order_timestamp").isNull() | F.col("unit_price").isNull() |
           (F.col("unit_price") <= 0) | F.col("total_price").isNull() |
           (F.col("total_price") <= 0) | F.col("pizza_size").isNull() |
           F.col("pizza_category").isNull() | F.col("pizza_name").isNull())

    bad_sales = parsed.filter(bad)
    valid = parsed.filter(~bad)

    # Dedup assumes pizza_id is unique line-item ID; verify this assumption
    # against the source definition before production deployment.
    win = Window.partitionBy("pizza_id").orderBy("order_timestamp", "order_id")
    clean_sales = (valid.withColumn("_rn", F.row_number().over(win))
              .filter(F.col("_rn") == 1).drop("_rn")
              .withColumn("order_year", F.year("order_date"))
              .withColumn("order_month", F.month("order_date"))
              .withColumn("order_day", F.dayofmonth("order_date"))
              .withColumn("order_weekday", F.date_format("order_date", "EEEE"))
              .withColumn("order_hour", F.hour("order_timestamp"))
              .withColumn("order_month_key", F.date_format("order_date", "yyyy-MM")))
    return clean_sales, bad_sales


def make_sales_reports(s):
    # Each DataFrame is a lazy logical plan until an action (show/count/write).
    overall = s.agg(F.round(F.sum("total_price"), 2).alias("total_revenue"),
                    F.countDistinct("order_id").alias("total_orders"),
                    F.sum("quantity").alias("total_pizzas_sold"))         .withColumn("average_order_value",
                    F.round(F.col("total_revenue") / F.col("total_orders"), 2))

    category = s.groupBy("pizza_category").agg(
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
        F.sum("quantity").alias("pizzas_sold"),
        F.countDistinct("order_id").alias("orders")).orderBy(F.desc("total_revenue"))

    top_quantity = s.groupBy("pizza_name").agg(
        F.sum("quantity").alias("total_quantity_sold"),
        F.round(F.sum("total_price"), 2).alias("total_revenue"))         .orderBy(F.desc("total_quantity_sold")).limit(10)

    top_revenue = s.groupBy("pizza_name").agg(
        F.round(F.sum("total_price"), 2).alias("total_revenue"),
        F.sum("quantity").alias("total_quantity_sold"))         .orderBy(F.desc("total_revenue")).limit(10)

    size = s.groupBy("pizza_size").agg(F.round(F.sum("total_price"), 2).alias("revenue"),
                                       F.sum("quantity").alias("pizzas_sold"))         .orderBy(F.desc("revenue"))

    hourly = s.groupBy("order_hour").agg(F.countDistinct("order_id").alias("orders"),
                                         F.sum("quantity").alias("pizzas_sold"),
                                         F.round(F.sum("total_price"), 2).alias("revenue"))         .orderBy(F.desc("orders"))

    daily = s.groupBy("order_date").agg(F.round(F.sum("total_price"), 2).alias("daily_revenue"),
                                        F.countDistinct("order_id").alias("daily_orders"),
                                        F.sum("quantity").alias("daily_pizzas_sold"))         .orderBy("order_date")

    monthly = s.groupBy("order_month_key").agg(
        F.round(F.sum("total_price"), 2).alias("monthly_revenue"),
        F.countDistinct("order_id").alias("monthly_orders"),
        F.sum("quantity").alias("monthly_pizzas_sold")).orderBy("order_month_key")

    # AOV correctly calculated at order granularity, not row granularity.
    per_order = s.groupBy("order_id").agg(F.sum("total_price").alias("order_total"))
    order_stats = per_order.agg(F.round(F.avg("order_total"), 2).alias("average_order_value"),
                                F.round(F.max("order_total"), 2).alias("largest_order"),
                                F.round(F.min("order_total"), 2).alias("smallest_order"))

    weekday = s.groupBy(F.dayofweek("order_date").alias("weekday_number"),
                        F.date_format("order_date", "EEEE").alias("day_of_week"))         .agg(F.countDistinct("order_id").alias("orders"), F.sum("quantity").alias("pizzas_sold"),
             F.round(F.sum("total_price"), 2).alias("revenue")).orderBy("weekday_number")

    bottom = s.groupBy("pizza_name").agg(F.sum("quantity").alias("quantity_sold"),
                                         F.round(F.sum("total_price"), 2).alias("revenue"))         .orderBy(F.asc("quantity_sold")).limit(10)

    # Window function example: rank pizzas by revenue within each category.
    w = Window.partitionBy("pizza_category").orderBy(F.desc("revenue"))
    category_rank = (s.groupBy("pizza_category", "pizza_name")
                     .agg(F.round(F.sum("total_price"), 2).alias("revenue"))
                     .withColumn("rank_in_category", F.dense_rank().over(w))
                     .filter(F.col("rank_in_category") <= 3))

    return {"overall_metrics": overall, "revenue_by_category": category,
            "top_10_by_quantity": top_quantity, "top_10_by_revenue": top_revenue,
            "revenue_by_size": size, "sales_by_hour": hourly, "daily_revenue": daily,
            "monthly_revenue": monthly, "order_value_stats": order_stats,
            "sales_by_weekday": weekday, "bottom_10_by_quantity": bottom,
            "top_3_pizzas_per_category": category_rank}


def show_sql_examples(s):
    # createOrReplaceTempView bridges DataFrame API and Spark SQL.
    s.createOrReplaceTempView("pizza_sales_silver")
    sql_examples = [
        ("Overall metrics", """SELECT ROUND(SUM(total_price),2) revenue,
          COUNT(DISTINCT order_id) orders, SUM(quantity) pizzas_sold
          FROM pizza_sales_silver"""),
        ("Revenue by category", """SELECT pizza_category,
          ROUND(SUM(total_price),2) revenue FROM pizza_sales_silver
          GROUP BY pizza_category ORDER BY revenue DESC"""),
        ("Top pizzas", """SELECT pizza_name, SUM(quantity) quantity_sold
          FROM pizza_sales_silver GROUP BY pizza_name
          ORDER BY quantity_sold DESC LIMIT 10"""),
        ("Hourly", """SELECT order_hour, COUNT(DISTINCT order_id) orders,
          ROUND(SUM(total_price),2) revenue FROM pizza_sales_silver
          GROUP BY order_hour ORDER BY order_hour"""),
        ("Daily", """SELECT order_date, ROUND(SUM(total_price),2) revenue
          FROM pizza_sales_silver GROUP BY order_date ORDER BY order_date""")
    ]
    for label, query in sql_examples:
        logging.info("Spark SQL sample: %s", label)
        s.sparkSession.sql(query).show(20, truncate=False)


def save_to_bigquery(df, table, temp_bucket):
    # Needs the Spark BigQuery connector installed/configured in the runtime,
    # plus IAM permissions. Table name should be project.dataset.table.
    writer = df.write.format("bigquery").option("table", table).mode("overwrite")
    if temp_bucket:
        writer = writer.option("temporaryGcsBucket", temp_bucket)
    writer.save()


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    spark = start_spark()
    try:
        logging.info("Spark version: %s", spark.version)

        # STEP 1: Read CSV. Reading and transformations are lazy plans.
        raw_sales = read_pizza_sales(spark, SOURCE_PATH, SAMPLE_FRACTION)
        raw_sales.printSchema()
        raw_sales.show(5, truncate=False)

        # STEP 2: Bronze preserves source representation as Parquet.
        save_pizza_data(raw_sales, f"{OUTPUT_PATH}/bronze/raw_sales", WRITE_MODE)

        # STEP 3: Silver casts, standardizes, validates, quarantines, deduplicates.
        clean_sales, bad_sales = clean_pizza_sales(raw_sales, DATE_FORMAT)
        clean_sales.printSchema()
        save_pizza_data(clean_sales, f"{OUTPUT_PATH}/clean_sales/cleaned",
                      WRITE_MODE, ["order_year", "order_month"])
        save_pizza_data(bad_sales, f"{OUTPUT_PATH}/bad_sales/invalid", WRITE_MODE)

        # Avoid three separate count() actions here. Each count would re-run
        # the upstream CSV parsing/cleaning lineage, and on a small single-node
        # Dataproc cluster this can create unnecessary driver/executor pressure.
        # The Bronze/Silver/Bad writes above already execute the DataFrames.
        logging.info("Bronze, Silver, and quarantine datasets were written.")

        # STEP 4: Gold aggregate tables for business analytics.
        sales_reports = make_sales_reports(clean_sales)
        for name, df in sales_reports.items():
            logging.info("Gold result: %s", name)
            df.show(20, truncate=False)
            save_pizza_data(df, f"{OUTPUT_PATH}/sales_reports/{name}", WRITE_MODE)

        # STEP 5: Learn SQL interface over same Silver DataFrame.
        show_sql_examples(clean_sales)

        # STEP 6: Explain logical/physical plan; inspect Spark UI for stages,
        # shuffle, task time, skew, spill, executor memory, and failed tasks.
        clean_sales.groupBy("pizza_category").agg(F.sum("total_price").alias("revenue")).explain(mode="formatted")

        # STEP 7: Optional publish Gold datasets to BigQuery.
        if BQ_PROJECT_ID:
            for name, df in sales_reports.items():
                save_to_bigquery(df, f"{BQ_PROJECT_ID}.{BQ_DATASET}.{name}",
                                        BQ_TEMP_BUCKET)

        logging.info("ETL completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
