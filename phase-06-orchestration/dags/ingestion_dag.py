"""
Phase 06 — LAB 6A: Cloud Composer DAG — Daily Batch Ingestion Pipeline
=======================================================================
Orchestrates the full Bronze → Silver → Gold data pipeline on a nightly
schedule using Apache Airflow running on Cloud Composer.

DAG schedule: 02:00 UTC daily (after midnight, once the previous day's
Pub/Sub data has finished flowing into GCS via the batch ingest script).

Task graph:
  check_raw_data         (GCS file existence check)
       │
  spark_etl              (Dataproc PySpark job: Bronze → Silver Parquet)
       │
  dbt_silver             (dbt incremental model: Silver Parquet → BQ Silver)
       │
  bq_gold_daily_revenue  (BQ INSERT: Silver BQ → Gold aggregates)
       │
  dbt_test               (dbt test: data quality checks on silver + gold)

Key Airflow concepts demonstrated:
  - Airflow Variables ({{ var.value.key }}) for environment-agnostic config
  - Jinja templating for dynamic dates ({{ ds }}, {{ macros.ds_format(...) }})
  - Operator chaining with >> (linear dependency)
  - max_active_runs=1 prevents overlapping pipeline runs

Deployment:
  1. Upload this file to the Cloud Composer GCS DAGs folder:
       gsutil cp ingestion_dag.py gs://COMPOSER_BUCKET/dags/
  2. Set Airflow Variables in the Composer UI:
       project_id, region, bucket_name, bq_dataset

Airflow Variables needed:
  project_id    — GCP project ID
  region        — GCP region (e.g. us-central1)
  bucket_name   — GCS bucket name
  bq_dataset    — BigQuery dataset name
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago


# ── Configuration ─────────────────────────────────────────────────────────────
# Airflow Variables are resolved at runtime from the Composer Variables store.
# This makes the DAG portable across dev/staging/prod environments without
# code changes — just set different variable values in each environment.
PROJECT_ID  = "{{ var.value.project_id }}"
REGION      = "{{ var.value.region }}"
BUCKET      = "{{ var.value.bucket_name }}"
DATASET     = "{{ var.value.bq_dataset }}"
CLUSTER     = "course-cluster"                  # Dataproc cluster name (created in phase-03)
DBT_DIR     = "/home/airflow/gcs/dags/dbt"     # dbt project path inside Composer's GCS bucket


# ── Default task arguments ─────────────────────────────────────────────────────
# These apply to every task unless overridden at the task level.
# retries=2 with 5-minute delays handles transient GCP API errors (quota, network).
default_args = {
    "owner":            "victor",
    "depends_on_past":  False,    # each DAG run is independent (no chain dependencies)
    "email_on_failure": True,     # set email in Airflow connections for alerts
    "email_on_retry":   False,    # don't spam on every retry; only on final failure
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="daily_ingestion_pipeline",
    description="Daily batch: GCS → Spark ETL → dbt Silver → BQ Gold",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # cron: minute=0, hour=2, every day (02:00 UTC)
    start_date=days_ago(1),
    catchup=False,                   # don't backfill missed runs when DAG is first activated
    tags=["phase-06", "ingestion", "batch"],
    max_active_runs=1,               # prevent concurrent pipeline runs (avoids data races)
) as dag:

    # ── Task 1: Verify yesterday's raw data landed in GCS ────────────────────
    # GCSListObjectsOperator lists objects at the given prefix.
    # If the prefix is empty (no files), the task fails — signalling that the
    # upstream ingest script (phase-02) or Pub/Sub export did not run.
    # {{ ds }} is Airflow's execution date in YYYY-MM-DD format.
    # macros.ds_format(...) extracts year/month/day components for the Hive path.
    check_raw_data = GCSListObjectsOperator(
        task_id="check_raw_data",
        bucket=BUCKET,
        prefix="raw/sales/year={{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}"
               "/month={{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}"
               "/day={{ macros.ds_format(ds, '%Y-%m-%d', '%d') }}/",
    )

    # ── Task 2: Submit Spark ETL job on Dataproc ─────────────────────────────
    # DataprocSubmitJobOperator submits a PySpark job to the persistent cluster
    # and waits for it to complete (blocking). If the job fails, the task retries.
    # {{ ds }} passes the execution date to spark_etl.py as --date argument.
    spark_etl = DataprocSubmitJobOperator(
        task_id="spark_etl",
        project_id=PROJECT_ID,
        region=REGION,
        job={
            "placement": {"cluster_name": CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{BUCKET}/scripts/spark_etl.py",
                "args": [
                    "--bucket", BUCKET,
                    "--dataset", "sales",
                    "--date", "{{ ds }}",   # Airflow injects the execution date here
                ],
            },
        },
    )

    # ── Task 3: Run dbt Silver incremental model ──────────────────────────────
    # BashOperator runs dbt inside the Composer worker.
    # The dbt project is mounted from the Composer GCS bucket at DBT_DIR.
    # --select silver runs only the silver_events model (skips bronze and gold).
    # --vars passes the run_date for use in custom macros if needed.
    # --profiles-dir points to the profiles.yml in the same dbt project folder.
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select silver --vars '{\"run_date\": \"{{ ds }}\"}' "
            "--profiles-dir /home/airflow/gcs/dags/dbt"
        ),
    )

    # ── Task 4: Materialise Gold aggregates in BigQuery ───────────────────────
    # BigQueryInsertJobOperator runs a BigQuery job (SQL query).
    # This INSERT adds yesterday's aggregated row to gold_daily_revenue.
    # Note: {{ ds }} in the BQ query must be double-escaped as {{{{ ds }}}} because
    # Airflow first renders the Python f-string, then Jinja renders {{ ds }}.
    bq_gold = BigQueryInsertJobOperator(
        task_id="bq_gold_daily_revenue",
        configuration={
            "query": {
                "query": f"""
                    INSERT INTO `{PROJECT_ID}.{DATASET}.gold_daily_revenue`
                    (report_date, category, event_type, order_count,
                     total_quantity, total_revenue, avg_order_value, updated_at)
                    SELECT
                      event_date, category, event_type,
                      COUNT(DISTINCT event_id),
                      SUM(quantity),
                      ROUND(SUM(total), 2),
                      ROUND(AVG(total), 2),
                      CURRENT_TIMESTAMP()
                    FROM `{PROJECT_ID}.{DATASET}.silver_events`
                    -- {{{{ ds }}}} is Airflow Jinja; double-brace escapes the Python f-string
                    WHERE event_date = DATE('{{{{ ds }}}}')
                    GROUP BY event_date, category, event_type
                """,
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",   # append; do not overwrite existing rows
            }
        },
        location=REGION,
    )

    # ── Task 5: dbt data quality tests ───────────────────────────────────────
    # Runs all dbt tests defined in schema.yml for the silver and gold models.
    # If any test fails (e.g. duplicate event_ids, null values), the task fails
    # and the team is alerted before bad data reaches dashboards.
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test --select silver gold "
            "--profiles-dir /home/airflow/gcs/dags/dbt"
        ),
    )

    # ── Task dependency chain ─────────────────────────────────────────────────
    # >> sets the execution order: A >> B means B runs after A succeeds.
    # Linear chain: check → etl → dbt → gold → test
    check_raw_data >> spark_etl >> dbt_silver >> bq_gold >> dbt_test
