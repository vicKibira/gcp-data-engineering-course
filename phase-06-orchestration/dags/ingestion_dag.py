"""
Phase 06 — LAB 6A: Cloud Composer DAG — Daily Batch Ingestion Pipeline
Orchestrates: GCS ingest → Dataproc Spark ETL → dbt Silver → BQ Gold
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


PROJECT_ID  = "{{ var.value.project_id }}"
REGION      = "{{ var.value.region }}"
BUCKET      = "{{ var.value.bucket_name }}"
DATASET     = "{{ var.value.bq_dataset }}"
CLUSTER     = "course-cluster"
DBT_DIR     = "/home/airflow/gcs/dags/dbt"


default_args = {
    "owner":            "victor",
    "depends_on_past":  False,
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
}

with DAG(
    dag_id="daily_ingestion_pipeline",
    description="Daily batch: GCS → Spark ETL → dbt Silver → BQ Gold",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # 02:00 UTC every day
    start_date=days_ago(1),
    catchup=False,
    tags=["phase-06", "ingestion", "batch"],
    max_active_runs=1,
) as dag:

    # ── 1. Verify yesterday's raw data landed in GCS ──────────────────────────
    check_raw_data = GCSListObjectsOperator(
        task_id="check_raw_data",
        bucket=BUCKET,
        prefix="raw/sales/year={{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}"
               "/month={{ macros.ds_format(ds, '%Y-%m-%d', '%m') }}"
               "/day={{ macros.ds_format(ds, '%Y-%m-%d', '%d') }}/",
    )

    # ── 2. Submit Spark ETL job on Dataproc ───────────────────────────────────
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
                    "--date", "{{ ds }}",
                ],
            },
        },
    )

    # ── 3. Run dbt Silver layer ───────────────────────────────────────────────
    dbt_silver = BashOperator(
        task_id="dbt_silver",
        bash_command=(
            f"cd {DBT_DIR} && "
            "dbt run --select silver --vars '{\"run_date\": \"{{ ds }}\"}' "
            "--profiles-dir /home/airflow/gcs/dags/dbt"
        ),
    )

    # ── 4. Materialise Gold aggregates in BigQuery ────────────────────────────
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
                    WHERE event_date = DATE('{{{{ ds }}}}')
                    GROUP BY event_date, category, event_type
                """,
                "useLegacySql": False,
                "writeDisposition": "WRITE_APPEND",
            }
        },
        location=REGION,
    )

    # ── 5. dbt tests ──────────────────────────────────────────────────────────
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_DIR} && dbt test --select silver gold "
            "--profiles-dir /home/airflow/gcs/dags/dbt"
        ),
    )

    check_raw_data >> spark_etl >> dbt_silver >> bq_gold >> dbt_test
