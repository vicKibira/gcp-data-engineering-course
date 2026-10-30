"""
Phase 06 — LAB 6B: Cloud Composer DAG — Weekly ML Training Pipeline
Triggers the Vertex AI KFP training pipeline on a schedule.
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    RunPipelineJobOperator,
)
from airflow.utils.dates import days_ago


PROJECT_ID = "{{ var.value.project_id }}"
REGION     = "{{ var.value.region }}"
DATASET    = "{{ var.value.bq_dataset }}"
PIPELINE_ROOT = "{{ var.value.vertex_staging_bucket }}/pipelines"


default_args = {
    "owner":            "victor",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),
}

with DAG(
    dag_id="weekly_ml_training",
    description="Weekly: refresh BQ features → trigger Vertex AI training pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * 1",   # 03:00 UTC every Monday
    start_date=days_ago(7),
    catchup=False,
    tags=["phase-06", "ml", "training"],
    max_active_runs=1,
) as dag:

    # ── 1. Refresh ML feature table ───────────────────────────────────────────
    refresh_features = BigQueryInsertJobOperator(
        task_id="refresh_ml_features",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.ml_features_churn` AS
                    WITH user_stats AS (
                      SELECT
                        user_id,
                        COUNT(DISTINCT event_id)                               AS total_orders,
                        SUM(total)                                             AS lifetime_value,
                        AVG(total)                                             AS avg_order_value,
                        DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY)        AS days_since_last_order,
                        DATE_DIFF(MAX(event_date), MIN(event_date), DAY)       AS customer_lifespan_days,
                        COUNTIF(event_type = 'ORDER_CANCELLED') /
                          NULLIF(COUNT(DISTINCT event_id), 0)                  AS cancellation_rate,
                        COUNT(DISTINCT category)                               AS category_diversity
                      FROM `{PROJECT_ID}.{DATASET}.silver_events`
                      GROUP BY user_id
                    )
                    SELECT *, IF(days_since_last_order > 60, 1, 0) AS churned
                    FROM user_stats WHERE total_orders >= 2
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # ── 2. Launch Vertex AI KFP pipeline ─────────────────────────────────────
    run_training_pipeline = RunPipelineJobOperator(
        task_id="run_kfp_training_pipeline",
        project_id=PROJECT_ID,
        region=REGION,
        display_name=f"churn-training-{{{{ ds }}}}",
        template_path=f"{PIPELINE_ROOT}/churn_pipeline.json",
        parameter_values={
            "project":       PROJECT_ID,
            "region":        REGION,
            "dataset":       DATASET,
            "pipeline_root": PIPELINE_ROOT,
            "run_date":      "{{ ds }}",
        },
        pipeline_root=PIPELINE_ROOT,
    )

    refresh_features >> run_training_pipeline
