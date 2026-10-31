"""
Phase 06 — LAB 6B: Cloud Composer DAG — Weekly ML Training Pipeline
====================================================================
Orchestrates the weekly ML model re-training on a Monday morning schedule.
Refreshes the BigQuery feature table from the latest silver data, then
triggers the Vertex AI Kubeflow Pipelines (KFP) training pipeline.

DAG schedule: 03:00 UTC every Monday (after the previous day's ingestion
pipeline has finished and silver data is fresh).

Task graph:
  refresh_ml_features     (BQ: rebuild churn feature table from silver_events)
       │
  run_kfp_training_pipeline (Vertex AI: launch the KFP pipeline from phase-08)

Key concepts:
  - RunPipelineJobOperator: triggers a pre-compiled KFP pipeline JSON template
    stored in GCS. The pipeline runs asynchronously in Vertex AI; this operator
    waits for it to complete (blocking task).
  - Weekly cadence balances freshness (enough new data to matter) with cost
    (Vertex AI training incurs per-node-hour charges).
  - The DAG is independent of ingestion_dag.py — it doesn't read from that
    DAG's XCom or share state. Use Airflow's ExternalTaskSensor if you need
    explicit cross-DAG dependencies.

Airflow Variables needed:
  project_id              — GCP project ID
  region                  — GCP region
  bq_dataset              — BigQuery dataset with silver_events and ml_features_churn
  vertex_staging_bucket   — GCS path for Vertex AI pipeline artifacts (gs://BUCKET/path)
"""
from datetime import timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.vertex_ai.pipeline_job import (
    RunPipelineJobOperator,
)
from airflow.utils.dates import days_ago


# ── Configuration ─────────────────────────────────────────────────────────────
# Airflow Variables — set these in the Composer UI (Admin → Variables).
PROJECT_ID    = "{{ var.value.project_id }}"
REGION        = "{{ var.value.region }}"
DATASET       = "{{ var.value.bq_dataset }}"
PIPELINE_ROOT = "{{ var.value.vertex_staging_bucket }}/pipelines"


# ── Default task arguments ─────────────────────────────────────────────────────
default_args = {
    "owner":            "victor",
    "depends_on_past":  False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=10),   # longer delay: BQ/Vertex errors are usually transient
}

with DAG(
    dag_id="weekly_ml_training",
    description="Weekly: refresh BQ features → trigger Vertex AI training pipeline",
    default_args=default_args,
    schedule_interval="0 3 * * 1",   # cron: 03:00 UTC on Mondays (day-of-week = 1)
    start_date=days_ago(7),
    catchup=False,                   # skip missed weeks; always train on current data
    tags=["phase-06", "ml", "training"],
    max_active_runs=1,               # prevent concurrent training runs (expensive)
) as dag:

    # ── Task 1: Refresh ML feature table ─────────────────────────────────────
    # Re-creates the ml_features_churn table from scratch using the latest silver_events.
    # Full rebuild (CREATE OR REPLACE) is correct here because:
    #   - Feature definitions may change between weeks
    #   - Historical churn labels change over time (a user inactive 55 days last week
    #     becomes churned=1 this week after crossing the 60-day threshold)
    #   - The table is small (~1 row/user × 10k users = fast to rebuild)
    #
    # This is the same SQL as 03_bqml_model.sql Step 1 — the DAG automates it.
    refresh_features = BigQueryInsertJobOperator(
        task_id="refresh_ml_features",
        configuration={
            "query": {
                "query": f"""
                    -- Rebuild churn feature table from latest silver data
                    -- Full refresh (not incremental) because churn labels change over time
                    CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.ml_features_churn` AS
                    WITH user_stats AS (
                      SELECT
                        user_id,
                        COUNT(DISTINCT event_id)                               AS total_orders,
                        SUM(total)                                             AS lifetime_value,
                        AVG(total)                                             AS avg_order_value,
                        DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY)        AS days_since_last_order,
                        DATE_DIFF(MAX(event_date), MIN(event_date), DAY)       AS customer_lifespan_days,
                        -- Cancellation rate: what fraction of orders were cancelled?
                        COUNTIF(event_type = 'ORDER_CANCELLED') /
                          NULLIF(COUNT(DISTINCT event_id), 0)                  AS cancellation_rate,
                        COUNT(DISTINCT category)                               AS category_diversity
                      FROM `{PROJECT_ID}.{DATASET}.silver_events`
                      GROUP BY user_id
                    )
                    -- Label: churned=1 if last order was >60 days ago
                    -- Exclude one-time buyers (total_orders < 2) — no "return" behaviour to model
                    SELECT *, IF(days_since_last_order > 60, 1, 0) AS churned
                    FROM user_stats WHERE total_orders >= 2
                """,
                "useLegacySql": False,
            }
        },
        location=REGION,
    )

    # ── Task 2: Launch Vertex AI KFP pipeline ─────────────────────────────────
    # RunPipelineJobOperator submits a compiled KFP pipeline (JSON) to Vertex AI
    # and polls until it completes (blocks the task until the pipeline finishes).
    # The pipeline JSON was compiled by pipeline.py (phase-08) and uploaded
    # to GCS at PIPELINE_ROOT/churn_pipeline.json.
    #
    # parameter_values map to the @dsl.pipeline function arguments in pipeline.py.
    # display_name uses {{ ds }} to make each run uniquely identifiable in the
    # Vertex AI console (search by date).
    run_training_pipeline = RunPipelineJobOperator(
        task_id="run_kfp_training_pipeline",
        project_id=PROJECT_ID,
        region=REGION,
        # Include the execution date in the display name for easy filtering in the console
        display_name=f"churn-training-{{{{ ds }}}}",
        # Path to the compiled KFP pipeline JSON; must be uploaded before the DAG runs
        template_path=f"{PIPELINE_ROOT}/churn_pipeline.json",
        parameter_values={
            "project":       PROJECT_ID,
            "region":        REGION,
            "dataset":       DATASET,
            "pipeline_root": PIPELINE_ROOT,
            "run_date":      "{{ ds }}",   # execution date for traceability
        },
        pipeline_root=PIPELINE_ROOT,      # where Vertex AI writes pipeline artifacts
    )

    # ── Task dependency ───────────────────────────────────────────────────────
    # Feature refresh must complete before training starts.
    # Training uses the features table as its data source.
    refresh_features >> run_training_pipeline
