# Phase 06 — Orchestration (Cloud Composer + Airflow)

## Why this phase exists

You now have five independent scripts: a batch ingest, a Spark ETL, dbt models, a BigQuery Gold query, and a Vertex AI training pipeline. Running each manually in order every day is not a data platform — it's a to-do list.

**Orchestration** is what turns a collection of scripts into a pipeline. It adds:
- **Scheduling** — run at 02:00 UTC daily, not whenever you remember
- **Dependency ordering** — dbt Silver only runs after Spark ETL succeeds
- **Retries** — if BigQuery has a transient error, retry in 5 minutes automatically
- **Observability** — a visual DAG graph showing exactly where a failure happened
- **Backfilling** — re-process a specific date range if upstream data was fixed

**Cloud Composer** is managed Apache Airflow on GCP. You write Python DAG files; Composer handles the scheduler, workers, metadata database, and web UI. The DAG files live in a GCS bucket that Composer automatically syncs.

---

## What you'll build

- A **daily ingestion DAG** (`daily_ingestion_pipeline`) that runs the full Bronze → Silver → Gold pipeline every night
- A **weekly ML training DAG** (`weekly_ml_training`) that refreshes features and triggers the KFP training pipeline every Monday

---

## Files

### `dags/ingestion_dag.py`

A five-task Airflow DAG that orchestrates the full data pipeline:

```
check_raw_data → spark_etl → dbt_silver → bq_gold_daily_revenue → dbt_test
```

**Why this order?**
- `check_raw_data` — fails fast if upstream ingest didn't run (no point launching a Dataproc cluster for an empty input)
- `spark_etl` — Bronze → Silver in Parquet (must precede dbt)
- `dbt_silver` — loads Parquet into BQ Silver (must precede Gold aggregation)
- `bq_gold_daily_revenue` — aggregates Silver into Gold (daily summary row)
- `dbt_test` — validates data quality after the full chain (alerts before dashboards read bad data)

**Key patterns:**

**Airflow Variables (environment-agnostic config):**
```python
PROJECT_ID = "{{ var.value.project_id }}"
REGION     = "{{ var.value.region }}"
```
Variables are set in the Composer UI (Admin → Variables), not hardcoded in the DAG. The same DAG file runs unchanged in dev, staging, and prod — only the variable values differ.

**Jinja date templating:**
```python
# {{ ds }} = execution date in YYYY-MM-DD format
# macros.ds_format() extracts year/month/day components
prefix="raw/sales/year={{ macros.ds_format(ds, '%Y-%m-%d', '%Y') }}/..."
```
`{{ ds }}` is Airflow's execution date — the logical date the run is processing, not the wall clock time. For the daily 02:00 UTC run, `{{ ds }}` is yesterday's date. This is how you process "yesterday's data" without hardcoding dates.

**DataprocSubmitJobOperator:**
```python
DataprocSubmitJobOperator(
    job={
        "placement": {"cluster_name": CLUSTER},
        "pyspark_job": {
            "main_python_file_uri": f"gs://{BUCKET}/scripts/spark_etl.py",
            "args": ["--date", "{{ ds }}"],
        },
    }
)
```
This submits the Phase 03 Spark ETL job to the running Dataproc cluster. The operator blocks until the job completes — success or failure propagates to the task status.

**BigQueryInsertJobOperator** — runs the Gold aggregation SQL directly from the DAG. The double-brace `{{{{ ds }}}}` escapes Airflow's Jinja templating inside a Python f-string: the f-string renders first (removing the outer `{}` layer), leaving `{{ ds }}` for Airflow's Jinja pass.

**DAG-level config:**
```python
schedule_interval="0 2 * * *"  # 02:00 UTC daily
catchup=False                   # don't backfill when first activated
max_active_runs=1               # prevent concurrent runs on the same cluster
```
`max_active_runs=1` prevents two pipeline runs from submitting to the same Dataproc cluster simultaneously.

---

### `dags/training_dag.py`

A two-task DAG that runs every Monday morning:

```
refresh_ml_features → run_kfp_training_pipeline
```

**Task 1 — Feature refresh (`BigQueryInsertJobOperator`):**
Rebuilds `ml_features_churn` as a `CREATE OR REPLACE TABLE`. Why full rebuild instead of incremental?
- Churn labels change over time: a user who was 55 days inactive last week crosses the 60-day threshold this week
- Feature definitions may change between model iterations
- The table is small (one row per user) so full rebuild is fast

**Task 2 — KFP pipeline (`RunPipelineJobOperator`):**
```python
RunPipelineJobOperator(
    template_path=f"{PIPELINE_ROOT}/churn_pipeline.json",
    parameter_values={"run_date": "{{ ds }}", ...},
)
```
This submits the compiled KFP pipeline JSON (from Phase 08) to Vertex AI Pipelines and polls until it completes. The pipeline JSON must already be compiled and uploaded to GCS before this DAG runs.

**Why separate DAGs instead of one big DAG?**
- Different schedules: ingestion runs daily, training runs weekly
- Independent failure domains: a bad training run doesn't block tomorrow's ingestion
- Easier to debug: the Airflow UI shows two simple graphs instead of one complex one

---

## Prerequisites

- Phase 01–05 complete (infrastructure, data pipeline, BQ tables, KFP pipeline JSON from Phase 08)
- Cloud Composer environment created:
  ```bash
  gcloud composer environments create course-composer \
    --location us-central1 \
    --image-version composer-3-airflow-2.7.3 \
    --service-account $COMPOSER_SA
  ```
- Airflow Variables set (see Lab steps below)

---

## Lab — Step by step

### Step 1 — Create the Composer environment
```bash
export COMPOSER_ENV=course-composer
export LOCATION=us-central1

gcloud composer environments create $COMPOSER_ENV \
  --location $LOCATION \
  --image-version composer-3-airflow-2.7.3 \
  --service-account $COMPOSER_SA \
  --network $VPC_NAME \
  --subnetwork $VPC_NAME-subnet
```
This takes 15–20 minutes. Go get coffee.

### Step 2 — Get the DAGs bucket
```bash
COMPOSER_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV \
  --location $LOCATION \
  --format="value(config.dagGcsPrefix)")
echo "DAGs bucket: $COMPOSER_BUCKET"
```

### Step 3 — Set Airflow Variables
```bash
# Set each variable in the Composer environment
for VAR_PAIR in \
  "project_id=$PROJECT_ID" \
  "region=us-central1" \
  "bucket_name=$BUCKET_NAME" \
  "bq_dataset=course_dataset" \
  "vertex_staging_bucket=gs://$BUCKET_NAME"; do
  KEY="${VAR_PAIR%%=*}"
  VAL="${VAR_PAIR#*=}"
  gcloud composer environments run $COMPOSER_ENV \
    --location $LOCATION variables set -- "$KEY" "$VAL"
done
```

### Step 4 — Upload the DAGs
```bash
gsutil cp phase-06-orchestration/dags/ingestion_dag.py $COMPOSER_BUCKET/
gsutil cp phase-06-orchestration/dags/training_dag.py  $COMPOSER_BUCKET/
```
Composer polls the GCS bucket every 30 seconds. The DAGs appear in the Airflow UI within a minute.

### Step 5 — Upload the dbt project to Composer
```bash
# Copy dbt project into the Composer DAGs folder so BashOperators can find it
gsutil -m rsync -r phase-05-bigquery/dbt/ $COMPOSER_BUCKET/dbt/
```

### Step 6 — Open the Airflow UI
```bash
gcloud composer environments describe $COMPOSER_ENV \
  --location $LOCATION \
  --format="value(config.airflowUri)"
```
Open the URL. You'll see both DAGs listed.

### Step 7 — Trigger a manual test run
```bash
# Trigger ingestion pipeline for a specific date
gcloud composer environments run $COMPOSER_ENV \
  --location $LOCATION \
  dags trigger -- daily_ingestion_pipeline \
  --conf '{"ds": "2024-01-15"}'
```
Or click "Trigger DAG" in the Airflow UI and set the date manually.

### Step 8 — Monitor the run
In the Airflow UI:
1. Click `daily_ingestion_pipeline`
2. Click the run instance (circle in the calendar view)
3. Click each task to see logs
4. Green = success, Red = failed, Yellow = running

### Step 9 — Check the final output
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$PROJECT_ID.course_dataset.gold_daily_revenue\`
   WHERE report_date = '2024-01-15'"
```

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `gold_daily_revenue` rows (refreshed daily) | BI dashboards, Phase 07 model input |
| `ml_features_churn` rows (refreshed weekly) | Phase 07 AutoML + custom training |
| Vertex AI KFP pipeline trigger | Phase 08 (the pipeline JSON Composer submits) |

---

## Key concepts covered

- **DAG** — Directed Acyclic Graph; the dependency map of tasks in Airflow
- **Airflow Variables** — runtime configuration that keeps DAG code environment-agnostic
- **Jinja templating** — `{{ ds }}`, `{{ macros.ds_format() }}` for dynamic date injection
- **Operator types** — DataprocSubmitJobOperator, BigQueryInsertJobOperator, BashOperator, RunPipelineJobOperator
- **`>>` chaining** — Airflow's dependency syntax (A >> B = B depends on A)
- **`catchup=False`** — preventing backfill runs when a DAG is first enabled
- **`max_active_runs=1`** — preventing concurrent pipeline runs on shared resources
- **Separate DAG files** — decoupling daily and weekly schedules for independent failure domains
- **Cross-DAG dependencies** — ExternalTaskSensor (mentioned: use to explicitly link DAGs)
