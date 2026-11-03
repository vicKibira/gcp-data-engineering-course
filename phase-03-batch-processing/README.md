# Phase 03 — Batch Processing (Dataproc + Spark)

## Why this phase exists

The Bronze zone has raw data but it's messy — wrong types, duplicate records from publisher retries, nulls in critical fields. You cannot hand that to analysts or train an ML model on it directly.

This phase does the **Bronze → Silver** transformation using PySpark on Dataproc. Spark is the right tool because:
- The raw files can be hundreds of GB — you need a distributed compute cluster, not a single machine
- Spark processes data in parallel across all cluster workers simultaneously
- Adaptive Query Execution (AQE) automatically optimises join and shuffle strategies at runtime
- You pay per cluster-hour: spin up, process, tear down — no idle infrastructure cost

This is the most common workload in enterprise data engineering. If you can write a Spark ETL job and submit it to a managed cluster, you can work at any company running a modern data platform.

---

## What you'll build

- A Dataproc cluster (1 master + 2 workers) running Spark 3.3
- A PySpark ETL job that cleans and enriches daily sales data
- Silver-zone Parquet files under `gs://BUCKET/processed/`

---

## Files

### `spark_etl.py`
The full Bronze → Silver transformation pipeline. Four functions, each with a single responsibility:

**`build_spark()`** — Creates the SparkSession with Adaptive Query Execution enabled:
```python
SparkSession.builder
  .config("spark.sql.adaptive.enabled", "true")
  .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
  .getOrCreate()
```
AQE rewrites the physical execution plan after each shuffle stage using actual data statistics. On Dataproc this typically cuts shuffle sizes by 30–60% for skewed datasets. `coalescePartitions` merges small output partitions so you don't end up with 200 tiny files.

**`read_raw()`** — Reads Bronze Parquet for a specific date:
```python
path = f"gs://{bucket}/raw/{dataset}/year={year}/month={month}/day={day}/*.parquet"
spark.read.parquet(path)
```
The `*` wildcard reads all files in the partition. Spark pushes the partition filter to the GCS file listing — only that day's files are opened.

**`transform_sales()`** — Applies five transformation steps in a single DataFrame chain:
1. Rename `order_date` → `event_date` (standardise schema across datasets)
2. Cast `total` to `DoubleType`, `quantity` to `integer` (enforce types)
3. Add `unit_price = total / quantity` (derived metric for per-unit analytics)
4. Add `revenue_band`: `< $50 → low`, `$50–$199 → medium`, `$200+ → high` (BI filter column)
5. Add `processed_at = current_timestamp()` (audit trail)
6. `dropDuplicates(["event_id"])` — Pub/Sub guarantees at-least-once, so retries create duplicates; we keep only the first
7. `dropna(["event_id", "user_id", "product_id", "total"])` — rows missing these fields are not analytically useful

All these steps are **lazy** in Spark — nothing runs until `write_silver()` triggers execution. This lets Spark build an optimised physical plan across all steps at once.

**`write_silver()`** — Writes the cleaned DataFrame:
```python
df.repartition(4).write.mode("overwrite").parquet(path)
```
`repartition(4)` consolidates to 4 output files (tune per daily data volume). `mode("overwrite")` makes re-running the same date idempotent — Bronze is never touched.

---

### `submit_dataproc.sh`
Handles cluster creation and job submission in one script:

1. **Cluster check** — `gcloud dataproc clusters describe` exits non-zero if the cluster doesn't exist; the `if !` pattern creates it only when needed
2. **Script upload** — `gsutil cp spark_etl.py gs://BUCKET/scripts/` so Dataproc workers download the latest version at job time
3. **Job submission** — the bare `--` separator tells gcloud everything after it is a job argument to `spark_etl.py`, not a gcloud flag

**Cluster flags explained:**
```bash
--num-workers=2              # 1 master + 2 workers (minimum for distributed jobs)
--worker-machine-type=n2-standard-2  # 2 vCPU, 8 GB RAM per worker
--image-version=2.1-debian11         # Spark 3.3 + Python 3.10
--optional-components=JUPYTER        # adds Jupyter for interactive exploration
--enable-component-gateway           # exposes Spark UI via browser (no VPN needed)
```

---

## Prerequisites

- Phase 01 complete (GCS bucket, VPC, `sa-dataproc` service account exist)
- Phase 02 complete (Bronze Parquet files exist at `gs://BUCKET/raw/sales/...`)
- `.env` with `PROJECT_ID`, `BUCKET_NAME`, `REGION`

---

## Lab — Step by step

### Step 1 — Load your environment
```bash
source .env
cd phase-03-batch-processing
```

### Step 2 — Submit the ETL job
```bash
# Process yesterday's date (default)
bash submit_dataproc.sh

# Or process a specific date
bash submit_dataproc.sh 2024-01-15
```

The script will:
1. Create the `course-cluster` if it doesn't exist (~3 minutes)
2. Upload `spark_etl.py` to GCS
3. Submit the job and stream logs to your terminal

### Step 3 — Watch the Spark UI
While the job runs, open the **Component Gateway** URL printed by the cluster creation step. Go to **Jobs → Your Job ID** to see the Spark DAG visualisation — you'll see the transformation stages and how work splits across workers.

### Step 4 — Verify output
```bash
gsutil ls gs://$BUCKET_NAME/processed/sales/
# Expected: year=2024/month=01/day=15/  (or whatever date you processed)

gsutil ls gs://$BUCKET_NAME/processed/sales/year=2024/month=01/day=15/
# Expected: 4 Parquet files (from repartition(4))
```

### Step 5 — Check row counts
The job prints before/after counts in the logs:
```
[transform] 5,280 → 5,201 rows after cleaning
```
The difference = duplicates removed + nulls dropped. That's your data quality signal.

### Step 6 — (Optional) Tear down the cluster
```bash
gcloud dataproc clusters delete course-cluster --region=us-central1 --quiet
```
In production you'd do this automatically. The cluster costs money while idle.

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `gs://BUCKET/processed/sales/year=.../.../*.parquet` | Phase 05 (BigQuery external table or BQ Load job reads Silver Parquet) |
| `course-cluster` Dataproc cluster | Phase 06 (Composer DAG submits Dataproc jobs via `DataprocSubmitJobOperator`) |

The Silver Parquet files are the primary input to Phase 05's `silver_events` BigQuery table via a `bq load` command or the Composer-orchestrated load job.

---

## Key concepts covered

- **Medallion Architecture: Bronze → Silver** — why raw data is never directly consumed
- **Distributed computing** — Spark splits data across cluster workers in parallel
- **Lazy evaluation** — Spark builds a full execution plan before running anything
- **Adaptive Query Execution (AQE)** — runtime plan optimisation for skewed data
- **Idempotent writes** — `mode("overwrite")` makes re-runs safe
- **At-least-once deduplication** — `dropDuplicates(["event_id"])` handles Pub/Sub retries
- **Data quality enforcement** — `dropna` and type casting at the Silver boundary
- **Spark partitioning** — `repartition(N)` controlling output file count
