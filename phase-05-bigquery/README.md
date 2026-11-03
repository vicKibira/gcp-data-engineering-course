# Phase 05 — BigQuery Mastery

## Why this phase exists

You've got data landing in BigQuery from Dataflow (streaming) and Spark (batch). Now you need to make it usable for analysts, BI tools, and ML models. Raw data is never directly queryable at scale — it has wrong types, no business logic applied, and no pre-aggregation for dashboards.

This phase covers **three layers of data transformation in BigQuery**:

1. **SQL DDL + analytics** — define the Bronze/Silver/Gold tables and write production-grade analytical queries
2. **dbt** — manage SQL transformations as versioned, tested, documented code
3. **BigQuery ML (BQML)** — train and score a churn prediction model entirely in SQL

Together these represent the Medallion Architecture fully implemented: raw data comes in at Bronze, gets cleaned into Silver, gets aggregated into Gold, and the Gold features feed the ML pipeline.

---

## What you'll build

- `bronze_events` — partitioned raw event table (populated by Dataflow)
- `silver_events` — validated, typed, deduplicated, enriched table
- `gold_daily_revenue` — pre-aggregated revenue metrics for dashboards
- `ml_features_churn` — per-user behavioural features for ML training
- `model_churn_v1` — BQML logistic regression churn model
- `churn_scores` — scored predictions for every user
- dbt models that build the Bronze → Silver → Gold pipeline as SQL

---

## Files

### `sql/01_create_tables.sql`
DDL for all three Medallion layers. Key design decisions:

**Partitioning and clustering:**
```sql
-- Bronze: partition by TIMESTAMP (event happens at any time of day)
PARTITION BY DATE(timestamp)
CLUSTER BY event_type, category

-- Silver: partition by DATE (queries always filter whole days)
PARTITION BY event_date
CLUSTER BY event_type, category, revenue_band
```
Partitioning cuts the bytes scanned for any date-filtered query (you only pay for what BigQuery reads). Clustering sorts data within partitions to speed up `GROUP BY` and `WHERE` on the cluster keys — no extra cost.

**Why Silver uses `DATE` partition instead of `TIMESTAMP`?**
Dashboard queries always say `WHERE event_date = '2024-01-15'` — a whole day. DATE partitions are simpler and BigQuery can use them in `BETWEEN` range predicates more efficiently than TIMESTAMP.

---

### `sql/02_analytics_queries.sql`
Four progressively complex query patterns:

**Query 1 — Revenue by category (last 30 days):**
Standard aggregation with `COUNT(DISTINCT event_id)` — distinct count avoids double-counting if any retries slipped through. `DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)` is the idiomatic BQ date range pattern.

**Query 2 — User cohort retention:**
Uses three CTEs chained together. The `first_order` CTE finds each user's acquisition month. `orders_with_cohort` joins all orders back to their cohort. The final SELECT counts active users per `(cohort_month, month_number)` cell. This builds the classic retention curve — you need this to know if your product has a user retention problem.

**Query 3 — 7-day rolling revenue (window functions):**
```sql
AVG(SUM(total)) OVER (ORDER BY event_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
```
Two levels of aggregation: `SUM(total)` groups by date, then `AVG(...) OVER (...)` applies a 7-day rolling average across the grouped result. Rolling averages smooth out weekend dips so you can see real trends.

**Query 4 — Gold layer materialisation:**
The `INSERT INTO gold_daily_revenue` that runs nightly via Composer (Phase 06). Processes only `event_date = CURRENT_DATE() - 1 day` — the incremental load pattern. Historical rows are never overwritten.

---

### `sql/03_bqml_model.sql`
Five steps that take you from raw events to a deployed churn prediction model using only SQL:

**Step 1 — Feature engineering:**
```sql
CREATE OR REPLACE TABLE ml_features_churn AS
SELECT user_id,
  COUNT(DISTINCT event_id) AS total_orders,
  DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) AS days_since_last_order,
  COUNTIF(event_type = 'ORDER_CANCELLED') / NULLIF(COUNT(DISTINCT event_id), 0) AS cancellation_rate,
  IF(days_since_last_order > 60, 1, 0) AS churned  -- binary label
...
```
`NULLIF(..., 0)` prevents division-by-zero for new users with no cancellations. `days_since_last_order > 60` is the churn label — users inactive for 60 days are considered churned.

**Step 2 — Train the model:**
```sql
CREATE OR REPLACE MODEL model_churn_v1
OPTIONS (
  model_type='LOGISTIC_REG',
  auto_class_weights=TRUE,   -- compensates for churned minority class
  model_registry='vertex_ai' -- registers to Vertex AI Model Registry
)
AS SELECT ... FROM ml_features_churn
WHERE MOD(ABS(FARM_FINGERPRINT(user_id)), 10) < 8;  -- deterministic 80% train split
```
`FARM_FINGERPRINT(user_id)` hashes each user to an integer deterministically — the same user always ends up in the same split across re-runs (no data leakage between runs).

**Step 3 — Evaluate:**
`ML.EVALUATE` scores the model on the 20% held-out users. Target: AUC-ROC > 0.75.

**Step 4 — Score all users:**
`ML.PREDICT` runs inference. `predicted_churned_probs[OFFSET(1)].prob` extracts the probability for the positive class (churned=1). Results sorted by `churn_probability DESC` — CRM teams contact highest-risk users first.

**Step 5 — Feature importance:**
`ML.FEATURE_IMPORTANCE` shows which features matter most. Use to drop low-signal features and explain predictions to business stakeholders.

---

### `dbt/` — dbt models

**Why dbt on top of SQL?**
Raw SQL files in a folder work until you have 20 analysts each writing their own version of "monthly revenue." dbt gives you:
- Dependency management: `{{ ref('bronze_events') }}` automatically builds Bronze before Silver
- Incremental loading: the Silver model only processes new rows since the last run
- Testing: `schema.yml` defines constraints (not null, unique, accepted values) that run with `dbt test`
- Documentation: auto-generated lineage graph
- Version control: SQL-as-code in Git

**`bronze_events.sql`** — A `view` materialization over the raw BigQuery table. Zero storage cost; always reflects live streaming data. Exists to put Bronze into the dbt DAG so lineage is visible.

**`silver_events.sql`** — An `incremental` materialization. On first run, processes all rows. On subsequent runs, only processes rows where `ingested_at > (last run time)`. Applies the same cleaning logic as the Spark ETL: dedup, type casting, derived columns.

**`gold_daily_revenue.sql`** — A `table` materialization. Rebuilt daily by Composer. Aggregates Silver into the pre-computed revenue metrics that BI dashboards read.

**`profiles.yml`** — Tells dbt where to write results:
```yaml
course_project:
  target: dev
  outputs:
    dev:
      type: bigquery
      method: oauth
      project: "{{ env_var('PROJECT_ID') }}"
      dataset: course_dataset
      location: US
```
Uses `env_var('PROJECT_ID')` so the same profile works across environments without hardcoding.

---

## Prerequisites

- Phase 01 complete (BigQuery dataset exists)
- Phase 04 complete or at least one run (some `bronze_events` rows exist)
- `bq` CLI authenticated
- dbt: `pip install dbt-bigquery`

---

## Lab — Step by step

### Part A — Create tables (SQL DDL)

#### Step 1 — Run the DDL
```bash
# Replace placeholders and run
sed "s/\${PROJECT}/$PROJECT_ID/g; s/\${DATASET}/course_dataset/g" \
  phase-05-bigquery/sql/01_create_tables.sql | bq query --use_legacy_sql=false
```

#### Step 2 — Verify tables exist
```bash
bq ls $PROJECT_ID:course_dataset
# Should show: bronze_events, silver_events, gold_daily_revenue
```

---

### Part B — Run analytics queries

#### Step 1 — Revenue by category
```bash
sed "s/\${PROJECT}/$PROJECT_ID/g; s/\${DATASET}/course_dataset/g" \
  phase-05-bigquery/sql/02_analytics_queries.sql | \
  head -20 | bq query --use_legacy_sql=false
```

#### Step 2 — Explore in the BQ console
Open the BQ console, run the cohort and rolling window queries manually. Use the **Execution details** tab to see bytes processed and partition pruning in action.

---

### Part C — dbt pipeline

#### Step 1 — Configure dbt
```bash
cd phase-05-bigquery/dbt
export PROJECT_ID=$PROJECT_ID
dbt debug  # verifies connection to BigQuery
```

#### Step 2 — Install dependencies
```bash
dbt deps  # installs packages from packages.yml
```

#### Step 3 — Run the full pipeline
```bash
dbt run
# Runs: bronze_events (view) → silver_events (incremental) → gold_daily_revenue (table)
```

#### Step 4 — Run tests
```bash
dbt test
# Tests: not_null, unique, accepted_values constraints from schema.yml
```

#### Step 5 — View lineage
```bash
dbt docs generate
dbt docs serve  # opens a browser with the DAG and table documentation
```

---

### Part D — Train a BQML churn model

#### Step 1 — Create features + train model
```bash
sed "s/\${PROJECT}/$PROJECT_ID/g; s/\${DATASET}/course_dataset/g" \
  phase-05-bigquery/sql/03_bqml_model.sql | bq query --use_legacy_sql=false
```
Training takes 2–5 minutes. You'll see the model appear in the BQ console under your dataset.

#### Step 2 — Check evaluation metrics
The evaluation query prints AUC-ROC, precision, recall. Target AUC > 0.75.

#### Step 3 — Check the scored output
```bash
bq query --use_legacy_sql=false \
  "SELECT user_id, churn_probability FROM \`$PROJECT_ID.course_dataset.churn_scores\`
   ORDER BY churn_probability DESC LIMIT 10"
```
These are your highest-risk users. A real CRM would send these to a retention campaign.

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `silver_events` BigQuery table | Phase 06 (Composer Gold refresh query), Phase 07 (BQML + custom training reads features) |
| `ml_features_churn` table | Phase 07 (AutoML training dataset), Phase 07 (XGBoost `load_data()`) |
| `gold_daily_revenue` table | Phase 06 (Composer refreshes it nightly) |
| `model_churn_v1` BQML model | Phase 07 (baseline to compare AutoML and custom training against) |

---

## Key concepts covered

- **Medallion Architecture in BigQuery** — Bronze / Silver / Gold layer purposes and patterns
- **Partitioning + clustering** — the two tools for reducing query cost in BigQuery
- **Window functions** — `OVER (ORDER BY ... ROWS BETWEEN ...)` for rolling analytics
- **Cohort analysis** — CTE chaining for user retention curves
- **dbt incremental materialisation** — only processing new records on each run
- **dbt testing** — not_null, unique, accepted_values in schema.yml
- **BQML** — full ML workflow (feature engineering → train → evaluate → score) in SQL
- **FARM_FINGERPRINT for deterministic splits** — reproducible train/test without data leakage
- **Class imbalance handling** — `auto_class_weights` in BQML, `scale_pos_weight` in XGBoost
