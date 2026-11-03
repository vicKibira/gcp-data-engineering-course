# Phase 10 — Capstone (Full Platform Deployment)

## Why this phase exists

You have ten separate phase folders, each with its own scripts, SQL, and Terraform config. Deploying a complete platform on a fresh GCP project by hand requires running the right commands in the right order, remembering which phases depend on which, and catching missing variables before they cause failures halfway through a 45-minute Terraform apply.

`deploy_all.sh` automates the automatable parts into a single idempotent command. It also documents — permanently, as executable code — exactly which phases can be deployed programmatically and which require manual steps (and why). That distinction matters when you're onboarding a new team member or reproducing the platform in a new GCP project.

---

## What you'll build

Running `bash phase-10-capstone/deploy_all.sh` on a fresh GCP project:

1. Enables all required APIs and provisions IAM, GCS, BigQuery, VPC, and Artifact Registry via Terraform
2. Creates the Pub/Sub topic and subscription for real-time event streaming
3. Uploads the PySpark ETL script to GCS (where Dataproc workers expect it)
4. Creates all BigQuery tables (Bronze, Silver, Gold, ML features) from DDL
5. Builds the XGBoost training Docker container and pushes it to Artifact Registry
6. Compiles the KFP churn training pipeline and submits the first Vertex AI training run

After the script exits, three manual steps remain (Dataflow streaming, Composer DAGs, Cloud Build CI/CD trigger) — each is printed as a next-steps banner with exact commands.

---

## Full platform architecture

```
┌─────────────────── Data Ingestion ─────────────────────┐
│                                                         │
│  E-commerce events                                      │
│  (Phase 02: pubsub_publisher.py)                        │
│         │                                               │
│         ▼                                               │
│  Pub/Sub topic: raw-events                              │
│         │                                               │
│         ▼                                               │
│  Dataflow / Apache Beam (Phase 04)                      │
│  beam_pipeline.py — validates, routes, dead-letters     │
│         │                                               │
│         ▼                                               │
│  GCS Bronze zone (raw Parquet files)                    │
│         │                                               │
│         ▼                                               │
│  BigQuery: bronze_events (Phase 05)                     │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────── Batch Transform ────────────────────┐
│                                                         │
│  Dataproc / PySpark (Phase 03)                          │
│  spark_etl.py — dedup, type-cast, partition             │
│         │                                               │
│         ▼                                               │
│  GCS Silver zone (processed Parquet)                    │
│         │                                               │
│         ▼                                               │
│  BigQuery: silver_events (Phase 05)                     │
│         │                                               │
│         ▼                                               │
│  dbt Silver + Gold (Phase 05)                           │
│  BigQuery: gold_daily_revenue, ml_features_churn        │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────── Orchestration ──────────────────────┐
│                                                         │
│  Cloud Composer / Airflow (Phase 06)                    │
│  Daily DAG: raw check → Spark → dbt → BQ Gold → test   │
│  Weekly DAG: feature refresh → KFP training pipeline   │
└─────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────── ML Platform ────────────────────────┐
│                                                         │
│  Vertex AI AutoML (Phase 07)                            │
│  Vertex AI Custom Training — XGBoost (Phase 07)         │
│         │                                               │
│         ▼                                               │
│  KFP v2 Pipeline (Phase 08)                             │
│  extract → preprocess → train → evaluate → register    │
│         │                                               │
│         ▼                                               │
│  Vertex AI Model Registry                               │
│         │                                               │
│         ▼                                               │
│  Cloud Build CI/CD (Phase 09)                           │
│  test → build → push → submit pipeline → canary deploy  │
│         │                                               │
│         ▼                                               │
│  Vertex AI Endpoint (5% canary → 100% production)      │
│  + Model Monitoring (skew/drift alerts)                 │
└─────────────────────────────────────────────────────────┘
```

**Infrastructure backbone (Phase 01):** Terraform provisions everything — IAM, GCS buckets, BigQuery dataset, VPC, Artifact Registry — before any data moves.

---

## Files

### `deploy_all.sh`

**The shell safety triplet at line 1:**
```bash
set -euo pipefail
```
- `-e` — exit immediately on any non-zero return code. Without this, a failed `terraform apply` prints an error and the script happily keeps going, running Phase 05 DDL against infrastructure that was never created.
- `-u` — treat unset variables as errors. A typo (`$PROJET_ID` instead of `$PROJECT_ID`) becomes a loud failure at line 1, not a silent empty string passed to Terraform.
- `-o pipefail` — if any command in a pipeline fails, the pipeline fails. Without this, `cmd | head` succeeds even if `cmd` fails (because `head` exits 0).

**Variable validation with `:? syntax`:**
```bash
: "${PROJECT_ID:?  → set PROJECT_ID in .env}"
: "${REGION:?      → set REGION in .env (e.g. us-central1)}"
: "${BUCKET_NAME:? → set BUCKET_NAME in .env (GCS bucket name, no gs:// prefix)}"
: "${BQ_DATASET:?  → set BQ_DATASET in .env (BigQuery dataset name)}"
```
`:` is the bash no-op command. `${VAR:?message}` expands to `$VAR` if set and non-empty, or exits with `message` if not. This validates all required variables before any GCP API calls are made — so you get a clear error at startup instead of a cryptic `gcloud` error 30 minutes in.

**Idempotent Phase 01 (Terraform):**
```bash
terraform init -input=false
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" ...
```
Terraform reads existing state. A re-run on an already-provisioned project compares desired config against live GCP state and only modifies resources that have drifted. `-auto-approve` skips the "yes/no" prompt — safe in a script, dangerous interactively.

**Idempotent Phase 02 (Pub/Sub):**
```bash
gcloud pubsub topics create "${PUBSUB_TOPIC}" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  topic already exists"
```
`gcloud pubsub topics create` exits non-zero if the topic exists. `2>/dev/null` suppresses the error output; `|| echo "already exists"` turns the failure into success. Without `|| echo`, `-e` would kill the script on re-run. This is the idiomatic shell pattern for "create if not exists."

**Phase 03 (PySpark script upload):**
```bash
gsutil cp "$SCRIPT_DIR/phase-03-batch-processing/spark_etl.py" \
  "gs://${BUCKET_NAME}/scripts/spark_etl.py"
```
`gsutil cp` overwrites silently — uploading the same file twice is harmless. Dataproc workers read the script from GCS at job submission time; uploading here ensures the Airflow DAG's `main_python_file_uri` path always exists.

**Phase 05 (BigQuery DDL with placeholder substitution):**
```bash
sed \
  -e "s/\${PROJECT}/$PROJECT_ID/g" \
  -e "s/\${DATASET}/$BQ_DATASET/g" \
  "$SCRIPT_DIR/phase-05-bigquery/sql/01_create_tables.sql" | \
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID"
```
`01_create_tables.sql` uses `${PROJECT}` and `${DATASET}` as placeholder strings. `sed` replaces them before piping to `bq`. The DDL uses `CREATE TABLE IF NOT EXISTS` — re-running on an existing dataset is safe.

**Phase 07 (Docker build and push):**
```bash
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
docker build -t "${REGISTRY}/churn-train:latest" ...
docker push "${REGISTRY}/churn-train:latest"
```
`gcloud auth configure-docker` installs a credential helper that makes `docker push` authenticate via the application default credentials. This must run before any `docker push` to Artifact Registry, including in CI/CD.

**Phase 08 (KFP compile and submit — async):**
```bash
python pipeline.py \
  --project="$PROJECT_ID" \
  --pipeline_root="${VERTEX_STAGING_BUCKET}/pipelines" \
  --run_date="capstone"
```
`pipeline.py` compiles `churn_pipeline.json` and calls `job.submit()` (non-blocking). The KFP training run starts in Vertex AI and continues **after `deploy_all.sh` exits**. Monitor it at the Vertex AI Pipelines console. The script uses `--run_date="capstone"` as the run label to distinguish the initial capstone deployment from date-tagged automated runs.

**Why phases 04, 06, and 09 are excluded:**

| Phase | Why it needs manual setup |
|---|---|
| Phase 04 (Dataflow) | Requires a running Pub/Sub publisher to have data to process. Starting an idle Dataflow job wastes ~$5/hour. Instructions in next-steps banner. |
| Phase 06 (Composer) | DAG deployment requires the Composer environment's GCS DAGs bucket path, which is only known after the environment is created (15–20 minute provisioning). Composer itself requires manual IAM configuration and VPC peer approval. |
| Phase 09 (Cloud Build) | GitHub repo must be connected to Cloud Build via the GCP console (OAuth flow) before a trigger can be created. Cannot be scripted without the GitHub App ID. |

**Next-steps banner:**
After deployment, the script prints a formatted banner with the exact commands to:
1. Start the Pub/Sub event publisher
2. Launch the Dataflow streaming pipeline
3. Check the Vertex AI Pipelines console
4. Create the Cloud Build GitHub trigger
5. Upload DAGs to the Composer bucket

This banner is the runbook for the three manual phases. It uses `$PROJECT_ID`, `$PUBSUB_TOPIC`, `$BUCKET_NAME`, and `$REGION` so the commands are copy-pasteable without edits.

---

## Prerequisites

```bash
# 1. Create your environment file
cp .env.example .env
# Edit .env and fill in:
#   PROJECT_ID=your-gcp-project-id
#   REGION=us-central1
#   BUCKET_NAME=your-bucket-name
#   BQ_DATASET=course_dataset

# 2. Authenticate to GCP
gcloud auth application-default login
gcloud auth login

# 3. Ensure Terraform is installed
terraform version  # needs 1.0+

# 4. Ensure Docker is running
docker info

# 5. Install Python dependencies
pip install kfp google-cloud-aiplatform
```

---

## Lab — Step by step

### Step 1 — Create the .env file
```bash
cd gcp-data-engineering-course

# Create from the template
cat > .env << 'EOF'
PROJECT_ID=your-gcp-project-id
REGION=us-central1
BUCKET_NAME=your-unique-bucket-name
BQ_DATASET=course_dataset
PUBSUB_TOPIC=raw-events
EOF
```

### Step 2 — Run the capstone deployment
```bash
bash phase-10-capstone/deploy_all.sh
```
Expected runtime: 15–25 minutes (Terraform apply is the longest step at ~10 minutes).

Watch for the phase-by-phase progress lines:
```
▶ Phase 01 — GCP Fundamentals (APIs + Terraform)
✓ Phase 01 complete
▶ Phase 02 — Data Ingestion (Pub/Sub setup)
...
╔══ ✓ Capstone deployment complete! ...╗
```

### Step 3 — Start the event stream (Phase 04 manual step)
```bash
# Terminal 1: run the publisher (keeps running, generates events)
python phase-02-data-ingestion/pubsub_publisher.py \
  --project=$PROJECT_ID \
  --topic=raw-events

# Terminal 2: start Dataflow streaming pipeline
python phase-04-stream-processing/beam_pipeline.py \
  --project=$PROJECT_ID \
  --runner=DataflowRunner \
  --region=us-central1 \
  --temp_location=gs://$BUCKET_NAME/temp \
  --subscription=projects/$PROJECT_ID/subscriptions/raw-events-sub
```

### Step 4 — Create Composer environment (Phase 06 manual step)
```bash
# This takes 15–20 minutes
gcloud composer environments create course-composer \
  --location us-central1 \
  --image-version composer-3-airflow-2.7.3

# Get the DAGs bucket
COMPOSER_BUCKET=$(gcloud composer environments describe course-composer \
  --location us-central1 \
  --format="value(config.dagGcsPrefix)")

# Upload DAGs
gsutil cp phase-06-orchestration/dags/*.py $COMPOSER_BUCKET/

# Set Airflow Variables (see Phase 06 README for full list)
gcloud composer environments run course-composer \
  --location us-central1 variables set -- project_id $PROJECT_ID
```

### Step 5 — Set up Cloud Build CI/CD (Phase 09 manual step)
1. In the GCP console: **Cloud Build → Triggers → Connect repository**
2. Authenticate with GitHub and select `gcp-data-engineering-course`
3. Create the trigger:
```bash
gcloud builds triggers create github \
  --repo-name=gcp-data-engineering-course \
  --repo-owner=vicKibira \
  --branch-pattern="^main$" \
  --build-config=phase-09-mlops/cloudbuild.yaml
```

### Step 6 — Monitor the KFP training run
```bash
# Check that the first training run is in progress
gcloud ai pipeline-jobs list --region=us-central1 --limit=5
```
Or open the Vertex AI Pipelines console printed in the banner.

### Step 7 — Re-run safely (idempotency check)
```bash
# Re-running the script should produce only "already exists" messages, no errors
bash phase-10-capstone/deploy_all.sh
```
If any step fails, fix the root cause and re-run. The script will skip already-complete phases automatically.

---

## What the full platform produces

| Component | Phase | GCP Service | Output |
|---|---|---|---|
| Infrastructure | 01 | Terraform → IAM, GCS, BQ, VPC | Foundation for everything |
| Event stream | 02 | Pub/Sub | `raw-events` topic |
| Batch ETL | 03 | Dataproc + PySpark | Silver Parquet in GCS |
| Stream processing | 04 | Dataflow + Apache Beam | `bronze_events` rows in BigQuery |
| Data warehouse | 05 | BigQuery + dbt | Silver/Gold/ML feature tables |
| Orchestration | 06 | Cloud Composer + Airflow | Nightly + weekly pipeline runs |
| ML training | 07 | Vertex AI Custom Training | `model.pkl` in GCS |
| ML pipeline | 08 | Vertex AI Pipelines (KFP) | Registered model in Model Registry |
| CI/CD + monitoring | 09 | Cloud Build + Model Monitoring | Canary endpoint + drift alerts |

---

## Troubleshooting

**"ERROR: .env file not found"**
Create `.env` from `.env.example`:
```bash
cp .env.example .env && nano .env
```

**"→ set PROJECT_ID in .env" on startup**
The `:?` validation fired — one of the required variables is empty. Open `.env` and check each line.

**Terraform apply fails with "Error: Error creating Bucket"**
Bucket name `$BUCKET_NAME` is taken (GCS names are global). Choose a more unique name (e.g., add your project ID as a suffix).

**"docker: Cannot connect to the Docker daemon"**
Docker Desktop is not running. Start it before re-running the script.

**"ERROR 403: Permission denied" on `bq query`**
Your application default credentials lack `roles/bigquery.dataOwner` on the project. Run:
```bash
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="user:YOUR_EMAIL" \
  --role="roles/bigquery.dataOwner"
```

**KFP training run never finishes**
Check Vertex AI Pipelines console. Common causes: `ml_features_churn` table is empty (run the BQ DDL and seed some data first), or the training container image was not pushed (check Artifact Registry for `churn-train:latest`).

---

## Key concepts covered

- **`set -euo pipefail`** — the shell safety triplet for reliable scripts
- **`:? syntax`** — fail-fast variable validation before any side effects
- **Idempotency patterns** — Terraform state, `|| echo "already exists"`, `IF NOT EXISTS`
- **Script portability** — `SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"` locates the repo root regardless of where you run the script from
- **Async Vertex AI operations** — `job.submit()` is non-blocking; the pipeline outlives the script
- **Manual vs automated deployment** — distinguishing one-time OAuth/console steps from fully scriptable infrastructure
- **`.env` pattern** — keeping credentials and project-specific values out of git while making them available to deployment scripts
- **`sed` for config injection** — substituting SQL placeholders at deploy time without template engines
