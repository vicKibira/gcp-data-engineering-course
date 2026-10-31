#!/usr/bin/env bash
# Phase 10 — CAPSTONE: Deploy the full GCP Data & ML Platform in one script
# ==========================================================================
# This script orchestrates the initial deployment of every component built
# across all 10 phases. Run it once on a fresh GCP project to stand up the
# entire platform end-to-end.
#
# What this script does (in order):
#   Phase 01 — Enable required GCP APIs + Terraform infrastructure (IAM, GCS, BQ, VPC)
#   Phase 02 — Create Pub/Sub topic + subscription for real-time event streaming
#   Phase 03 — Upload the PySpark ETL script to GCS (Dataproc reads it from there)
#   Phase 05 — Create BigQuery tables (bronze, silver, gold, ML features)
#   Phase 07 — Build the XGBoost training container + push to Artifact Registry
#   Phase 08 — Compile the KFP pipeline + submit the first Vertex AI training run
#
# Phases 04 (Dataflow), 06 (Composer DAGs), and 09 (Cloud Build triggers) are
# deployed separately because they require manual configuration steps
# (Dataflow needs a running publisher; Composer needs the DAG bucket path;
# Cloud Build needs a GitHub connection).
#
# Prerequisites:
#   1. Create a .env file from .env.example with your project values.
#   2. Authenticate: gcloud auth application-default login
#   3. Grant yourself Owner or Editor + Security Admin roles on the project.
#   4. Ensure Docker Desktop (or Docker Engine) is running locally.
#   5. Terraform is installed and on PATH.
#   6. Python 3.10+ with kfp + google-cloud-aiplatform installed.
#
# Usage:
#   bash phase-10-capstone/deploy_all.sh
#
# Re-run safety: all steps are idempotent — re-running won't duplicate
# resources (Terraform detects existing state; gcloud exits 0 for existing
# resources with || echo "already exists").

set -euo pipefail
# -e : exit immediately if any command returns non-zero
# -u : treat unset variables as errors (catches typos in variable names)
# -o pipefail : propagate pipe failures (e.g. cmd | head fails if cmd fails)

# ── Locate the repo root ──────────────────────────────────────────────────────
# $0 is the path to this script; dirname gets its directory (phase-10-capstone).
# We go one level up (..) to reach the repo root, then resolve the absolute path.
SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

# ── Load environment variables ────────────────────────────────────────────────
# .env is not tracked by Git (see .gitignore). It contains project-specific
# values (project ID, bucket names, etc.) that differ across environments.
if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env file not found at $ENV_FILE"
  echo "  Copy: cp $SCRIPT_DIR/.env.example $SCRIPT_DIR/.env"
  echo "  Then fill in your project values."
  exit 1
fi

source "$ENV_FILE"  # exports all KEY=VALUE lines as environment variables

# ── Validate required variables ───────────────────────────────────────────────
# The :? syntax fails with an error message if the variable is unset or empty.
# This catches a partially-filled .env file before any GCP calls are made.
: "${PROJECT_ID:?  → set PROJECT_ID in .env}"
: "${REGION:?      → set REGION in .env (e.g. us-central1)}"
: "${BUCKET_NAME:? → set BUCKET_NAME in .env (GCS bucket name, no gs:// prefix)}"
: "${BQ_DATASET:?  → set BQ_DATASET in .env (BigQuery dataset name)}"

# Optional variables with defaults
PUBSUB_TOPIC="${PUBSUB_TOPIC:-raw-events}"           # default Pub/Sub topic name
VERTEX_STAGING_BUCKET="${VERTEX_STAGING_BUCKET:-gs://${BUCKET_NAME}/vertex-staging}"

# ── Print deployment summary ──────────────────────────────────────────────────
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║    GCP Data Engineering & ML — Capstone Deployment          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Project : $PROJECT_ID"
echo "║  Region  : $REGION"
echo "║  Bucket  : $BUCKET_NAME"
echo "║  Dataset : $BQ_DATASET"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Phase 01: Enable APIs and provision infrastructure with Terraform ─────────
echo "▶ Phase 01 — GCP Fundamentals (APIs + Terraform)"

# enable_apis.sh calls 'gcloud services enable' for each required API.
# Running it even if APIs are already enabled is safe (gcloud is idempotent here).
bash "$SCRIPT_DIR/phase-01-gcp-fundamentals/scripts/enable_apis.sh" "$PROJECT_ID"

# Terraform provisions IAM, GCS buckets (with lifecycle rules), VPC, and
# the Artifact Registry repository.
cd "$SCRIPT_DIR/phase-01-gcp-fundamentals/terraform"
terraform init -input=false  # download provider plugins; -input=false: non-interactive
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="region=$REGION" \
  -var="bq_dataset=$BQ_DATASET"
# -auto-approve: skip the interactive "yes/no" prompt; safe for scripted use.
# Terraform reads existing state and only modifies resources that have drifted.

cd "$SCRIPT_DIR"
echo "✓ Phase 01 complete"

# ── Phase 02: Create Pub/Sub topic and subscription ───────────────────────────
echo ""
echo "▶ Phase 02 — Data Ingestion (Pub/Sub setup)"

# Create the topic that pubsub_publisher.py publishes to.
# 2>/dev/null suppresses the "already exists" error; || echo keeps the script alive.
gcloud pubsub topics create "${PUBSUB_TOPIC}" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  topic already exists"

# Create the pull subscription that pubsub_subscriber.py reads from.
# The subscription retains unacked messages for 7 days by default.
gcloud pubsub subscriptions create "${PUBSUB_TOPIC}-sub" \
  --topic="${PUBSUB_TOPIC}" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  subscription already exists"

echo "✓ Phase 02 complete"

# ── Phase 03: Upload PySpark script to GCS ───────────────────────────────────
echo ""
echo "▶ Phase 03 — Batch Processing (upload Spark script to GCS)"

# Dataproc workers read the PySpark job script directly from GCS.
# The Airflow DAG (phase-06) references gs://$BUCKET_NAME/scripts/spark_etl.py
# as the main_python_file_uri. The script must be uploaded here first.
gsutil cp "$SCRIPT_DIR/phase-03-batch-processing/spark_etl.py" \
  "gs://${BUCKET_NAME}/scripts/spark_etl.py"

echo "✓ Phase 03 script uploaded"

# ── Phase 05: Create BigQuery tables ─────────────────────────────────────────
echo ""
echo "▶ Phase 05 — BigQuery (create bronze, silver, gold, and ML feature tables)"

# 01_create_tables.sql uses ${PROJECT} and ${DATASET} placeholders.
# sed replaces them with the actual values from .env before sending to bq CLI.
# --use_legacy_sql=false: use standard SQL (not BigQuery's legacy SQL dialect).
sed \
  -e "s/\${PROJECT}/$PROJECT_ID/g" \
  -e "s/\${DATASET}/$BQ_DATASET/g" \
  "$SCRIPT_DIR/phase-05-bigquery/sql/01_create_tables.sql" | \
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID"

echo "✓ Phase 05 tables created"

# ── Phase 07: Build and push the training container ───────────────────────────
echo ""
echo "▶ Phase 07 — ML Foundations (build XGBoost training container)"

# The Artifact Registry repository was created by Terraform in Phase 01.
# We configure Docker to authenticate to it using the gcloud credential helper.
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/ml-containers"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet
# --quiet suppresses "Updated property [core/project]" output clutter

# Build the Docker image from the Dockerfile in custom_training/.
# The build context includes train.py and requirements.txt.
docker build \
  -t "${REGISTRY}/churn-train:latest" \
  "$SCRIPT_DIR/phase-07-ml-foundations/custom_training/"

# Push the image to Artifact Registry.
# The KFP pipeline (phase-08) and Cloud Build (phase-09) pull this image.
docker push "${REGISTRY}/churn-train:latest"
echo "✓ Phase 07 training container pushed to $REGISTRY"

# ── Phase 08: Compile KFP pipeline and submit first training run ──────────────
echo ""
echo "▶ Phase 08 — ML Pipelines (compile KFP pipeline + first training run)"

cd "$SCRIPT_DIR/phase-08-ml-pipelines"
# Install the KFP SDK and Vertex AI SDK if not already installed
pip install -q kfp google-cloud-aiplatform

# pipeline.py compiles churn_pipeline.json and submits a PipelineJob.
# This is an asynchronous operation: the pipeline runs in Vertex AI after this
# script exits. Monitor progress at:
#   https://console.cloud.google.com/vertex-ai/pipelines?project=$PROJECT_ID
python pipeline.py \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --dataset="$BQ_DATASET" \
  --pipeline_root="${VERTEX_STAGING_BUCKET}/pipelines" \
  --model_name="churn-xgb-v1" \
  --run_date="capstone"   # label this run as the capstone deployment run

cd "$SCRIPT_DIR"
echo "✓ Phase 08 pipeline submitted to Vertex AI"

# ── Deployment complete ───────────────────────────────────────────────────────
echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ✓ Capstone deployment complete!                            ║"
echo "║                                                              ║"
echo "║  NEXT STEPS:                                                 ║"
echo "║                                                              ║"
echo "║  1. Start event publisher (simulates e-commerce events):    ║"
echo "║     python phase-02-data-ingestion/pubsub_publisher.py \\   ║"
echo "║       --project=$PROJECT_ID --topic=$PUBSUB_TOPIC          ║"
echo "║                                                              ║"
echo "║  2. Start Dataflow streaming pipeline (Pub/Sub → BQ):      ║"
echo "║     python phase-04-stream-processing/beam_pipeline.py \\  ║"
echo "║       --project=$PROJECT_ID \\                              ║"
echo "║       --runner=DataflowRunner \\                            ║"
echo "║       --region=$REGION \\                                   ║"
echo "║       --temp_location=gs://$BUCKET_NAME/temp               ║"
echo "║                                                              ║"
echo "║  3. Check Vertex AI Pipelines console for training run:    ║"
echo "║     https://console.cloud.google.com/vertex-ai/pipelines   ║"
echo "║                                                              ║"
echo "║  4. Set up Cloud Build trigger for CI/CD (phase-09):       ║"
echo "║     gcloud builds triggers create github \\                 ║"
echo "║       --repo-name=gcp-data-engineering-course \\           ║"
echo "║       --branch-pattern=^main$ \\                           ║"
echo "║       --build-config=phase-09-mlops/cloudbuild.yaml        ║"
echo "║                                                              ║"
echo "║  5. Upload DAGs to Composer (phase-06):                    ║"
echo "║     gsutil cp phase-06-orchestration/dags/*.py \\          ║"
echo "║       gs://COMPOSER_BUCKET/dags/                            ║"
echo "╚══════════════════════════════════════════════════════════════╝"
