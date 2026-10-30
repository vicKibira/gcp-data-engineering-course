#!/usr/bin/env bash
# Phase 10 — CAPSTONE: Deploy the full GCP data platform in sequence
# This is the culmination of all 10 phases.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="$SCRIPT_DIR/.env"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: .env file not found. Copy .env.example to .env and fill it in."
  exit 1
fi

source "$ENV_FILE"

: "${PROJECT_ID:?}"
: "${REGION:?}"
: "${BUCKET_NAME:?}"
: "${BQ_DATASET:?}"

echo "╔══════════════════════════════════════════════════════════════╗"
echo "║    GCP Data Engineering & ML — Capstone Deployment          ║"
echo "╠══════════════════════════════════════════════════════════════╣"
echo "║  Project : $PROJECT_ID"
echo "║  Region  : $REGION"
echo "║  Bucket  : $BUCKET_NAME"
echo "║  Dataset : $BQ_DATASET"
echo "╚══════════════════════════════════════════════════════════════╝"
echo ""

# ── Phase 01: Enable APIs & Terraform ────────────────────────────────────────
echo "▶ Phase 01 — GCP Fundamentals"
bash "$SCRIPT_DIR/phase-01-gcp-fundamentals/scripts/enable_apis.sh" "$PROJECT_ID"

cd "$SCRIPT_DIR/phase-01-gcp-fundamentals/terraform"
terraform init -input=false
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="region=$REGION" \
  -var="bq_dataset=$BQ_DATASET"
cd "$SCRIPT_DIR"
echo "✓ Phase 01 complete"

# ── Phase 02: Create Pub/Sub topic ────────────────────────────────────────────
echo ""
echo "▶ Phase 02 — Data Ingestion (Pub/Sub setup)"
gcloud pubsub topics create "${PUBSUB_TOPIC:-raw-events}" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  topic already exists"
gcloud pubsub subscriptions create "${PUBSUB_TOPIC:-raw-events}-sub" \
  --topic="${PUBSUB_TOPIC:-raw-events}" \
  --project="$PROJECT_ID" 2>/dev/null || echo "  subscription already exists"
echo "✓ Phase 02 complete"

# ── Phase 03: Upload Spark script ─────────────────────────────────────────────
echo ""
echo "▶ Phase 03 — Batch Processing (upload Spark script)"
gsutil cp "$SCRIPT_DIR/phase-03-batch-processing/spark_etl.py" \
  "gs://${BUCKET_NAME}/scripts/spark_etl.py"
echo "✓ Phase 03 script uploaded"

# ── Phase 05: Create BigQuery tables ─────────────────────────────────────────
echo ""
echo "▶ Phase 05 — BigQuery (create tables)"
sed \
  -e "s/\${PROJECT}/$PROJECT_ID/g" \
  -e "s/\${DATASET}/$BQ_DATASET/g" \
  "$SCRIPT_DIR/phase-05-bigquery/sql/01_create_tables.sql" | \
  bq query --use_legacy_sql=false --project_id="$PROJECT_ID"
echo "✓ Phase 05 tables created"

# ── Phase 07: Build & push training container ─────────────────────────────────
echo ""
echo "▶ Phase 07 — ML Foundations (build training container)"
REGISTRY="${REGION}-docker.pkg.dev/${PROJECT_ID}/ml-containers"
gcloud auth configure-docker "${REGION}-docker.pkg.dev" --quiet

docker build \
  -t "${REGISTRY}/churn-train:latest" \
  "$SCRIPT_DIR/phase-07-ml-foundations/custom_training/"

docker push "${REGISTRY}/churn-train:latest"
echo "✓ Phase 07 training container pushed"

# ── Phase 08: Compile & upload KFP pipeline ──────────────────────────────────
echo ""
echo "▶ Phase 08 — ML Pipelines (compile KFP pipeline)"
cd "$SCRIPT_DIR/phase-08-ml-pipelines"
pip install -q kfp google-cloud-aiplatform
python pipeline.py \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --dataset="$BQ_DATASET" \
  --pipeline_root="${VERTEX_STAGING_BUCKET}/pipelines" \
  --model_name="churn-xgb-v1" \
  --run_date="capstone"
cd "$SCRIPT_DIR"
echo "✓ Phase 08 pipeline submitted"

echo ""
echo "╔══════════════════════════════════════════════════════════════╗"
echo "║  ✓ Capstone deployment complete!                            ║"
echo "║                                                              ║"
echo "║  Next steps:                                                 ║"
echo "║  1. Start the publisher:                                     ║"
echo "║     python phase-02-data-ingestion/pubsub_publisher.py \\    ║"
echo "║       --project=$PROJECT_ID --topic=raw-events              ║"
echo "║  2. Start Dataflow:                                          ║"
echo "║     python phase-04-stream-processing/beam_pipeline.py \\   ║"
echo "║       --project=$PROJECT_ID --runner=DataflowRunner ...     ║"
echo "║  3. Check Vertex AI console for the pipeline run            ║"
echo "║  4. Set up Cloud Build trigger (phase-09-mlops/cloudbuild)  ║"
echo "╚══════════════════════════════════════════════════════════════╝"
