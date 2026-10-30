#!/usr/bin/env bash
# LAB 3 — Create Dataproc cluster and submit the Spark ETL job
set -euo pipefail

source ../.env 2>/dev/null || true

PROJECT_ID="${PROJECT_ID:?set PROJECT_ID}"
REGION="${REGION:-us-central1}"
BUCKET="${BUCKET_NAME:?set BUCKET_NAME}"
CLUSTER="course-cluster"
DATE="${1:-$(date +%Y-%m-%d --date='yesterday')}"

echo "=== Phase 03: Dataproc Batch ETL ==="
echo "Project : $PROJECT_ID"
echo "Cluster : $CLUSTER"
echo "Date    : $DATE"

# Create cluster (skip if exists)
if ! gcloud dataproc clusters describe "$CLUSTER" --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then
  echo "Creating cluster..."
  gcloud dataproc clusters create "$CLUSTER" \
    --region="$REGION" \
    --project="$PROJECT_ID" \
    --num-workers=2 \
    --worker-machine-type=n2-standard-2 \
    --master-machine-type=n2-standard-2 \
    --image-version=2.1-debian11 \
    --optional-components=JUPYTER \
    --enable-component-gateway \
    --bucket="$BUCKET"
  echo "✓ Cluster created"
else
  echo "✓ Cluster already exists"
fi

# Upload the script to GCS
gsutil cp spark_etl.py "gs://${BUCKET}/scripts/spark_etl.py"

# Submit job
echo "Submitting Spark job..."
gcloud dataproc jobs submit pyspark "gs://${BUCKET}/scripts/spark_etl.py" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --bucket="$BUCKET" \
  --dataset=sales \
  --date="$DATE"

echo "✓ Job submitted — check Dataproc console for logs"
