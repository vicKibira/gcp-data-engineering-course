#!/usr/bin/env bash
# ============================================================================
# Phase 03 — LAB 3: Create Dataproc cluster and submit the Spark ETL job
#
# What this script does:
#   1. Checks if the Dataproc cluster exists; creates it if not
#   2. Uploads spark_etl.py to GCS so Dataproc workers can access it
#   3. Submits the PySpark job with the correct arguments
#
# The cluster is persistent (not ephemeral) to save re-creation time
# during the labs. In production you'd use a workflow template or
# Composer to spin up clusters per-job and delete them after.
#
# Usage:
#   bash submit_dataproc.sh                  # processes yesterday's date
#   bash submit_dataproc.sh 2024-01-15       # processes a specific date
# ============================================================================
set -euo pipefail

# Load environment variables from .env if present (see .env.example)
source ../.env 2>/dev/null || true

# Required variables — will error immediately if unset (set -u)
PROJECT_ID="${PROJECT_ID:?ERROR: set PROJECT_ID in .env or environment}"
BUCKET="${BUCKET_NAME:?ERROR: set BUCKET_NAME in .env or environment}"

# Optional variables with sensible defaults
REGION="${REGION:-us-central1}"
CLUSTER="course-cluster"

# Process yesterday's date if no argument provided
# Format: YYYY-MM-DD matching the partition structure in GCS
DATE="${1:-$(date +%Y-%m-%d --date='yesterday')}"

echo "=== Phase 03: Dataproc Batch ETL ==="
echo "Project : $PROJECT_ID"
echo "Cluster : $CLUSTER  (region: $REGION)"
echo "Bucket  : $BUCKET"
echo "Date    : $DATE"
echo ""

# ── Step 1: Create cluster if it doesn't already exist ───────────────────────
# `describe` exits non-zero if the cluster doesn't exist, so we suppress
# its error output and redirect to /dev/null to keep logs clean
if ! gcloud dataproc clusters describe "$CLUSTER" \
      --region="$REGION" --project="$PROJECT_ID" &>/dev/null; then

  echo "Cluster '$CLUSTER' not found — creating..."

  # NOTE: Comments after a backslash continuation (\) break bash syntax.
  # All flag explanations are kept in block comments below the command.
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
  # Flag notes:
  #   --num-workers=2          : 1 master + 2 workers (minimum for distributed jobs)
  #   --worker-machine-type    : n2-standard-2 = 2 vCPU, 8 GB RAM per worker
  #   --image-version=2.1      : Spark 3.3 + Python 3.10 (Debian 11)
  #   --optional-components=JUPYTER : adds Jupyter for interactive exploration
  #   --enable-component-gateway    : exposes Spark UI and Jupyter via IAP (no VPN needed)
  #   --bucket                 : staging bucket for job files and logs

  echo "✓ Cluster created"
else
  echo "✓ Cluster already exists — skipping creation"
fi

# ── Step 2: Upload the Spark script to GCS ───────────────────────────────────
# Dataproc workers download the script from GCS at job submission time.
# Uploading here ensures the latest version is always used — not a cached copy.
echo ""
echo "Uploading spark_etl.py to gs://${BUCKET}/scripts/ ..."
gsutil cp spark_etl.py "gs://${BUCKET}/scripts/spark_etl.py"
echo "✓ Script uploaded"

# ── Step 3: Submit the PySpark job ───────────────────────────────────────────
# The bare `--` separator tells gcloud that everything after it is a job
# argument to be passed to spark_etl.py, not a gcloud flag.
echo ""
echo "Submitting Spark ETL job for date: $DATE ..."
gcloud dataproc jobs submit pyspark "gs://${BUCKET}/scripts/spark_etl.py" \
  --cluster="$CLUSTER" \
  --region="$REGION" \
  --project="$PROJECT_ID" \
  -- \
  --bucket="$BUCKET" \
  --dataset=sales \
  --date="$DATE"

echo ""
echo "✓ Job submitted successfully"
echo "  View logs: https://console.cloud.google.com/dataproc/jobs?project=$PROJECT_ID"
echo ""
echo "Expected output in GCS:"
echo "  gs://${BUCKET}/processed/sales/year=.../month=.../day=.../*.parquet"
