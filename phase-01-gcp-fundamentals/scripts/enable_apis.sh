#!/usr/bin/env bash
# ============================================================================
# Phase 01 — Enable all GCP APIs needed for the course
#
# Run this BEFORE terraform apply. Terraform can create resources but it
# cannot enable APIs — that must happen first via gcloud.
#
# Usage:
#   bash enable_apis.sh                    # uses gcloud default project
#   bash enable_apis.sh my-project-id      # explicit project
# ============================================================================
set -euo pipefail

# Accept project as first argument or fall back to the gcloud default
PROJECT_ID="${1:-$(gcloud config get-value project)}"
echo "Enabling APIs on project: $PROJECT_ID"
echo ""

gcloud services enable \
  storage.googleapis.com \          # Cloud Storage (GCS) — data lake bucket
  bigquery.googleapis.com \         # BigQuery — Silver/Gold tables + BQML
  dataproc.googleapis.com \         # Dataproc — managed Spark clusters
  dataflow.googleapis.com \         # Dataflow — managed Apache Beam runner
  pubsub.googleapis.com \           # Pub/Sub — real-time event streaming
  composer.googleapis.com \         # Cloud Composer — managed Airflow
  aiplatform.googleapis.com \       # Vertex AI — AutoML, custom training, KFP
  cloudbuild.googleapis.com \       # Cloud Build — CI/CD pipeline (Phase 09)
  artifactregistry.googleapis.com \ # Artifact Registry — Docker image storage
  cloudscheduler.googleapis.com \   # Cloud Scheduler — cron triggers
  datastream.googleapis.com \       # Datastream — CDC / replication (Phase 02 bonus)
  notebooks.googleapis.com \        # Vertex AI Workbench — Jupyter notebooks
  compute.googleapis.com \          # Compute Engine — Dataproc VMs, VPC
  iam.googleapis.com \              # IAM — service accounts and role bindings
  --project="$PROJECT_ID"

echo ""
echo "✓ All APIs enabled for project: $PROJECT_ID"
echo ""
echo "Next step:"
echo "  cd phase-01-gcp-fundamentals/terraform"
echo "  terraform init && terraform apply -var=\"project_id=$PROJECT_ID\""
