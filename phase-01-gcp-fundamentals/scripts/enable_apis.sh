#!/usr/bin/env bash
# LAB 1 — Enable all GCP APIs needed for the course
set -euo pipefail

PROJECT_ID="${1:-$(gcloud config get-value project)}"
echo "Enabling APIs on project: $PROJECT_ID"

gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  dataproc.googleapis.com \
  dataflow.googleapis.com \
  pubsub.googleapis.com \
  composer.googleapis.com \
  aiplatform.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  cloudscheduler.googleapis.com \
  datastream.googleapis.com \
  notebooks.googleapis.com \
  compute.googleapis.com \
  iam.googleapis.com \
  --project="$PROJECT_ID"

echo "✓ All APIs enabled"
