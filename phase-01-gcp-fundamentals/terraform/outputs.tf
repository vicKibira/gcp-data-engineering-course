# ============================================================================
# Phase 01 — Terraform Outputs
# Outputs are shown after `terraform apply` and can be referenced by other
# Terraform modules or captured in CI scripts:
#   terraform output -raw bucket_name
# ============================================================================

# The GCS bucket name includes the project ID prefix, so it's globally unique
output "bucket_name" {
  description = "GCS data lake bucket name — set as BUCKET_NAME in .env"
  value       = google_storage_bucket.datalake.name
}

# The BigQuery dataset ID (not the full path — just the dataset portion)
output "bq_dataset" {
  description = "BigQuery dataset ID — set as BQ_DATASET in .env"
  value       = google_bigquery_dataset.main.dataset_id
}

# VPC network name used when creating Dataproc clusters and Composer envs
output "vpc_name" {
  description = "VPC network name — reference when creating GCP services in this network"
  value       = google_compute_network.vpc.name
}

# Service account emails — used in gcloud / Airflow / Cloud Build configs
output "dataproc_sa" {
  description = "Dataproc service account email — pass to cluster creation commands"
  value       = google_service_account.dataproc.email
}

output "composer_sa" {
  description = "Composer service account email — used when creating the Composer environment"
  value       = google_service_account.composer.email
}

output "vertex_sa" {
  description = "Vertex AI service account email — used for training job and pipeline submissions"
  value       = google_service_account.vertex.email
}

# Full Artifact Registry path used in Docker push/pull and Vertex AI container URIs
output "artifact_registry" {
  description = "Artifact Registry URL for the ml-containers repo — use as image prefix"
  value       = "${var.region}-docker.pkg.dev/${var.project_id}/ml-containers"
}
