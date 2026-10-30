output "bucket_name" {
  value = google_storage_bucket.datalake.name
}

output "bq_dataset" {
  value = google_bigquery_dataset.main.dataset_id
}

output "vpc_name" {
  value = google_compute_network.vpc.name
}

output "dataproc_sa" {
  value = google_service_account.dataproc.email
}

output "composer_sa" {
  value = google_service_account.composer.email
}

output "vertex_sa" {
  value = google_service_account.vertex.email
}

output "artifact_registry" {
  value = "${var.region}-docker.pkg.dev/${var.project_id}/ml-containers"
}
