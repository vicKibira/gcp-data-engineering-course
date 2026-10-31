# ============================================================================
# Phase 01 — Terraform Variable Definitions
# Pass values via -var flags, a terraform.tfvars file, or environment
# variables prefixed with TF_VAR_ (e.g. TF_VAR_project_id=my-project).
# ============================================================================

variable "project_id" {
  description = "GCP project ID where all resources will be created"
  type        = string
  # No default — must be supplied explicitly to prevent accidental provisioning
}

variable "region" {
  description = "GCP region for all regional resources (GCS, BQ, Dataproc, etc.)"
  type        = string
  default     = "us-central1"  # Iowa — good latency for US + competitive pricing
}

variable "zone" {
  description = "GCP zone used for zonal resources (Dataproc master VM)"
  type        = string
  default     = "us-central1-a"
}

variable "bq_dataset" {
  description = "BigQuery dataset ID that will contain all course tables (Bronze/Silver/Gold)"
  type        = string
  default     = "course_dataset"
}
