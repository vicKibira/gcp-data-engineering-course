# ============================================================================
# Phase 01 — LAB 1: GCP Foundations with Terraform
# Provisions the full data platform infrastructure:
#   - GCS data lake bucket with lifecycle rules and folder structure
#   - VPC network + private subnet with Google API access
#   - BigQuery dataset
#   - 3 service accounts (Dataproc / Composer / Vertex AI)
#   - IAM bindings using for_each over member/role pairs (avoids duplication)
#   - Artifact Registry repo for custom ML containers
#
# Usage:
#   terraform init
#   terraform apply -var="project_id=YOUR_PROJECT" -var="region=us-central1"
# ============================================================================

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Data Lake Bucket ──────────────────────────────────────────────────────
# Single bucket housing all 5 zones: raw (Bronze), processed (Silver),
# curated (Gold), models, and scripts.
resource "google_storage_bucket" "datalake" {
  name          = "${var.project_id}-datalake"
  location      = var.region
  force_destroy = false  # prevent accidental deletion

  # Uniform bucket-level access — no per-object ACLs, simpler IAM management
  uniform_bucket_level_access = true

  # Move objects older than 1 year to Nearline (cheaper, slightly slower reads)
  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }

  # Keep object version history (useful for accidental overwrites in raw zone)
  versioning { enabled = true }
}

# Create the folder placeholders so the bucket structure is visible
# in the Console immediately after apply. GCS has no real folders —
# the trailing "/" in the object name creates the visual hierarchy.
resource "google_storage_bucket_object" "folders" {
  for_each = toset(["raw/", "processed/", "curated/", "models/", "scripts/"])
  name     = each.key
  bucket   = google_storage_bucket.datalake.name
  content  = " "  # GCS requires non-empty content for an object
}

# ── VPC Network ───────────────────────────────────────────────────────────────
# Custom-mode VPC so we control subnets explicitly (no auto-created subnets
# in every region, which would be wasteful and hard to audit).
resource "google_compute_network" "vpc" {
  name                    = "${var.project_id}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.project_id}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id

  # Allow VMs with no external IPs to still reach Google APIs
  # (BigQuery, GCS, etc.) via Google's private network.
  private_ip_google_access = true
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────
# One dataset that holds Bronze / Silver / Gold tables plus ML feature tables.
# All tables will be co-located with the GCS bucket (same region).
resource "google_bigquery_dataset" "main" {
  dataset_id  = var.bq_dataset
  location    = var.region
  description = "GCP course main dataset"

  labels = {
    env     = "course"
    managed = "terraform"  # signals to the team this is IaC-managed
  }
}

# ── Service Accounts ─────────────────────────────────────────────────────────
# Separate SAs per workload (least-privilege principle):
#   sa-dataproc  — used by Dataproc master/worker nodes for Spark jobs
#   sa-composer  — used by Cloud Composer Airflow workers
#   sa-vertex    — used by Vertex AI training jobs and pipelines

resource "google_service_account" "dataproc" {
  account_id   = "sa-dataproc"
  display_name = "Dataproc Service Account"
}

resource "google_service_account" "composer" {
  account_id   = "sa-composer"
  display_name = "Cloud Composer Service Account"
}

resource "google_service_account" "vertex" {
  account_id   = "sa-vertex"
  display_name = "Vertex AI Service Account"
}

# ── IAM Bindings ──────────────────────────────────────────────────────────────
# Pattern: define a map of {member → [roles]}, then flatten it into
# individual {member, role} pairs and use for_each. This keeps the
# binding list readable and avoids repeating resource blocks.

locals {
  sa_roles = {
    # Dataproc SA needs worker role + R/W on GCS and BQ for the Spark ETL
    "serviceAccount:${google_service_account.dataproc.email}" = [
      "roles/dataproc.worker",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
    ]
    # Composer SA needs to trigger Dataproc jobs and write results back
    "serviceAccount:${google_service_account.composer.email}" = [
      "roles/composer.worker",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
      "roles/dataproc.editor",
    ]
    # Vertex SA needs to read/write GCS (artefacts), query BQ (features),
    # push images to Artifact Registry, and submit AI Platform jobs
    "serviceAccount:${google_service_account.vertex.email}" = [
      "roles/aiplatform.user",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
      "roles/artifactregistry.writer",
    ]
  }
}

resource "google_project_iam_member" "bindings" {
  # Flatten: [{member, role}, ...] → map keyed by "member/role" (unique)
  for_each = {
    for pair in flatten([
      for member, roles in local.sa_roles : [
        for role in roles : { member = member, role = role }
      ]
    ]) : "${pair.member}/${pair.role}" => pair
  }

  project = var.project_id
  role    = each.value.role
  member  = each.value.member
}

# ── Artifact Registry ─────────────────────────────────────────────────────────
# Stores the custom Docker container that runs the XGBoost training job
# on Vertex AI (built in Phase 07 and pushed via Cloud Build in Phase 09).
resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "ml-containers"
  format        = "DOCKER"
  description   = "Custom training containers for Vertex AI"
}
