terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS Data Lake Bucket ──────────────────────────────────────────────────────
resource "google_storage_bucket" "datalake" {
  name          = "${var.project_id}-datalake"
  location      = var.region
  force_destroy = false

  uniform_bucket_level_access = true

  lifecycle_rule {
    condition { age = 365 }
    action    { type = "SetStorageClass"; storage_class = "NEARLINE" }
  }

  versioning { enabled = true }
}

resource "google_storage_bucket_object" "folders" {
  for_each = toset(["raw/", "processed/", "curated/", "models/", "scripts/"])
  name     = each.key
  bucket   = google_storage_bucket.datalake.name
  content  = " "
}

# ── VPC Network ───────────────────────────────────────────────────────────────
resource "google_compute_network" "vpc" {
  name                    = "${var.project_id}-vpc"
  auto_create_subnetworks = false
}

resource "google_compute_subnetwork" "subnet" {
  name          = "${var.project_id}-subnet"
  ip_cidr_range = "10.0.0.0/24"
  region        = var.region
  network       = google_compute_network.vpc.id

  private_ip_google_access = true
}

# ── BigQuery Dataset ──────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "main" {
  dataset_id  = var.bq_dataset
  location    = var.region
  description = "GCP course main dataset"

  labels = {
    env     = "course"
    managed = "terraform"
  }
}

# ── Service Accounts ─────────────────────────────────────────────────────────
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
locals {
  sa_roles = {
    "serviceAccount:${google_service_account.dataproc.email}" = [
      "roles/dataproc.worker",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
    ]
    "serviceAccount:${google_service_account.composer.email}" = [
      "roles/composer.worker",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
      "roles/dataproc.editor",
    ]
    "serviceAccount:${google_service_account.vertex.email}" = [
      "roles/aiplatform.user",
      "roles/storage.objectAdmin",
      "roles/bigquery.dataEditor",
      "roles/artifactregistry.writer",
    ]
  }
}

resource "google_project_iam_member" "bindings" {
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
resource "google_artifact_registry_repository" "containers" {
  location      = var.region
  repository_id = "ml-containers"
  format        = "DOCKER"
  description   = "Custom training containers for Vertex AI"
}
