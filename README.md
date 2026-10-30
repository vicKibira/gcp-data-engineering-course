# Data Engineering, Big Data & ML on GCP
### A to Z Course — 10 Phases · 21 Labs · ~120 Hours

A complete, production-grade GCP data engineering and ML course built around real tools, real patterns, and a hands-on capstone project.

---

## Course Structure

| Phase | Topic | Duration | Labs |
|-------|-------|----------|------|
| 01 | GCP Fundamentals & Infrastructure | ~1 week | 1 |
| 02 | Data Ingestion | ~1 week | 1 |
| 03 | Batch Processing (Dataproc + Spark) | ~1.5 weeks | 1 |
| 04 | Stream Processing (Dataflow + Beam) | ~1.5 weeks | 1 |
| 05 | BigQuery Mastery | ~2 weeks | 3 |
| 06 | Orchestration (Cloud Composer) | ~1 week | 2 |
| 07 | ML Foundations on Vertex AI | ~1.5 weeks | 1 |
| 08 | ML Engineering & Pipelines (KFP) | ~1.5 weeks | 1 |
| 09 | MLOps & CI/CD for ML | ~1.5 weeks | 1 |
| 10 | Architecture Patterns & Capstone | ~2 weeks | 1 |

---

## Prerequisites

- GCP account with billing enabled
- Python 3.9+
- `gcloud` CLI installed and authenticated
- Terraform >= 1.5
- Docker (for custom training containers)

## GCP Setup

```bash
# Authenticate
gcloud auth login
gcloud auth application-default login

# Set your project
export PROJECT_ID="your-project-id"
export REGION="us-central1"
gcloud config set project $PROJECT_ID
```

## Architecture

```
Raw Data (GCS)
    │
    ├── Batch: Dataproc (Spark) ──────────────────┐
    │                                              ▼
    └── Stream: Pub/Sub → Dataflow ──────→ BigQuery (Bronze → Silver → Gold)
                                                   │
                                         Cloud Composer (Orchestration)
                                                   │
                                         Vertex AI (ML Training + Serving)
                                                   │
                                      Cloud Build CI/CD (MLOps)
```

---

## Repository Layout

```
gcp-data-engineering-course/
├── phase-01-gcp-fundamentals/   # IAM, VPC, Terraform
├── phase-02-data-ingestion/     # GCS, Pub/Sub, Datastream
├── phase-03-batch-processing/   # Dataproc, Spark, PySpark
├── phase-04-stream-processing/  # Dataflow, Apache Beam
├── phase-05-bigquery/           # SQL, dbt, BQML
├── phase-06-orchestration/      # Cloud Composer, Airflow DAGs
├── phase-07-ml-foundations/     # Vertex AI, AutoML, Feature Store
├── phase-08-ml-pipelines/       # KFP v2, Vizier, batch prediction
├── phase-09-mlops/              # CI/CD, monitoring, Cloud Build
└── phase-10-capstone/           # Full platform integration
```

---

## Quick Start (Capstone)

```bash
git clone https://github.com/YOUR_USERNAME/gcp-data-engineering-course.git
cd gcp-data-engineering-course
cp .env.example .env          # fill in your project details
bash phase-10-capstone/deploy_all.sh
```

---

*Course by Victor Ahonakibira · GCP Professional Data Engineer track*
