# Phase 01 — GCP Fundamentals & Infrastructure

## Why this phase exists

Before you write a single line of data code, someone has to provision the infrastructure. In the real world that means buckets, networks, IAM roles, service accounts — and if you do it manually via the console, the next engineer can't reproduce it, you can't audit what changed, and you'll spend hours clicking through the same menus for every new environment.

This phase teaches **Infrastructure as Code (IaC)** with Terraform. Every GCP resource the course needs — the data lake bucket, the VPC, the BigQuery dataset, the service accounts — is declared in code, version-controlled, and reproducible with a single command. That's how production teams operate.

---

## What you'll build

| Resource | Purpose |
|---|---|
| GCS bucket (`PROJECT-datalake`) | Data lake with Bronze / Silver / Gold / models / scripts zones |
| VPC + subnet | Private network for Dataproc clusters and Composer |
| BigQuery dataset | Holds all course tables (Bronze → Silver → Gold → ML features) |
| Service accounts (×3) | Least-privilege identities for Dataproc, Composer, Vertex AI |
| IAM bindings | Role assignments for each service account |
| Artifact Registry repo | Docker image storage for custom ML containers (Phase 07) |

---

## Files

### `scripts/enable_apis.sh`
Enables all 14 GCP APIs the course needs in one shot. **This must run before `terraform apply`** — Terraform can create resources but cannot enable APIs.

```
storage, bigquery, dataproc, dataflow, pubsub, composer,
aiplatform, cloudbuild, artifactregistry, cloudscheduler,
datastream, notebooks, compute, iam
```

Why a script and not Terraform? The `google_project_service` Terraform resource works but enabling APIs via `gcloud` is faster, more reliable, and easier to debug. The script is idempotent — safe to run multiple times.

### `terraform/variables.tf`
Declares every input variable. `project_id` has no default intentionally — you must pass it explicitly. This prevents Terraform from accidentally provisioning resources in the wrong project.

### `terraform/main.tf`
The full infrastructure definition. Key patterns to understand:

**GCS bucket** — `uniform_bucket_level_access = true` disables per-object ACLs. All access is controlled at the bucket level via IAM, which is simpler to audit. The lifecycle rule automatically moves objects older than 365 days to Nearline storage (cheaper). Versioning is on so you can recover accidentally overwritten raw files.

**Folder placeholders** — GCS has no real folders; the `/` in an object name creates the visual hierarchy in the console. The `for_each = toset([...])` loop creates one placeholder object per zone.

**VPC** — `auto_create_subnetworks = false` gives you a clean custom-mode VPC. The subnet has `private_ip_google_access = true` so Dataproc workers with no external IPs can still reach GCS and BigQuery via Google's private network (no NAT gateway needed).

**IAM bindings** — The `local.sa_roles` map + `flatten` + `for_each` pattern generates individual `google_project_iam_member` resources for every (member, role) combination without duplicating resource blocks. This is the idiomatic Terraform approach for multiple roles across multiple service accounts.

### `terraform/outputs.tf`
Prints the bucket name, BQ dataset ID, VPC name, and service account emails after `terraform apply`. Copy these into your `.env` file — every subsequent phase references them.

---

## Prerequisites

- GCP project with billing enabled
- `gcloud` CLI authenticated (`gcloud auth login && gcloud auth application-default login`)
- Terraform >= 1.5 installed
- Owner or Editor role on the project (needed to enable APIs and create service accounts)

---

## Lab — Step by step

### Step 1 — Configure your environment
```bash
cd gcp-data-engineering-course
cp .env.example .env
# Open .env and fill in:
#   PROJECT_ID=your-project-id
#   REGION=us-central1
```

### Step 2 — Enable APIs
```bash
bash phase-01-gcp-fundamentals/scripts/enable_apis.sh $PROJECT_ID
```
Wait for the command to complete (~60 seconds). All 14 APIs must be enabled before Terraform can create resources that depend on them.

### Step 3 — Initialise Terraform
```bash
cd phase-01-gcp-fundamentals/terraform
terraform init
```
This downloads the Google Cloud provider plugin. You'll see a `.terraform/` directory created.

### Step 4 — Preview the plan
```bash
terraform plan \
  -var="project_id=$PROJECT_ID" \
  -var="region=us-central1"
```
Read the output. It should show ~15 resources to be created, nothing to destroy. Review the IAM bindings and bucket settings.

### Step 5 — Apply
```bash
terraform apply -auto-approve \
  -var="project_id=$PROJECT_ID" \
  -var="region=us-central1"
```
Takes 2–3 minutes. When done, Terraform prints all outputs.

### Step 6 — Save outputs to .env
```bash
# Copy the printed values into your .env
BUCKET_NAME=$(terraform output -raw bucket_name)
BQ_DATASET=$(terraform output -raw bq_dataset)
VPC_NAME=$(terraform output -raw vpc_name)
DATAPROC_SA=$(terraform output -raw dataproc_sa)
COMPOSER_SA=$(terraform output -raw composer_sa)
VERTEX_SA=$(terraform output -raw vertex_sa)
```

### Step 7 — Verify in the console
- **GCS**: Open the bucket and confirm you see `raw/`, `processed/`, `curated/`, `models/`, `scripts/` folders
- **BigQuery**: Confirm the dataset exists
- **IAM**: Confirm the three service accounts exist and have their roles

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `gs://PROJECT-datalake/raw/` | Phase 02 (GCS ingest), Phase 03 (Spark reads Bronze) |
| `gs://PROJECT-datalake/processed/` | Phase 03 (Spark writes Silver) |
| `gs://PROJECT-datalake/scripts/` | Phase 03 (Dataproc job script staging) |
| `gs://PROJECT-datalake/models/` | Phase 07/08 (model artefacts) |
| BigQuery dataset | Phase 04 (Dataflow writes), Phase 05 (dbt + SQL), Phase 07 (BQML) |
| VPC | Phase 03 (Dataproc cluster), Phase 06 (Composer environment) |
| `sa-dataproc` | Phase 03 (Spark jobs) |
| `sa-composer` | Phase 06 (Airflow DAGs) |
| `sa-vertex` | Phase 07/08/09 (Vertex AI training + pipelines) |
| Artifact Registry | Phase 07 (custom training container), Phase 09 (Cloud Build push) |

---

## Key concepts covered

- **Infrastructure as Code** — reproducible, reviewable, versioned infrastructure
- **Least-privilege IAM** — separate service accounts per workload, not one all-powerful SA
- **Medallion Architecture** — Bronze / Silver / Gold zone layout established here, used throughout
- **Terraform `for_each`** — avoiding resource block duplication with map iteration
- **GCS uniform bucket-level access** — why per-object ACLs are an anti-pattern at scale
- **Private Google Access** — reaching GCP APIs from VMs without public IPs
