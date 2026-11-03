# Phase 09 — MLOps (CI/CD + Monitoring)

## Why this phase exists

You now have a working ML pipeline. But every time someone pushes code to GitHub, does the model still work? If you retrain on new data and the model gets worse, does it get deployed anyway? And once deployed, how do you know if the predictions are degrading because the real world has changed?

This phase closes the loop with two things:

1. **CI/CD with Cloud Build** — automate the path from a code commit to a canary-deployed model. Every push to `main` triggers: tests → container build → training → deploy at 5% traffic.
2. **Model monitoring with Vertex AI** — sample 20% of live predictions, compare feature distributions against training data, and alert via email if something drifts.

Together these turn a weekend ML project into a production ML platform.

---

## What you'll build

- A Cloud Build trigger connected to the GitHub repo that fires on every `main` push
- A CI/CD pipeline: unit tests → Docker build → push to Artifact Registry → KFP pipeline → canary deploy
- A Vertex AI Model Monitor on the prediction endpoint with alert thresholds for training-serving skew and prediction drift

---

## Files

### `cloudbuild.yaml`

Five steps that run on every push to `main`:

**Step 1 — Unit tests:**
```yaml
- name: "python:3.10-slim"
  id: unit-tests
  entrypoint: bash
  args:
    - -c
    - |
      pip install -q pytest xgboost scikit-learn pandas pyarrow
      pytest phase-07-ml-foundations/ phase-08-ml-pipelines/ -v --tb=short
```
Runs before any GCP API calls. If tests fail, the whole build fails immediately — no wasted compute on building and pushing a broken container.

**Step 2 — Build training container:**
```yaml
- name: "gcr.io/cloud-builders/docker"
  id: build-container
  args:
    - build
    - -t "$_REGION-docker.pkg.dev/$_PROJECT_ID/$_REPO/churn-train:$SHORT_SHA"
    - -t "$_REGION-docker.pkg.dev/$_PROJECT_ID/$_REPO/churn-train:latest"
    - phase-07-ml-foundations/custom_training/
  waitFor: ["unit-tests"]
```
Tags with both `$SHORT_SHA` (the first 7 chars of the git commit SHA) and `latest`. Why two tags?
- `$SHORT_SHA` is **immutable** — you can always trace a model back to the exact code that trained it
- `latest` is **mutable** — the KFP pipeline always pulls the most recent training code

**Step 3 — Push to Artifact Registry:**
```yaml
- name: "gcr.io/cloud-builders/docker"
  args: [push, --all-tags, "$_REGION-docker.pkg.dev/$_PROJECT_ID/$_REPO/churn-train"]
```
`--all-tags` pushes both tags in one command. Artifact Registry (the modern successor to Container Registry) co-locates the image with Vertex AI workers for faster pulls, adds vulnerability scanning on push, and enforces IAM access.

**Step 4 — Submit KFP training pipeline:**
```yaml
- name: "python:3.10-slim"
  entrypoint: bash
  args:
    - -c
    - |
      pip install -q kfp google-cloud-aiplatform
      python phase-08-ml-pipelines/pipeline.py \
        --project=$_PROJECT_ID \
        --run_date=$SHORT_SHA
  waitFor: ["push-container"]
```
Compiles the KFP pipeline and submits it to Vertex AI. The pipeline runs **asynchronously** — `pipeline.py` returns immediately after submission. The actual training takes 30–90 minutes. The canary deploy step runs next, using the most recently registered model (which may be from the previous run if this run hasn't finished training yet).

**Step 5 — Canary deploy at 5% traffic:**
```bash
gcloud ai endpoints deploy-model "$ENDPOINT_ID" \
  --model="$MODEL_ID" \
  --display-name="canary-$SHORT_SHA" \
  --traffic-split="0=95,new=5"
```
`--traffic-split="0=95,new=5"` is the traffic split syntax:
- `0` = the placeholder for all existing deployed models (combined)
- `new` = the model being deployed right now
- `95/5` = 95% to existing stable model, 5% to the new canary

After running the canary for a week (or validating with A/B testing), promote it:
```bash
gcloud ai endpoints deploy-model $ENDPOINT_ID \
  --traffic-split="0=0,new=100"
```

**Substitution variables:**
```yaml
substitutions:
  _PROJECT_ID:    ${PROJECT_ID}
  _REGION:        us-central1
  _REPO:          ml-containers
  _AUC_THRESHOLD: "0.75"
```
Cloud Build substitution variables (prefixed `_`) can be overridden per trigger — useful for promoting a canary to production with a different threshold, or pointing a dev trigger at a different project.

---

### `monitoring_setup.py`

Configures Vertex AI Model Monitoring on the deployed endpoint.

**Two things monitoring detects:**

**1. Training-serving skew** — feature distributions at inference time differ from training data. This means your model is seeing inputs it wasn't trained on.
```python
"training_prediction_skew_detection_config": {
    "skew_thresholds": {
        "total_orders":          {"value": skew_threshold},  # key engagement signal
        "lifetime_value":        {"value": skew_threshold},  # revenue signal
        "days_since_last_order": {"value": skew_threshold},  # key churn signal
    }
}
```

**2. Prediction drift** — the model's output distribution shifts over time even if inputs look normal. This is concept drift: the real-world relationship between features and labels has changed.
```python
"prediction_drift_detection_config": {
    "drift_thresholds": {
        "days_since_last_order": {"value": drift_threshold},
    }
}
```

**How Jensen-Shannon divergence works:**
- Range 0 (identical distributions) to 1 (completely different)
- `0.3` (default) = alert on meaningful shifts, ignore sampling noise
- `0.1` = tighter, more sensitive (more false positives in production)
- `0.5` = loose, only alerts on severe drift

**How Vertex AI samples predictions:**
```python
"logging_sampling_strategy": {
    "random_sample_config": {"sample_rate": 0.2}  # log 20% of requests
}
```
20% sampling gives statistical power without logging every prediction (which adds cost and latency). Vertex AI runs the skew/drift analysis on the sampled logs on a schedule (configurable, default daily).

**Why monitor only three features?**
Not all features drift equally. `days_since_last_order` is the most important churn signal — if it drifts, predictions are likely wrong. `total_orders` is the most common data pipeline failure indicator. `lifetime_value` detects customer segment shifts. Monitoring fewer features reduces alert volume and makes real alerts more actionable.

---

## Prerequisites

- Phase 07–08 complete (model registered in Vertex AI, prediction endpoint deployed)
- Cloud Build API enabled (done in Phase 01's enable_apis.sh)
- GitHub repository connected to Cloud Build (one-time console setup)
- `pip install google-cloud-aiplatform`

---

## Lab — Step by step

### Part A — Set up Cloud Build CI/CD

#### Step 1 — Connect GitHub to Cloud Build
In the GCP console: **Cloud Build → Triggers → Connect repository**. Choose GitHub, authenticate, and select `gcp-data-engineering-course`.

#### Step 2 — Create the build trigger
```bash
gcloud builds triggers create github \
  --repo-name=gcp-data-engineering-course \
  --repo-owner=vicKibira \
  --branch-pattern="^main$" \
  --build-config=phase-09-mlops/cloudbuild.yaml \
  --project=$PROJECT_ID
```

#### Step 3 — Trigger a manual test build
```bash
# Run the Cloud Build pipeline manually (without a git push)
gcloud builds submit . \
  --config=phase-09-mlops/cloudbuild.yaml \
  --substitutions="_PROJECT_ID=$PROJECT_ID,SHORT_SHA=manual-test"
```

#### Step 4 — Monitor the build
```bash
# Stream build logs
gcloud builds log $(gcloud builds list --limit=1 --format="value(id)")
```
Or open **Cloud Build → History** in the console to see the pipeline graph.

#### Step 5 — Verify the canary deployment
```bash
gcloud ai endpoints list --region=us-central1
gcloud ai endpoints describe ENDPOINT_ID --region=us-central1
# Look for the traffic split in the output
```

---

### Part B — Set up model monitoring

#### Step 1 — Get the endpoint ID
```bash
ENDPOINT_ID=$(gcloud ai endpoints list \
  --region=us-central1 \
  --filter="displayName:churn-xgb-v1-endpoint" \
  --format="value(name)")
```

#### Step 2 — Run the monitoring setup script
```bash
python phase-09-mlops/monitoring_setup.py \
  --project $PROJECT_ID \
  --region us-central1 \
  --endpoint $ENDPOINT_ID \
  --bq_uri bq://$PROJECT_ID.course_dataset.ml_features_churn \
  --email your-email@example.com \
  --skew 0.3 \
  --drift 0.3
```

#### Step 3 — Enable monitoring via the console
Go to **Vertex AI → Endpoints → your endpoint → Monitoring tab → Enable monitoring**. The script prints the config; apply it here or via the v1beta1 API call printed in the output.

#### Step 4 — Send test predictions to trigger sampling
```bash
# Send a batch of test predictions to the endpoint
gcloud ai endpoints predict $ENDPOINT_ID \
  --region=us-central1 \
  --json-request='{"instances": [{"total_orders": 5, "lifetime_value": 250.0, "avg_order_value": 50.0, "days_since_last_order": 45, "customer_lifespan_days": 180, "cancellation_rate": 0.2, "category_diversity": 3}]}'
```

#### Step 5 — Check monitoring logs
After 24 hours, monitoring results appear in:
- **Vertex AI → Endpoints → Monitoring tab** (visual drift charts)
- **Cloud Logging** (if `enable_logging=True` in the config)

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| Deployed endpoint at 5% canary traffic | Business: live churn scoring API |
| Cloud Build trigger on `main` | All future code changes auto-deploy through the pipeline |
| Model monitoring on the endpoint | Operations: alerts when predictions degrade |

This is the final infrastructure phase. Phase 10 (Capstone) assembles the full deployment.

---

## Key concepts covered

- **CI/CD for ML** — automating test → build → train → deploy on every code push
- **`$SHORT_SHA` tagging** — immutable image tags for audit and rollback
- **Canary deployment** — traffic splitting (95/5) to safely test new model versions
- **`--traffic-split="0=95,new=5"`** — Vertex AI's traffic split syntax
- **Cloud Build `waitFor`** — controlling parallel vs sequential step execution
- **Substitution variables** — making a single cloudbuild.yaml work across environments
- **Training-serving skew** — feature distribution mismatch between training and production
- **Prediction drift** — concept drift: the world has changed faster than the model
- **Jensen-Shannon divergence** — the metric Vertex AI uses to measure distributional shift
- **Sampling rate** — balancing monitoring coverage vs cost (20% = standard)
