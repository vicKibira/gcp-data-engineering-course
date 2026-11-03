# Phase 07 — ML Foundations (Vertex AI)

## Why this phase exists

You have a churn feature table in BigQuery from Phase 05, and a BQML logistic regression model as a baseline. Now you need to go further: try a no-code AutoML approach that often beats hand-tuned models on tabular data, and build a custom XGBoost training pipeline you can version, tune, and deploy to production.

This phase covers **two ways to train models on Vertex AI** — the managed ML platform on GCP:

1. **AutoML Tabular** — upload data, choose an objective, let Google do the rest. Best when you want a strong baseline fast or don't have an ML engineer on the team.
2. **Custom Training** — write your own Python training script, package it in Docker, and run it on managed Vertex AI compute. Best when you need full control of the algorithm or need to wire into CI/CD pipelines.

Both approaches are solving the same problem — predicting which users will churn — so you can directly compare results.

---

## What you'll build

- A Vertex AI `TabularDataset` linked to the `ml_features_churn` BigQuery table
- An AutoML Tabular churn model registered in Vertex AI Model Registry
- A Docker container image pushed to Artifact Registry containing the XGBoost training script
- A custom XGBoost churn model trained via Vertex AI Custom Training
- A deployed prediction endpoint (optional — covered in Phase 08 via KFP pipeline)

---

## Files

### `automl_training.py`

**What it does:** Wraps three Vertex AI SDK calls — create dataset, define job, run training.

**`aiplatform.TabularDataset.create_from_dataframe()`:**
```python
vertex_dataset = aiplatform.TabularDataset.create_from_dataframe(
    display_name="churn-features-ml_features_churn",
    bq_source=f"bq://{project}.{dataset}.{bq_table}",
)
```
Despite the `_from_dataframe` name, this reads from BigQuery, not a pandas DataFrame — that's a Vertex AI SDK naming quirk. Vertex copies the BQ data to an internal GCS location for training.

**`AutoMLTabularTrainingJob` settings:**
```python
job = aiplatform.AutoMLTabularTrainingJob(
    optimization_prediction_type="classification",
    optimization_objective="maximize-au-roc",  # AUC-ROC, not accuracy
)
```
Why `maximize-au-roc` and not `minimize-log-loss` or accuracy?
- Churn data is imbalanced: ~25% churners, ~75% active. Accuracy is misleading (predicting "everyone stays" gives 75% accuracy but finds zero churners).
- AUC-ROC measures ranking quality: can the model separate churners from actives? A score of 0.85 means the model correctly ranks an 85% churner above an active user 85% of the time.

**Training budget:**
```python
model = job.run(budget_milli_node_hours=1000, sync=True)
```
`1000 milli_node_hours = 1 node-hour` of budget. AutoML uses this to decide how many model architectures to try. Typical cost: 1000 = ~$4–8. Higher budget = more trials = usually better model. Minimum for classification is 1000.

**Deployment:**
```python
endpoint = model.deploy(
    machine_type="n1-standard-4",
    min_replica_count=1,
    max_replica_count=3,
    sync=True,
)
```
Deploys the model to a REST endpoint. `min_replica_count=1` keeps one warm replica running at all times (low latency). `max_replica_count=3` lets Vertex autoscale on high load.

---

### `custom_training/train.py`

A self-contained training script that runs inside the Docker container.

**Feature list:**
```python
FEATURES = [
    "total_orders",           # how many orders the user placed
    "lifetime_value",         # cumulative spend
    "avg_order_value",        # spend intensity
    "days_since_last_order",  # recency — key churn signal
    "customer_lifespan_days", # tenure
    "cancellation_rate",      # friction / dissatisfaction signal
    "category_diversity",     # single-category buyers churn faster
]
```
These map 1-to-1 to columns in `ml_features_churn`. Adding a feature here requires updating the BQ feature table and retraining.

**The sklearn Pipeline:**
```python
Pipeline([
    ("scaler", StandardScaler()),                    # normalise inputs
    ("clf", xgb.XGBClassifier(
        scale_pos_weight=3,   # class imbalance: ~3x more actives than churners
        eval_metric="auc",
    )),
])
```
Why wrap XGBoost in a sklearn Pipeline?
- The fitted `StandardScaler` is serialised together with the classifier. When you load `model.pkl` for inference, you call `model.predict_proba(X)` and get correctly scaled predictions automatically — no separate scaler state to manage.
- XGBoost is tree-based and technically scale-invariant, but wrapping in a Pipeline makes it easy to swap in a logistic regression or SVM without changing the inference code.

**Stratified split:**
```python
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)
```
`stratify=y` preserves the churn ratio in both train and test sets. Without it, a random split might put all churners in the training set by chance, making the test AUC artificially high.

**AIP_MODEL_DIR:**
```python
model_dir = os.environ.get("AIP_MODEL_DIR", args.model_dir)
```
`AIP_MODEL_DIR` is a GCS URI injected by Vertex AI at container startup. The script saves `model.pkl` there. If not set (local testing), it falls back to `--model_dir`. This pattern is the idiomatic way to make a training script work both locally and on Vertex AI.

---

### `custom_training/Dockerfile`

**Base image:**
```dockerfile
FROM us-docker.pkg.dev/vertex-ai/training/sklearn-cpu.1-3:latest
```
Google maintains this image with Python 3.10, scikit-learn 1.3, numpy, scipy, and pandas pre-installed. Saves ~200MB and 2 minutes of build time vs starting from `python:3.10-slim`.

**Layer caching order:**
```dockerfile
COPY requirements.txt .        # slow: rarely changes
RUN pip install --no-cache-dir -r requirements.txt
COPY train.py .                # fast: changes often
```
Docker caches each layer until the layer's inputs change. `requirements.txt` changes far less often than `train.py`. By copying it first, subsequent `docker build` runs skip the slow `pip install` step when only `train.py` changed. This cuts CI build time from ~3 minutes to ~10 seconds.

**ENTRYPOINT exec form:**
```dockerfile
ENTRYPOINT ["python", "train.py"]
```
The JSON array form (exec form) starts Python directly, not via a shell. This means SIGTERM (sent by Vertex AI when cancelling a job) reaches the Python process immediately instead of being caught by the shell wrapper.

---

### `custom_training/requirements.txt`

Pinned versions prevent silent breaking changes:
```
xgboost==2.0.3
scikit-learn==1.4.0
pandas==2.1.4
google-cloud-bigquery==3.17.2
google-cloud-storage==2.14.0
db-dtypes==1.2.0   # required for BQ → pandas type mapping
```
`db-dtypes` is often overlooked: it maps BigQuery's INT64/FLOAT64/DATE types to pandas' nullable integer/float dtypes. Without it, `client.query(...).to_dataframe()` raises a type error.

---

## Prerequisites

- Phase 01 complete (Vertex AI API enabled, `sa-vertex` service account exists, Artifact Registry repo exists)
- Phase 05/06 complete (`ml_features_churn` table populated in BigQuery)
- Docker installed locally (for building the training container)
- `pip install google-cloud-aiplatform`

---

## Lab — Step by step

### Part A — AutoML training

#### Step 1 — Train the AutoML model
```bash
pip install google-cloud-aiplatform

python phase-07-ml-foundations/automl_training.py \
  --project $PROJECT_ID \
  --region us-central1 \
  --dataset course_dataset \
  --table ml_features_churn \
  --target churned \
  --budget 1000
```
Training takes 1–2 hours. The script prints the model resource name when done.

#### Step 2 — View results in the console
Go to **Vertex AI → Training → Training jobs**. Click the job to see:
- Model evaluation metrics (AUC-ROC, precision, recall curves)
- Feature importances (which features AutoML weighted most)
- Confusion matrix at various thresholds

#### Step 3 — (Optional) Deploy to endpoint
```bash
python phase-07-ml-foundations/automl_training.py \
  --project $PROJECT_ID \
  --deploy
```

---

### Part B — Custom Training (XGBoost)

#### Step 1 — Build the Docker image
```bash
cd phase-07-ml-foundations/custom_training

export IMAGE_URI=us-central1-docker.pkg.dev/$PROJECT_ID/ml-containers/churn-trainer:v1

docker build -t $IMAGE_URI .
```

#### Step 2 — Push to Artifact Registry
```bash
gcloud auth configure-docker us-central1-docker.pkg.dev
docker push $IMAGE_URI
```

#### Step 3 — Test locally first
```bash
# Run the training script directly (no Docker) to verify it works
python custom_training/train.py \
  --project $PROJECT_ID \
  --dataset course_dataset \
  --table ml_features_churn \
  --model_dir /tmp/churn_model
```
Check the AUC-ROC printed. Target: > 0.75.

#### Step 4 — Submit custom training job to Vertex AI
```bash
gcloud ai custom-jobs create \
  --region=us-central1 \
  --display-name="churn-xgboost-v1" \
  --worker-pool-spec=machine-type=n1-standard-4,replica-count=1,container-image-uri=$IMAGE_URI \
  --args="--project=$PROJECT_ID,--dataset=course_dataset,--table=ml_features_churn"
```
Vertex AI pulls the image, starts the container, and injects `AIP_MODEL_DIR` pointing to a GCS path. The training script writes `model.pkl` there.

#### Step 5 — Check the trained model
```bash
# List custom training jobs
gcloud ai custom-jobs list --region=us-central1

# Get the job details (includes the GCS output directory)
gcloud ai custom-jobs describe JOB_ID --region=us-central1
```

#### Step 6 — View logs
```bash
gcloud ai custom-jobs stream-logs JOB_ID --region=us-central1
```
You should see the AUC-ROC score and classification report printed by the script.

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| AutoML model (Vertex AI Model Registry) | Phase 08 (KFP pipeline evaluates and gates on this model) |
| Custom training container image in Artifact Registry | Phase 08 (KFP `custom_train_component` pulls this image) |
| `model.pkl` in GCS `models/` zone | Phase 08 (KFP pipeline registers it to Model Registry) |
| Prediction endpoint (if deployed) | Phase 09 (Cloud Build canary deployment, model monitoring) |

---

## Key concepts covered

- **AutoML vs custom training** — trade-offs between control, expertise needed, and time-to-model
- **AUC-ROC vs accuracy** — why ranking metrics matter for imbalanced classification
- **Training budget (milli node-hours)** — controlling AutoML exploration depth
- **sklearn Pipeline** — coupling scaler + classifier for skew-free inference
- **`scale_pos_weight`** — compensating for class imbalance in XGBoost
- **Stratified train/test split** — maintaining churn ratio across splits
- **`AIP_MODEL_DIR`** — the env var contract between Vertex AI and your training script
- **Docker layer caching** — ordering COPY/RUN for fast CI builds
- **Exec form ENTRYPOINT** — signal propagation for graceful job cancellation
