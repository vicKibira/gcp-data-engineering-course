# Phase 08 — ML Pipelines (Kubeflow Pipelines v2)

## Why this phase exists

Phase 07 gave you two training scripts: one for AutoML, one for custom XGBoost. But running them manually is not a production ML workflow:

- You might forget to re-evaluate the model after retraining
- You might deploy a model that performs worse than the current one
- You can't audit who ran what with which data on which date
- The Airflow DAG in Phase 06 can't directly call a Python function — it needs a compiled artifact

**KFP (Kubeflow Pipelines v2)** solves all of this. A KFP pipeline is a Python function where each step runs in an isolated container. KFP compiles the Python definition to a JSON specification that Vertex AI Pipelines can execute on-demand or on a schedule — including being triggered by the Airflow DAG.

The key addition over Phase 07 is the **quality gate**: a `dsl.Condition` that only registers the model if it clears an AUC threshold. A model that regresses never reaches the Model Registry.

---

## What you'll build

- A compiled KFP pipeline JSON (`churn_pipeline.json`) stored in GCS
- A 4-step training DAG: extract → preprocess → train → evaluate
- A conditional registration step that only runs when AUC-ROC ≥ 0.75
- Pipeline run artifacts visible in the Vertex AI console (ROC curve, confusion matrix)

---

## Files

### `pipeline.py`

One file that defines all five pipeline components plus the pipeline DAG.

**How KFP components work:**

Every function decorated with `@dsl.component` becomes a containerised step:

```python
@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["google-cloud-bigquery", "pandas", "pyarrow"],
)
def extract_features(
    project:  str,
    gcs_path: dsl.Output[dsl.Dataset],   # KFP allocates a GCS URI automatically
) -> None:
    ...
```

The decorator tells KFP: "when this step runs, start a `python:3.10-slim` container, pip install the packages, then call this function." The `dsl.Output[dsl.Dataset]` parameter is how KFP passes a GCS URI between components — you write to `gcs_path.path` and the next component reads from it.

---

**Component 1 — `extract_features`:**
Reads `ml_features_churn` from BigQuery, writes as Parquet to a KFP-assigned GCS path. Why Parquet?
- Columnar format: downstream components read only the columns they need
- Schema embedded: no silent dtype coercions between components
- Compressed: 5-10x smaller than CSV for this feature set

**Component 2 — `preprocess`:**
Reads the raw feature Parquet, performs a stratified 80/20 split, writes `train_data` and `test_data` as two separate Parquet files.

```python
train, test = train_test_split(df, test_size=0.2, stratify=df["churned"])
```

`stratify=df["churned"]` preserves the churn ratio in both splits. Without it, imbalanced data can cause all churners to end up in one split.

**Component 3 — `train_model`:**
Reads `train_data`, fits the same XGBoost Pipeline from Phase 07 (StandardScaler + XGBClassifier), writes `model.pkl` to a KFP-assigned GCS directory:

```python
os.makedirs(model_output.path, exist_ok=True)
with open(f"{model_output.path}/model.pkl", "wb") as f:
    pickle.dump(model, f)
```

`model_output.path` is a GCS directory path allocated by KFP. The registered `dsl.Model` artifact links to this directory, so Vertex AI knows where to find the model file.

**Component 4 — `evaluate_model`:**
The quality gate. Loads the model, scores the test set, logs metrics as KFP artifacts:

```python
def evaluate_model(...) -> bool:
    auc = roc_auc_score(y, y_pred_proba)
    metrics.log_roc_curve(...)
    metrics.log_confusion_matrix(...)
    return auc >= threshold  # THIS bool drives dsl.Condition
```

`dsl.ClassificationMetrics` is a special KFP artifact type. Logging to it makes the ROC curve and confusion matrix appear as interactive charts in the Vertex AI console — no separate plotting code needed.

The function returns a `bool`. That return value is what `dsl.Condition` inspects.

**Component 5 — `register_model` (conditional):**
Uploads the model to Vertex AI Model Registry:

```python
model = aiplatform.Model.upload(
    display_name=model_name,
    artifact_uri=model_input.path,              # GCS dir with model.pkl
    serving_container_image_uri=serving_image,  # Docker image for online prediction
)
```

The `serving_container_image_uri` must implement the Vertex AI prediction protocol (respond to POST /predict). The pre-built `us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest` handles this for sklearn Pipelines containing XGBoost.

---

**The `dsl.Condition` quality gate:**

```python
with dsl.Condition(eval_task.output == True, name="model-passes-eval"):
    register_model(model_input=train_task.outputs["model_output"], ...)
```

If `evaluate_model` returns `False` (AUC < 0.75), the entire `with dsl.Condition(...)` block is skipped. The pipeline run completes successfully but without registering the model. The team sees "Condition skipped" in the console — a clear signal that the model regressed.

---

**Pipeline DAG definition:**

```python
@dsl.pipeline(name="churn-training-pipeline")
def churn_pipeline(project, region, dataset, pipeline_root, auc_threshold=0.75):
    extract   = extract_features(project=project, dataset=dataset, table="ml_features_churn")
    preprocess_task = preprocess(raw_data=extract.outputs["gcs_path"])
    train_task      = train_model(train_data=preprocess_task.outputs["train_data"])
    eval_task       = evaluate_model(
        test_data=preprocess_task.outputs["test_data"],
        model_input=train_task.outputs["model_output"],
    )
    with dsl.Condition(eval_task.output == True):
        register_model(model_input=train_task.outputs["model_output"], ...)
```

KFP infers the DAG from data dependencies — you don't specify ordering manually. `extract` runs first because `preprocess` needs its output. `train_model` and `evaluate_model` both depend on `preprocess`. This parallelism is automatic.

---

**Compilation and submission:**

```python
compiler.Compiler().compile(churn_pipeline, "churn_pipeline.json")  # local, no GCP calls

job = aiplatform.PipelineJob(template_path="churn_pipeline.json", ...)
job.submit()  # non-blocking: returns immediately
```

Compilation converts the Python function to a portable JSON spec. The JSON is what the Airflow `training_dag.py` submits via `RunPipelineJobOperator` on its weekly schedule.

---

## Prerequisites

- Phase 07 complete (Artifact Registry repo exists, `sa-vertex` service account has Vertex AI roles)
- `pip install kfp google-cloud-aiplatform`
- The serving container image must exist in Artifact Registry (Cloud Build pushes it in Phase 09)

---

## Lab — Step by step

### Step 1 — Install KFP SDK
```bash
pip install kfp google-cloud-aiplatform
```

### Step 2 — Compile the pipeline
```bash
python phase-08-ml-pipelines/pipeline.py \
  --project $PROJECT_ID \
  --pipeline_root gs://$BUCKET_NAME/pipelines \
  --compile-only  # (if you add this flag to the script, or just run the compile step)
```

Or compile manually:
```python
from kfp.v2 import compiler
from pipeline import churn_pipeline

compiler.Compiler().compile(churn_pipeline, "churn_pipeline.json")
```

### Step 3 — Upload the compiled JSON to GCS
```bash
gsutil cp churn_pipeline.json gs://$BUCKET_NAME/pipelines/churn_pipeline.json
```
The Airflow DAG in Phase 06 reads it from this path.

### Step 4 — Submit a pipeline run
```bash
python phase-08-ml-pipelines/pipeline.py \
  --project $PROJECT_ID \
  --pipeline_root gs://$BUCKET_NAME/pipelines \
  --dataset course_dataset \
  --auc_threshold 0.75 \
  --run_date $(date +%Y-%m-%d)
```

### Step 5 — Monitor in the console
Go to **Vertex AI → Pipelines → Pipeline runs**. Click your run to see:
- The DAG graph with each step's status (running / succeeded / skipped)
- Logs for each component
- The ROC curve and confusion matrix from `evaluate_model`
- Whether `model-passes-eval` ran or was skipped

### Step 6 — Verify the model was registered
```bash
gcloud ai models list --region=us-central1
```
If the model passed the AUC gate, you'll see `churn-xgb-v1` in the list.

### Step 7 — (Optional) Lower the threshold for testing
If you have limited training data, the model might not reach 0.75. Lower the threshold to see the full pipeline run:
```bash
python phase-08-ml-pipelines/pipeline.py \
  ... \
  --auc_threshold 0.5
```

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `churn_pipeline.json` in GCS | Phase 06 (Airflow `RunPipelineJobOperator`) |
| Registered `churn-xgb-v1` model in Vertex AI | Phase 09 (Cloud Build canary deployment) |
| Pipeline run artifacts (ROC curve, confusion matrix) | Audit trail; visible in Vertex AI console |

---

## Key concepts covered

- **KFP v2 `@dsl.component`** — turning a Python function into a containerised pipeline step
- **`dsl.Input[T]` / `dsl.Output[T]`** — typed artifact passing between components via GCS
- **`dsl.Dataset` / `dsl.Model` / `dsl.ClassificationMetrics`** — KFP artifact types
- **`dsl.Condition`** — conditional execution for quality gating
- **Pipeline DAG inference** — KFP infers execution order from data dependencies
- **`compiler.Compiler().compile()`** — producing the portable JSON pipeline spec
- **AUC-ROC quality gate** — only register models that clear the threshold
- **Non-blocking `job.submit()`** — submitting pipelines for async execution
- **Serving container protocol** — what Vertex AI expects the serving image to implement
