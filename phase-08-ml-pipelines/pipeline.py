"""
Phase 08 — LAB 8: Full KFP v2 Pipeline on Vertex AI
====================================================
Defines a Kubeflow Pipelines (KFP) v2 training pipeline that automates the
full ML lifecycle from feature extraction to model registration.

What is a KFP pipeline?
  A pipeline is a Python function decorated with @dsl.pipeline that describes
  a directed acyclic graph (DAG) of containerised steps (components). Each
  component runs in an isolated container on managed GKE inside Vertex AI.
  Steps communicate only via typed artifacts (files: Dataset, Model, Metrics)
  stored in GCS — components cannot share in-memory state.

  KFP compiles the Python pipeline definition to a JSON specification
  (churn_pipeline.json) that Vertex AI can execute on-demand or on a schedule.

Pipeline steps (DAG):
  ┌─────────────────┐
  │ extract_features│  Pull feature table from BigQuery → GCS Parquet
  └────────┬────────┘
           │
  ┌────────▼────────┐
  │   preprocess    │  Train/test split → two GCS Parquet files
  └───┬─────────────┘
      │                   (produces train_data + test_data in parallel)
      ├──────────┐
      │          │
  ┌───▼──────┐ ┌▼──────────────────────────────────┐
  │train_model│ │          (test_data passed)          │
  └───┬──────┘ │              ▼                       │
      │        │         evaluate_model                │
      │        │         (AUC-ROC gate)                │
      └────────┘
           │
  ┌────────▼────────────────────────┐
  │ dsl.Condition(auc >= threshold) │  Only register if model is good enough
  │   └── register_model            │
  └─────────────────────────────────┘

Key KFP concepts used:
  @dsl.component  — turns a Python function into a containerised pipeline step.
                    The decorator builds a container image with base_image and
                    packages_to_install at compile time.
  dsl.Input[T]    — typed input artifact consumed by the component.
  dsl.Output[T]   — typed output artifact written by the component; KFP
                    resolves the GCS path automatically.
  dsl.Condition   — conditionally executes a sub-graph based on a component's
                    return value (here: auc_passed == True).

Usage:
  # Compile + submit to Vertex AI
  python pipeline.py --project my-project --pipeline_root gs://BUCKET/pipelines

  # The compiled JSON is also uploaded to pipeline_root for the Airflow DAG
  # (training_dag.py) to use on its weekly schedule.
"""
import argparse
from kfp import dsl
from kfp.v2 import compiler
from google.cloud import aiplatform


# ── Component: extract_features ───────────────────────────────────────────────

@dsl.component(
    # python:3.10-slim is a minimal image; components install their own deps.
    # This keeps each component's image small and independent (no shared environment).
    base_image="python:3.10-slim",
    # packages_to_install are pip-installed at component startup (not baked into
    # the base image). Keeps image build fast; adds ~30 sec to each component run.
    packages_to_install=["google-cloud-bigquery", "pandas", "db-dtypes", "pyarrow"],
)
def extract_features(
    project:  str,
    dataset:  str,
    table:    str,
    gcs_path: dsl.Output[dsl.Dataset],  # KFP auto-assigns a GCS URI for this artifact
) -> None:
    """
    Pull the ML feature table from BigQuery and serialise to GCS as Parquet.

    Why Parquet as intermediate format?
      - Columnar format: downstream components read only the columns they need
      - Efficient compression: 5-10x smaller than CSV for this feature set
      - Schema preservation: dtypes are embedded in the file, preventing
        silent type coercions between components

    The gcs_path.path attribute is the GCS URI where this component should
    write the Dataset artifact. KFP handles the path allocation — we don't
    choose the path ourselves.
    """
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project)
    # churned IS NOT NULL guard: exclude any rows with missing labels
    # (one-time buyers are filtered upstream in the feature engineering SQL)
    df = client.query(
        f"SELECT * FROM `{project}.{dataset}.{table}` WHERE churned IS NOT NULL"
    ).to_dataframe()

    # Write the DataFrame to the KFP-assigned GCS path as Parquet
    df.to_parquet(gcs_path.path, index=False)
    print(f"Extracted {len(df):,} rows → {gcs_path.path}")


# ── Component: preprocess ─────────────────────────────────────────────────────

@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "pyarrow"],
)
def preprocess(
    raw_data:   dsl.Input[dsl.Dataset],   # consumed from extract_features output
    train_data: dsl.Output[dsl.Dataset],  # 80% split → train_model input
    test_data:  dsl.Output[dsl.Dataset],  # 20% split → evaluate_model input
    test_size:  float = 0.2,              # configurable split ratio
) -> None:
    """
    Split the raw feature Parquet into stratified train and test Parquet files.

    Both output artifacts are written to separate GCS paths allocated by KFP.
    KFP's artifact passing system ensures that the train_model and evaluate_model
    components receive the correct paths without explicit coordination.

    Stratification ensures the churn class ratio is preserved in both splits.
    Without stratify, a 75/25 active/churned split in the full dataset might
    become 85/15 in one split by chance, distorting evaluation metrics.
    """
    import pandas as pd
    from sklearn.model_selection import train_test_split

    # Feature columns must match exactly the BQ feature table schema
    FEATURES = [
        "total_orders", "lifetime_value", "avg_order_value",
        "days_since_last_order", "customer_lifespan_days",
        "cancellation_rate", "category_diversity",
    ]
    # Read the Parquet from the path KFP provided for the input artifact
    df = pd.read_parquet(raw_data.path)
    # Keep only the columns we need; fillna(0) handles new users with sparse history
    df = df[FEATURES + ["churned"]].fillna(0)

    # Stratified split: maintains churn ratio across both sets
    train, test = train_test_split(df, test_size=test_size, random_state=42,
                                   stratify=df["churned"])
    train.to_parquet(train_data.path, index=False)
    test.to_parquet(test_data.path,  index=False)
    print(f"Train: {len(train):,}  Test: {len(test):,}")


# ── Component: train_model ────────────────────────────────────────────────────

@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "xgboost", "pyarrow"],
)
def train_model(
    train_data:    dsl.Input[dsl.Dataset],  # from preprocess
    model_output:  dsl.Output[dsl.Model],   # KFP allocates a GCS directory for the model
    n_estimators:  int   = 200,
    max_depth:     int   = 5,
    learning_rate: float = 0.05,
) -> None:
    """
    Train the XGBoost churn classifier and write model.pkl to GCS.

    This component mirrors the logic in phase-07/custom_training/train.py
    but runs inside the pipeline DAG rather than as a standalone Custom Job.

    The model_output is a dsl.Model artifact (a directory in GCS). We write
    'model.pkl' inside that directory. The register_model component later
    reads from model_output.path to upload to the Vertex AI Model Registry.

    Note: The sklearn Pipeline (scaler + clf) is pickled together so that
    inference code gets normalisation for free by calling pipeline.predict().
    """
    import pickle
    import pandas as pd
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
    import xgboost as xgb

    FEATURES = [
        "total_orders", "lifetime_value", "avg_order_value",
        "days_since_last_order", "customer_lifespan_days",
        "cancellation_rate", "category_diversity",
    ]
    df = pd.read_parquet(train_data.path)
    X, y = df[FEATURES], df["churned"]

    # Build and fit the sklearn Pipeline (same as custom_training/train.py)
    model = Pipeline([
        ("scaler", StandardScaler()),            # feature normalisation
        ("clf",    xgb.XGBClassifier(
            n_estimators=n_estimators,
            max_depth=max_depth,
            learning_rate=learning_rate,
            scale_pos_weight=3,           # weight for class imbalance
            use_label_encoder=False,      # suppress XGBoost deprecation warning
            eval_metric="auc",
            random_state=42,
        )),
    ])
    model.fit(X, y)

    # model_output.path is a directory URI; create it and write model.pkl inside
    import os
    os.makedirs(model_output.path, exist_ok=True)
    with open(f"{model_output.path}/model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("Model trained and saved")


# ── Component: evaluate_model ─────────────────────────────────────────────────

@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "xgboost", "pyarrow"],
)
def evaluate_model(
    test_data:   dsl.Input[dsl.Dataset],             # held-out test split
    model_input: dsl.Input[dsl.Model],               # trained model directory
    metrics:     dsl.Output[dsl.ClassificationMetrics],  # KFP metrics artifact (visible in console)
    threshold:   float = 0.75,                        # minimum AUC-ROC to proceed with registration
) -> bool:
    """
    Evaluate the model on the test set and return True if it meets the AUC threshold.

    The return value (bool) is used by dsl.Condition in the pipeline definition
    to gate the register_model step. If AUC < threshold, registration is skipped
    automatically — the CI/CD pipeline does not deploy a model that regressed.

    dsl.ClassificationMetrics:
      A KFP artifact type that is rendered as interactive charts in the
      Vertex AI console (ROC curve, confusion matrix). Useful for auditing
      pipeline runs without leaving the console.

    Why AUC-ROC as the gate metric?
      - Threshold-independent: we can evaluate model quality without choosing
        a business-specific operating point during the CI/CD gate.
      - Robust to class imbalance: accuracy would be misleading when churners
        are only 25% of the dataset (a trivial "predict all active" model
        gets 75% accuracy but 0.5 AUC-ROC).
    """
    import pickle
    import pandas as pd
    from sklearn.metrics import roc_auc_score, confusion_matrix

    FEATURES = [
        "total_orders", "lifetime_value", "avg_order_value",
        "days_since_last_order", "customer_lifespan_days",
        "cancellation_rate", "category_diversity",
    ]
    df = pd.read_parquet(test_data.path)
    X, y = df[FEATURES], df["churned"]

    # Load the model from the artifact directory written by train_model
    with open(f"{model_input.path}/model.pkl", "rb") as f:
        model = pickle.load(f)

    y_pred       = model.predict(X)
    y_pred_proba = model.predict_proba(X)[:, 1]  # probability of churned=1
    auc          = roc_auc_score(y, y_pred_proba)
    cm           = confusion_matrix(y, y_pred)

    # Log artifacts to the KFP metrics tracker (visible in Vertex AI console)
    # These are illustrative points; in production pass actual ROC curve arrays.
    metrics.log_roc_curve([0, 0, 1], [0, 1, 1], [0.5])
    metrics.log_confusion_matrix(["active", "churned"], cm.tolist())
    print(f"AUC-ROC: {auc:.4f}  |  threshold: {threshold}")

    # Return True/False — this value drives dsl.Condition in the pipeline
    return auc >= threshold


# ── Component: register_model ─────────────────────────────────────────────────

@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["google-cloud-aiplatform"],
)
def register_model(
    model_input:   dsl.Input[dsl.Model],    # trained model directory in GCS
    project:       str,
    region:        str,
    model_name:    str,
    serving_image: str,                     # Docker image URI for online prediction
    model_uri:     dsl.Output[dsl.Artifact], # output: the Vertex AI Model resource URI
) -> str:
    """
    Upload and register the trained model in Vertex AI Model Registry.

    Why the Model Registry?
      - Versioning: every registered model gets a version; old versions are
        preserved and can be rolled back to if a new model regresses.
      - Lineage: the registry links the model to the pipeline run that produced it,
        enabling full audit trail from data → features → training → endpoint.
      - Deployment: registered models can be deployed to endpoints by specifying
        their resource_name — cloudbuild.yaml's canary-deploy step uses this.

    serving_image:
      The Docker image that Vertex AI uses to serve online predictions.
      It must implement the Vertex AI prediction container protocol
      (respond to POST /predict with JSON). For sklearn + XGBoost, use the
      pre-built serving image:
        us-docker.pkg.dev/vertex-ai/prediction/sklearn-cpu.1-3:latest

    Returns:
        The registered Model's resource_name (used in model_uri.metadata).
    """
    from google.cloud import aiplatform
    aiplatform.init(project=project, location=region)

    # Upload registers the model artefact and serving container in Vertex AI.
    # artifact_uri points to the GCS directory containing model.pkl.
    model = aiplatform.Model.upload(
        display_name=model_name,
        artifact_uri=model_input.path,              # GCS path to model directory
        serving_container_image_uri=serving_image,  # container for online prediction
    )

    # Write the resource_name to the output artifact so downstream pipeline steps
    # (or the Airflow DAG via XCom) can reference the registered model.
    model_uri.uri                         = model.resource_name
    model_uri.metadata["resource_name"]   = model.resource_name
    print(f"Registered: {model.resource_name}")
    return model.resource_name


# ── Pipeline definition ───────────────────────────────────────────────────────

@dsl.pipeline(
    name="churn-training-pipeline",
    description="End-to-end churn prediction: BQ → train → evaluate → register",
)
def churn_pipeline(
    project:       str,
    region:        str,
    dataset:       str,
    pipeline_root: str,   # GCS path where KFP stores all pipeline artifacts
    model_name:    str   = "churn-xgb-v1",
    auc_threshold: float = 0.75,  # gate: only register models that pass this AUC
):
    """
    Defines the full training DAG by chaining component calls.

    KFP infers the DAG structure from data dependencies:
      - extract.outputs["gcs_path"]  → preprocess (input)
      - preprocess.outputs["train_data"] → train_model (input)
      - preprocess.outputs["test_data"]  → evaluate_model (input)
      - train_model.outputs["model_output"] → evaluate_model + register_model

    The dsl.Condition creates a conditional branch: register_model only runs
    if evaluate_model returns True (i.e. AUC-ROC ≥ auc_threshold).
    This is the quality gate that prevents regressed models from being registered.
    """
    # Step 1: Extract features from BigQuery
    extract = extract_features(
        project=project,
        dataset=dataset,
        table="ml_features_churn",
    )

    # Step 2: Preprocess (split) — depends on extract via raw_data artifact
    preprocess_task = preprocess(raw_data=extract.outputs["gcs_path"])

    # Step 3: Train — depends on preprocess via train_data artifact
    train_task = train_model(train_data=preprocess_task.outputs["train_data"])

    # Step 4: Evaluate — depends on both preprocess (test set) and train (model)
    eval_task = evaluate_model(
        test_data=preprocess_task.outputs["test_data"],
        model_input=train_task.outputs["model_output"],
        threshold=auc_threshold,
    )

    # Step 5: Conditional registration — only runs if eval_task returned True
    # dsl.Condition wraps the sub-graph; KFP skips it if the condition is false.
    # The serving image path follows the convention established in cloudbuild.yaml.
    with dsl.Condition(eval_task.output == True, name="model-passes-eval"):
        register_model(
            model_input=train_task.outputs["model_output"],
            project=project,
            region=region,
            model_name=model_name,
            # The serving image must exist in Artifact Registry (built by Cloud Build)
            serving_image=(
                f"{region}-docker.pkg.dev/{project}/ml-containers/churn-serve:latest"
            ),
        )


# ── Compile and submit ────────────────────────────────────────────────────────

def compile_and_run(args) -> None:
    """
    Compile the pipeline to JSON and submit to Vertex AI.

    Two distinct operations:
      1. Compile: converts the @dsl.pipeline Python function to a JSON spec
         (churn_pipeline.json). This step is local — no GCP API calls.
      2. Submit: uploads the JSON spec to Vertex AI and creates a PipelineJob.
         Vertex AI schedules and executes the pipeline on managed infrastructure.

    The compiled JSON is also referenced by the Airflow training_dag.py which
    uses it for weekly scheduled runs (it reads the JSON from pipeline_root).
    """
    # Step 1: Compile Python pipeline definition → JSON spec
    pipeline_path = "churn_pipeline.json"
    compiler.Compiler().compile(churn_pipeline, pipeline_path)
    print(f"Pipeline compiled → {pipeline_path}")

    # Step 2: Initialise Vertex AI SDK
    aiplatform.init(project=args.project, location=args.region)

    # Step 3: Create and submit a PipelineJob
    # display_name includes --run_date for traceability in the console.
    job = aiplatform.PipelineJob(
        display_name=f"churn-training-{args.run_date}",  # e.g. 'churn-training-2024-01-15'
        template_path=pipeline_path,                     # compiled JSON
        pipeline_root=args.pipeline_root,                # GCS artifact store
        parameter_values={
            "project":       args.project,
            "region":        args.region,
            "dataset":       args.dataset,
            "pipeline_root": args.pipeline_root,
            "model_name":    args.model_name,
            "auc_threshold": args.auc_threshold,
        },
    )
    # submit() is non-blocking: returns immediately with a resource_name.
    # The pipeline runs asynchronously; monitor it in the Vertex AI console.
    # Use job.wait() if you need to block (e.g. in a local CI script).
    job.submit()
    print(f"Pipeline submitted: {job.resource_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compile and run KFP churn training pipeline")
    parser.add_argument("--project",       required=True,           help="GCP project ID")
    parser.add_argument("--region",        default="us-central1",   help="Vertex AI region")
    parser.add_argument("--dataset",       default="course_dataset", help="BigQuery dataset")
    parser.add_argument("--pipeline_root", required=True,
                        help="GCS root for pipeline artifacts (gs://BUCKET/pipelines)")
    parser.add_argument("--model_name",    default="churn-xgb-v1",  help="Model display name")
    parser.add_argument("--auc_threshold", type=float, default=0.75,
                        help="Minimum AUC-ROC to register the model")
    parser.add_argument("--run_date",      default="latest",
                        help="Run date tag for display name (e.g. 2024-01-15 or git SHA)")
    args = parser.parse_args()

    compile_and_run(args)
