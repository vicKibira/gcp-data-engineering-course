"""
Phase 07 — LAB 7B: Custom Training Job on Vertex AI (XGBoost churn model)
==========================================================================
This script runs inside a Docker container that Vertex AI Custom Training
launches on managed compute. It is NOT run locally (use automl_training.py
for local experimentation).

Why custom training vs AutoML?
  - Full control over the algorithm (here: XGBoost with sklearn Pipeline wrapper)
  - Fine-grained hyperparameter tuning (can plug in Vertex AI HyperparameterTuningJob)
  - Reproducible, versionable code checked into source control
  - Can be integrated into KFP pipelines (see phase-08/pipeline.py)

How Vertex AI Custom Training works:
  1. Vertex AI launches a container (built from Dockerfile in this directory).
  2. It injects several environment variables that tell the script where to
     read data and write the model artefact.
  3. The script runs to completion; Vertex AI monitors exit code and logs.
  4. The model file written to AIP_MODEL_DIR is automatically picked up by
     Vertex AI and registered in the Model Registry if run from a pipeline.

Key environment variables injected by Vertex AI:
  AIP_MODEL_DIR         — GCS path to write the serialised model (gs://BUCKET/...)
  AIP_TRAINING_DATA_URI — optional: BigQuery or GCS URI for training data
  AIP_DATA_FORMAT       — optional: 'bigquery' or 'jsonl' (if using managed datasets)

Usage (local test — reads BQ, writes to /tmp/model):
  python train.py --project my-project

Usage (via Vertex AI — AIP_MODEL_DIR is set by the platform):
  gcloud ai custom-jobs create --region=us-central1 \\
    --config=custom_training_job.yaml
"""
import os
import pickle
import argparse
import numpy as np
import pandas as pd
from google.cloud import bigquery, storage
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline
import xgboost as xgb


# ── Feature and label definitions ────────────────────────────────────────────
# These must match exactly the columns in the ml_features_churn BQ table
# (created by the feature engineering SQL in 03_bqml_model.sql and refreshed
# by the Airflow DAG in phase-06).
FEATURES = [
    "total_orders",           # raw engagement: how many orders the user placed
    "lifetime_value",         # cumulative spend: higher = more committed customer
    "avg_order_value",        # spend intensity: high = premium buyer
    "days_since_last_order",  # recency: key churn indicator (>60 days → churned)
    "customer_lifespan_days", # tenure: longer tenure = stronger habit
    "cancellation_rate",      # friction: high cancellations signal dissatisfaction
    "category_diversity",     # breadth: single-category buyers churn faster
]
TARGET = "churned"  # binary label: 1 = churned, 0 = active


def load_data(project: str, dataset: str, table: str) -> pd.DataFrame:
    """
    Pull the ML feature table from BigQuery and return as a pandas DataFrame.

    Uses the BigQuery Storage API under the hood (faster than standard BQ reads
    for DataFrames). The table is expected to have been populated by the
    Airflow DAG's refresh_ml_features task.

    Args:
        project: GCP project ID.
        dataset: BigQuery dataset (e.g. 'course_dataset').
        table:   Table name (e.g. 'ml_features_churn').

    Returns:
        DataFrame with feature columns + 'churned' label column.
    """
    client = bigquery.Client(project=project)
    # Filter out rows where the label is NULL (one-time buyers excluded by
    # the feature engineering SQL, but this is a safety net).
    query = f"""
        SELECT {', '.join(FEATURES + [TARGET])}
        FROM `{project}.{dataset}.{table}`
        WHERE {TARGET} IS NOT NULL
    """
    print(f"Loading data from BQ: {project}.{dataset}.{table}")
    df = client.query(query).to_dataframe()
    print(f"  Loaded {len(df):,} rows")
    return df


def build_model(params: dict) -> Pipeline:
    """
    Construct a two-stage sklearn Pipeline: StandardScaler → XGBClassifier.

    Why a Pipeline?
      - Scaler and classifier are serialised together: when we pickle the Pipeline,
        the trained scaler is embedded. Inference code calls pipeline.predict()
        and gets the same normalisation automatically — no risk of train/serve skew.
      - sklearn Pipeline is a first-class citizen in scikit-learn, so it's compatible
        with cross-validation, HyperparameterTuning, and SHAP explainability.

    XGBoost hyperparameters:
      n_estimators:     number of boosting rounds (more = slower but potentially better)
      max_depth:        tree depth (5 = moderate complexity, avoids overfitting)
      learning_rate:    shrinkage rate per round (lower = slower but more robust)
      subsample:        row sampling ratio (0.8 = 80% of rows per tree)
      colsample_bytree: feature sampling ratio (0.8 = 80% of features per tree)
      scale_pos_weight: ratio of negative/positive examples; compensates for
                        class imbalance (churned users are rare). Set to ~3
                        because roughly 25% of users churn.
      eval_metric:      'auc' — XGBoost uses this for early stopping if provided.

    Args:
        params: dict with optional overrides for n_estimators, max_depth,
                learning_rate (others use defaults above).

    Returns:
        An unfitted sklearn Pipeline.
    """
    return Pipeline([
        # Step 1: StandardScaler normalises each feature to zero mean and unit variance.
        # XGBoost is tree-based and technically scale-invariant, but scaling helps
        # if we later switch to a linear model without changing the pipeline.
        ("scaler", StandardScaler()),

        # Step 2: XGBClassifier — gradient-boosted decision trees.
        # predict_proba() returns [P(active), P(churned)]; we use [:, 1] for scores.
        ("clf", xgb.XGBClassifier(
            n_estimators     = params.get("n_estimators", 200),
            max_depth        = params.get("max_depth", 5),
            learning_rate    = params.get("learning_rate", 0.05),
            subsample        = params.get("subsample", 0.8),         # row subsampling
            colsample_bytree = params.get("colsample_bytree", 0.8),  # feature subsampling
            scale_pos_weight = params.get("scale_pos_weight", 3),    # class imbalance weight
            eval_metric      = "auc",        # AUC-ROC used if early stopping is enabled
            use_label_encoder= False,        # suppress deprecation warning in XGBoost ≥1.6
            random_state     = 42,           # reproducible splits inside XGBoost
        )),
    ])


def save_model(model: Pipeline, model_dir: str) -> None:
    """
    Serialise the trained Pipeline to disk, then upload to GCS if necessary.

    Vertex AI Custom Training writes model artefacts to AIP_MODEL_DIR, which
    is a GCS path. We first save the file locally (to a temp dir if model_dir
    is GCS), then upload it.

    The file is named 'model.pkl' — this is the expected filename for the
    sklearn/XGBoost serving container in Vertex AI Prediction.

    Args:
        model:     The fitted sklearn Pipeline to serialise.
        model_dir: Local directory path or GCS URI (gs://BUCKET/path).
    """
    # If model_dir is a GCS path, we write locally first, then upload
    local_dir = "/tmp/model" if model_dir.startswith("gs://") else model_dir
    os.makedirs(local_dir, exist_ok=True)

    model_path = os.path.join(local_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"✓ Model saved locally to {model_path}")

    # Upload to GCS if AIP_MODEL_DIR is a GCS URI
    if model_dir.startswith("gs://"):
        # Parse gs://BUCKET/optional/prefix  →  bucket_name, blob_prefix
        bucket_name, blob_prefix = model_dir[5:].split("/", 1)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob   = bucket.blob(f"{blob_prefix}/model.pkl")
        blob.upload_from_filename(model_path)
        print(f"✓ Uploaded to {model_dir}/model.pkl")


def train(args) -> None:
    """
    Main training function: load data → split → train → evaluate → save.

    This function is the end-to-end training workflow:
      1. Load features from BigQuery.
      2. Fill NaN with 0 (safe: nulls mean zero activity for this feature set).
      3. Stratified train/test split to maintain churn ratio in both sets.
      4. Build and fit the XGBoost Pipeline.
      5. Evaluate on the test set: AUC-ROC + classification report.
      6. Save the model to AIP_MODEL_DIR (set by Vertex AI) or --model_dir arg.

    AUC-ROC interpretation:
      0.5  → random (no better than guessing)
      0.75 → acceptable for baseline churn model
      0.85 → good; suitable for production A/B test
      0.90+→ excellent; often seen after feature engineering iteration
    """
    # ── Load ──────────────────────────────────────────────────────────────────
    df = load_data(args.project, args.dataset, args.table)

    # fillna(0): NaN values in feature columns mean "no activity" (e.g. a new
    # user has no cancellation_rate yet). Filling with 0 is semantically correct.
    X = df[FEATURES].fillna(0)
    y = df[TARGET]

    # ── Split ─────────────────────────────────────────────────────────────────
    # stratify=y ensures churn ratio is the same in train and test sets.
    # Without stratify, a random split might put all churners in train by chance.
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    # ── Hyperparameters ───────────────────────────────────────────────────────
    # Override defaults with CLI args; remaining params use build_model() defaults.
    params = {
        "n_estimators":  args.n_estimators,
        "max_depth":     args.max_depth,
        "learning_rate": args.learning_rate,
    }

    # ── Train ─────────────────────────────────────────────────────────────────
    model = build_model(params)
    print(f"\nTraining XGBoost pipeline...  params={params}")
    model.fit(X_train, y_train)

    # ── Evaluate ──────────────────────────────────────────────────────────────
    # predict_proba()[:, 1] → probability of the positive class (churned=1)
    # AUC-ROC measures ranking quality: can the model distinguish churners from
    # actives? It's threshold-independent, which matters for churn scoring.
    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"\n[metrics] AUC-ROC = {auc:.4f}")
    # Classification report shows precision/recall/F1 at the default 0.5 threshold.
    # In production, the threshold is tuned (e.g. to 0.3) to favour recall for
    # churn (catching churners matters more than false-positive rate).
    print(classification_report(y_test, model.predict(X_test),
                                target_names=["active", "churned"]))

    # ── Save ──────────────────────────────────────────────────────────────────
    # AIP_MODEL_DIR takes precedence: Vertex AI injects it at runtime.
    # Fallback: --model_dir arg (useful for local testing).
    model_dir = os.environ.get("AIP_MODEL_DIR", args.model_dir)
    save_model(model, model_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Train XGBoost churn model on Vertex AI Custom Training"
    )
    parser.add_argument("--project",       required=True,          help="GCP project ID")
    parser.add_argument("--dataset",       default="course_dataset", help="BigQuery dataset")
    parser.add_argument("--table",         default="ml_features_churn", help="Feature table")
    parser.add_argument("--model_dir",     default="/tmp/model",   help="Local model output dir")
    # Hyperparameters — can be swept by Vertex AI HyperparameterTuningJob
    parser.add_argument("--n_estimators",  type=int,   default=200, help="XGBoost trees")
    parser.add_argument("--max_depth",     type=int,   default=5,   help="Tree max depth")
    parser.add_argument("--learning_rate", type=float, default=0.05, help="Shrinkage rate")
    args = parser.parse_args()
    train(args)
