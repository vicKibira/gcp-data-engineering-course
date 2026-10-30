"""
Phase 07 — Custom Training Job on Vertex AI (XGBoost churn model)
This script runs inside a container on Vertex AI Training.

Environment variables injected by Vertex AI:
  AIP_MODEL_DIR        — where to write the model artefact
  AIP_TRAINING_DATA_URI — BigQuery table URI
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


FEATURES = [
    "total_orders", "lifetime_value", "avg_order_value",
    "days_since_last_order", "customer_lifespan_days",
    "cancellation_rate", "category_diversity",
]
TARGET = "churned"


def load_data(project: str, dataset: str, table: str) -> pd.DataFrame:
    client = bigquery.Client(project=project)
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
    return Pipeline([
        ("scaler", StandardScaler()),
        ("clf", xgb.XGBClassifier(
            n_estimators     = params.get("n_estimators", 200),
            max_depth        = params.get("max_depth", 5),
            learning_rate    = params.get("learning_rate", 0.05),
            subsample        = params.get("subsample", 0.8),
            colsample_bytree = params.get("colsample_bytree", 0.8),
            scale_pos_weight = params.get("scale_pos_weight", 3),
            eval_metric      = "auc",
            use_label_encoder= False,
            random_state     = 42,
        )),
    ])


def save_model(model: Pipeline, model_dir: str) -> None:
    os.makedirs(model_dir, exist_ok=True)
    model_path = os.path.join(model_dir, "model.pkl")
    with open(model_path, "wb") as f:
        pickle.dump(model, f)
    print(f"✓ Model saved to {model_path}")

    # If model_dir is a GCS path, upload via storage client
    if model_dir.startswith("gs://"):
        bucket_name, blob_prefix = model_dir[5:].split("/", 1)
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob   = bucket.blob(f"{blob_prefix}/model.pkl")
        blob.upload_from_filename(model_path)
        print(f"✓ Uploaded to {model_dir}/model.pkl")


def train(args) -> None:
    df = load_data(args.project, args.dataset, args.table)

    X = df[FEATURES].fillna(0)
    y = df[TARGET]

    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )

    params = {
        "n_estimators":  args.n_estimators,
        "max_depth":     args.max_depth,
        "learning_rate": args.learning_rate,
    }

    model = build_model(params)
    print(f"\nTraining XGBoost pipeline...  params={params}")
    model.fit(X_train, y_train)

    y_pred_proba = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, y_pred_proba)
    print(f"\n[metrics] AUC-ROC = {auc:.4f}")
    print(classification_report(y_test, model.predict(X_test),
                                target_names=["active", "churned"]))

    model_dir = os.environ.get("AIP_MODEL_DIR", args.model_dir)
    save_model(model, model_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",       required=True)
    parser.add_argument("--dataset",       default="course_dataset")
    parser.add_argument("--table",         default="ml_features_churn")
    parser.add_argument("--model_dir",     default="/tmp/model")
    parser.add_argument("--n_estimators",  type=int,   default=200)
    parser.add_argument("--max_depth",     type=int,   default=5)
    parser.add_argument("--learning_rate", type=float, default=0.05)
    args = parser.parse_args()
    train(args)
