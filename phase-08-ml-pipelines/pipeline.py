"""
Phase 08 — LAB 8: Full KFP v2 Pipeline on Vertex AI
Steps: BQ feature pull → preprocess → train → evaluate → register → deploy
"""
import argparse
from kfp import dsl
from kfp.v2 import compiler
from google.cloud import aiplatform


# ── Components ────────────────────────────────────────────────────────────────

@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["google-cloud-bigquery", "pandas", "db-dtypes", "pyarrow"],
)
def extract_features(
    project:  str,
    dataset:  str,
    table:    str,
    gcs_path: dsl.Output[dsl.Dataset],
) -> None:
    """Pull feature table from BigQuery and save as parquet to GCS."""
    from google.cloud import bigquery
    import pandas as pd

    client = bigquery.Client(project=project)
    df = client.query(
        f"SELECT * FROM `{project}.{dataset}.{table}` WHERE churned IS NOT NULL"
    ).to_dataframe()

    df.to_parquet(gcs_path.path, index=False)
    print(f"Extracted {len(df):,} rows → {gcs_path.path}")


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "pyarrow"],
)
def preprocess(
    raw_data:       dsl.Input[dsl.Dataset],
    train_data:     dsl.Output[dsl.Dataset],
    test_data:      dsl.Output[dsl.Dataset],
    test_size:      float = 0.2,
) -> None:
    import pandas as pd
    from sklearn.model_selection import train_test_split

    FEATURES = [
        "total_orders", "lifetime_value", "avg_order_value",
        "days_since_last_order", "customer_lifespan_days",
        "cancellation_rate", "category_diversity",
    ]
    df = pd.read_parquet(raw_data.path)
    df = df[FEATURES + ["churned"]].fillna(0)

    train, test = train_test_split(df, test_size=test_size, random_state=42,
                                   stratify=df["churned"])
    train.to_parquet(train_data.path, index=False)
    test.to_parquet(test_data.path, index=False)
    print(f"Train: {len(train):,}  Test: {len(test):,}")


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "xgboost", "pyarrow"],
)
def train_model(
    train_data:   dsl.Input[dsl.Dataset],
    model_output: dsl.Output[dsl.Model],
    n_estimators: int   = 200,
    max_depth:    int   = 5,
    learning_rate: float = 0.05,
) -> None:
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

    model = Pipeline([
        ("scaler", StandardScaler()),
        ("clf",    xgb.XGBClassifier(
            n_estimators=n_estimators, max_depth=max_depth,
            learning_rate=learning_rate, scale_pos_weight=3,
            use_label_encoder=False, eval_metric="auc", random_state=42,
        )),
    ])
    model.fit(X, y)

    import os; os.makedirs(model_output.path, exist_ok=True)
    with open(f"{model_output.path}/model.pkl", "wb") as f:
        pickle.dump(model, f)
    print("Model trained and saved")


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["pandas", "scikit-learn", "xgboost", "pyarrow"],
)
def evaluate_model(
    test_data:    dsl.Input[dsl.Dataset],
    model_input:  dsl.Input[dsl.Model],
    metrics:      dsl.Output[dsl.ClassificationMetrics],
    threshold:    float = 0.75,
) -> bool:
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

    with open(f"{model_input.path}/model.pkl", "rb") as f:
        model = pickle.load(f)

    y_pred       = model.predict(X)
    y_pred_proba = model.predict_proba(X)[:, 1]
    auc          = roc_auc_score(y, y_pred_proba)
    cm           = confusion_matrix(y, y_pred)

    metrics.log_roc_curve([0, 0, 1], [0, 1, 1], [0.5])
    metrics.log_confusion_matrix(["active", "churned"], cm.tolist())
    print(f"AUC-ROC: {auc:.4f}  |  threshold: {threshold}")

    return auc >= threshold


@dsl.component(
    base_image="python:3.10-slim",
    packages_to_install=["google-cloud-aiplatform"],
)
def register_model(
    model_input:   dsl.Input[dsl.Model],
    project:       str,
    region:        str,
    model_name:    str,
    serving_image: str,
    model_uri:     dsl.Output[dsl.Artifact],
) -> str:
    from google.cloud import aiplatform
    aiplatform.init(project=project, location=region)

    model = aiplatform.Model.upload(
        display_name=model_name,
        artifact_uri=model_input.path,
        serving_container_image_uri=serving_image,
    )
    model_uri.uri    = model.resource_name
    model_uri.metadata["resource_name"] = model.resource_name
    print(f"Registered: {model.resource_name}")
    return model.resource_name


# ── Pipeline ──────────────────────────────────────────────────────────────────

@dsl.pipeline(
    name="churn-training-pipeline",
    description="End-to-end churn prediction: BQ → train → evaluate → register → deploy",
)
def churn_pipeline(
    project:       str,
    region:        str,
    dataset:       str,
    pipeline_root: str,
    model_name:    str  = "churn-xgb-v1",
    auc_threshold: float = 0.75,
):
    extract = extract_features(project=project, dataset=dataset,
                               table="ml_features_churn")
    preprocess_task = preprocess(raw_data=extract.outputs["gcs_path"])
    train_task = train_model(train_data=preprocess_task.outputs["train_data"])
    eval_task  = evaluate_model(
        test_data=preprocess_task.outputs["test_data"],
        model_input=train_task.outputs["model_output"],
        threshold=auc_threshold,
    )

    with dsl.Condition(eval_task.output == True, name="model-passes-eval"):
        register_model(
            model_input=train_task.outputs["model_output"],
            project=project, region=region,
            model_name=model_name,
            serving_image=f"{region}-docker.pkg.dev/{project}/ml-containers/churn-serve:latest",
        )


def compile_and_run(args) -> None:
    pipeline_path = "churn_pipeline.json"
    compiler.Compiler().compile(churn_pipeline, pipeline_path)
    print(f"Pipeline compiled → {pipeline_path}")

    aiplatform.init(project=args.project, location=args.region)

    job = aiplatform.PipelineJob(
        display_name=f"churn-training-{args.run_date}",
        template_path=pipeline_path,
        pipeline_root=args.pipeline_root,
        parameter_values={
            "project":       args.project,
            "region":        args.region,
            "dataset":       args.dataset,
            "pipeline_root": args.pipeline_root,
            "model_name":    args.model_name,
            "auc_threshold": args.auc_threshold,
        },
    )
    job.submit()
    print(f"Pipeline submitted: {job.resource_name}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",       required=True)
    parser.add_argument("--region",        default="us-central1")
    parser.add_argument("--dataset",       default="course_dataset")
    parser.add_argument("--pipeline_root", required=True,
                        help="gs://BUCKET/pipelines")
    parser.add_argument("--model_name",    default="churn-xgb-v1")
    parser.add_argument("--auc_threshold", type=float, default=0.75)
    parser.add_argument("--run_date",      default="latest")
    args = parser.parse_args()

    compile_and_run(args)
