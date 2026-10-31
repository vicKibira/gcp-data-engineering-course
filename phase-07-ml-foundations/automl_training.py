"""
Phase 07 — LAB 7A: Train a classification model with Vertex AI AutoML
======================================================================
Demonstrates Vertex AI AutoML Tabular — a no-code ML training option where
Google's infrastructure handles algorithm selection, feature engineering,
hyperparameter tuning, and ensembling automatically.

When to use AutoML vs custom training:
  AutoML (this script):
    - No ML expertise required — good for business analysts
    - Automatic feature engineering (handles categoricals, nulls, scaling)
    - NAS (Neural Architecture Search) finds the best model family
    - Typically outperforms hand-tuned single models on tabular data
    - Budget-controlled: you pay for node-hours, not for success

  Custom training (train.py):
    - Full control over algorithm, architecture, hyperparameters
    - Can integrate domain-specific feature engineering
    - Faster iteration cycle when you know the right approach
    - Required when AutoML's model families don't fit (e.g. time-series LSTM)

CRISP-DM phase: Modeling (both AutoML and custom training are alternative
modelling strategies for the churn prediction use case).

Usage:
  # Train only (takes 1-3 hours for budget=1000 milli node-hours)
  python automl_training.py --project my-project

  # Train and immediately deploy to a prediction endpoint
  python automl_training.py --project my-project --deploy

  # Larger budget = better model quality (Google tunes for longer)
  python automl_training.py --project my-project --budget 5000
"""
import argparse
from google.cloud import aiplatform


def train_automl(
    project: str,
    region: str,
    dataset: str,
    bq_table: str,
    target_column: str,
    model_display_name: str,
    budget_milli_node_hours: int = 1000,
) -> aiplatform.AutoMLTabularTrainingJob:
    """
    Train an AutoML Tabular classification model from a BigQuery feature table.

    Steps:
      1. Create a Vertex AI TabularDataset pointing at the BQ table.
         (Vertex imports the data to GCS internally; the BQ table is read-only.)
      2. Configure an AutoMLTabularTrainingJob with the optimization objective.
      3. Run the job (blocking): Google trains an ensemble model. Typical duration:
         1000 milli node-hours ≈ 1-2 hours wall time (parallelised internally).
      4. Return the trained Model resource (registered to Vertex AI Model Registry).

    Args:
        project:                 GCP project ID.
        region:                  GCP region for Vertex AI (must match dataset location).
        dataset:                 BigQuery dataset name containing the feature table.
        bq_table:                BigQuery table name (ml_features_churn).
        target_column:           Column to predict ('churned' = binary label).
        model_display_name:      Human-readable name in Vertex AI Model Registry.
        budget_milli_node_hours: Training budget. 1000 = 1 node-hour equivalent.
                                 Higher = more trials = (usually) better model.
                                 Minimum: 1000 for classification.

    Returns:
        The trained aiplatform.Model resource.
    """
    # Initialise the Vertex AI SDK with project and region
    # Uses Application Default Credentials (ADC) for authentication
    aiplatform.init(project=project, location=region)

    # ── Step 1: Create a Vertex AI Dataset from BigQuery ─────────────────────
    # TabularDataset wraps the BQ table as a Vertex AI managed dataset.
    # Vertex AI copies data to internal GCS for training (the BQ table isn't
    # read directly during training for performance reasons).
    print(f"Creating Vertex dataset from bq://{project}.{dataset}.{bq_table} ...")
    vertex_dataset = aiplatform.TabularDataset.create_from_dataframe(
        display_name=f"churn-features-{bq_table}",
        bq_source=f"bq://{project}.{dataset}.{bq_table}",
    )
    print(f"  Dataset: {vertex_dataset.resource_name}")

    # ── Step 2: Define the AutoML training job ────────────────────────────────
    # optimization_prediction_type='classification' → binary or multi-class
    # optimization_objective='maximize-au-roc' → maximise Area Under ROC curve
    # AUC-ROC is the right objective for churn because:
    #   - Class imbalance (few churners) makes accuracy misleading
    #   - We care about ranking users by risk, not a hard 0/1 decision
    job = aiplatform.AutoMLTabularTrainingJob(
        display_name=model_display_name,
        optimization_prediction_type="classification",
        optimization_objective="maximize-au-roc",   # alternatives: maximize-au-prc, minimize-log-loss
    )

    # ── Step 3: Run training ──────────────────────────────────────────────────
    # column_transformations tells AutoML how to treat each feature column.
    # 'numeric' → standard scaling + polynomial interactions
    # 'categorical' → one-hot or embedding depending on cardinality
    # AutoML also experiments with raw features vs transformed features.
    print(f"Starting AutoML training (budget={budget_milli_node_hours} milli node-hours)...")
    model = job.run(
        dataset=vertex_dataset,
        target_column=target_column,                         # 'churned'
        budget_milli_node_hours=budget_milli_node_hours,
        model_display_name=model_display_name,
        column_transformations=[
            {"numeric": {"column_name": "total_orders"}},
            {"numeric": {"column_name": "lifetime_value"}},
            {"numeric": {"column_name": "avg_order_value"}},
            {"numeric": {"column_name": "days_since_last_order"}},
            {"numeric": {"column_name": "customer_lifespan_days"}},
            {"numeric": {"column_name": "cancellation_rate"}},
            {"numeric": {"column_name": "category_diversity"}},
        ],
        sync=True,    # block until training completes (could take hours for large budgets)
    )

    print(f"\n✓ Model trained: {model.resource_name}")
    print(f"  Display name : {model.display_name}")
    return model


def deploy_model(model: aiplatform.Model, endpoint_display_name: str) -> aiplatform.Endpoint:
    """
    Deploy a trained AutoML model to a Vertex AI prediction endpoint.

    Endpoint deployment provisions managed compute that serves online predictions
    via a REST API. The endpoint supports:
      - Batch prediction (send a BigQuery table, get scores back)
      - Online prediction (REST POST for real-time scoring)
      - Traffic splitting (canary deployments — see cloudbuild.yaml)

    Autoscaling:
      min_replica_count=1  — always keep one replica warm (low latency)
      max_replica_count=3  — scale up if QPS increases (cost-efficient ceiling)
      Vertex AI scales between min and max based on CPU utilisation.

    Args:
        model:                  The trained Vertex AI Model resource.
        endpoint_display_name:  Name for the prediction endpoint.

    Returns:
        The deployed aiplatform.Endpoint.
    """
    print(f"\nDeploying model to endpoint '{endpoint_display_name}' ...")
    endpoint = model.deploy(
        deployed_model_display_name=endpoint_display_name,
        machine_type="n1-standard-4",   # 4 vCPU, 15 GB RAM — sufficient for tabular inference
        min_replica_count=1,            # always keep one warm for low-latency prediction
        max_replica_count=3,            # autoscale up to 3 replicas under high load
        sync=True,                      # block until deployment is complete
    )
    print(f"✓ Endpoint: {endpoint.resource_name}")
    print(f"  Predict URL: https://{endpoint.location}-aiplatform.googleapis.com/"
          f"v1/{endpoint.resource_name}:predict")
    return endpoint


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train churn model with Vertex AI AutoML")
    parser.add_argument("--project",   required=True,          help="GCP project ID")
    parser.add_argument("--region",    default="us-central1",  help="Vertex AI region")
    parser.add_argument("--dataset",   default="course_dataset", help="BigQuery dataset")
    parser.add_argument("--table",     default="ml_features_churn", help="BQ feature table")
    parser.add_argument("--target",    default="churned",      help="Target column name")
    parser.add_argument("--name",      default="churn-automl-v1", help="Model display name")
    parser.add_argument("--budget",    type=int, default=1000,
                        help="Training budget in milli node-hours (1000 = ~1-2 hrs)")
    parser.add_argument("--deploy",    action="store_true",
                        help="Deploy the model to a prediction endpoint after training")
    args = parser.parse_args()

    model = train_automl(
        args.project, args.region, args.dataset,
        args.table, args.target, args.name, args.budget,
    )

    if args.deploy:
        deploy_model(model, f"{args.name}-endpoint")
