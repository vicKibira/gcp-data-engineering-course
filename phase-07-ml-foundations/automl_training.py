"""
Phase 07 — LAB 7: Train a classification model with Vertex AI AutoML
Uses the churn features table in BigQuery as the training source.
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

    aiplatform.init(project=project, location=region)

    # ── 1. Create a Vertex AI Dataset from BigQuery ───────────────────────────
    print(f"Creating Vertex dataset from bq://{project}.{dataset}.{bq_table} ...")
    vertex_dataset = aiplatform.TabularDataset.create_from_dataframe(
        display_name=f"churn-features-{bq_table}",
        bq_source=f"bq://{project}.{dataset}.{bq_table}",
    )
    print(f"  Dataset: {vertex_dataset.resource_name}")

    # ── 2. Define the AutoML job ──────────────────────────────────────────────
    job = aiplatform.AutoMLTabularTrainingJob(
        display_name=model_display_name,
        optimization_prediction_type="classification",
        optimization_objective="maximize-au-roc",
    )

    # ── 3. Run training ───────────────────────────────────────────────────────
    print(f"Starting AutoML training (budget={budget_milli_node_hours} milli node-hours)...")
    model = job.run(
        dataset=vertex_dataset,
        target_column=target_column,
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
        sync=True,
    )

    print(f"\n✓ Model trained: {model.resource_name}")
    print(f"  Display name : {model.display_name}")
    return model


def deploy_model(model: aiplatform.Model, endpoint_display_name: str) -> aiplatform.Endpoint:
    print(f"\nDeploying model to endpoint '{endpoint_display_name}' ...")
    endpoint = model.deploy(
        deployed_model_display_name=endpoint_display_name,
        machine_type="n1-standard-4",
        min_replica_count=1,
        max_replica_count=3,
        sync=True,
    )
    print(f"✓ Endpoint: {endpoint.resource_name}")
    return endpoint


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",   required=True)
    parser.add_argument("--region",    default="us-central1")
    parser.add_argument("--dataset",   default="course_dataset")
    parser.add_argument("--table",     default="ml_features_churn")
    parser.add_argument("--target",    default="churned")
    parser.add_argument("--name",      default="churn-automl-v1")
    parser.add_argument("--budget",    type=int, default=1000)
    parser.add_argument("--deploy",    action="store_true")
    args = parser.parse_args()

    model = train_automl(
        args.project, args.region, args.dataset,
        args.table, args.target, args.name, args.budget,
    )

    if args.deploy:
        deploy_model(model, f"{args.name}-endpoint")
