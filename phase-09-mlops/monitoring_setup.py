"""
Phase 09 — LAB 9B: Set up Vertex AI Model Monitoring on a deployed endpoint
============================================================================
Configures continuous monitoring of a live prediction endpoint to detect
degradation in input data quality, training-serving skew, and prediction drift.

What is model monitoring and why does it matter?
  A model trained on historical data will eventually become stale as the world
  changes. Two failure modes are especially common in production:

  1. Training-serving SKEW: the feature distributions at serving time differ
     from the training data. Causes: data pipeline bug, schema change upstream,
     a new business process that changes customer behaviour.
     Example: 'days_since_last_order' spikes after a holiday period but the
     model was trained on normal-season data → predictions become unreliable.

  2. Prediction DRIFT: the model's output distribution shifts over time even
     if inputs look normal. Causes: the real-world relationship between features
     and the target changes (concept drift).
     Example: customers who used to churn at 60 days now churn at 30 days
     due to a competitor entering the market → threshold tuning is needed.

  Vertex AI Model Monitoring samples a configurable fraction of live prediction
  requests, compares their feature distributions against the training dataset,
  and alerts via email (or Cloud Monitoring) when a statistical threshold is crossed.

How it works (Vertex AI architecture):
  1. Monitoring samples 20% of inference requests (sample_rate=0.2) and logs
     them to a BigQuery or GCS table.
  2. On a schedule (configurable, default: 24 hours), Vertex AI runs a batch
     job that computes distributional statistics over the sampled requests.
  3. It compares those distributions against the training dataset (bq_training_uri).
  4. If Jensen-Shannon divergence exceeds the threshold, it sends an email alert.

Usage:
  # Minimal setup — monitor an endpoint with default thresholds
  python monitoring_setup.py \\
    --project my-project \\
    --endpoint projects/123/locations/us-central1/endpoints/456 \\
    --bq_uri bq://my-project.course_dataset.ml_features_churn \\
    --email ops-team@example.com

  # Tighter thresholds (0.1 = warn on small distributional changes)
  python monitoring_setup.py ... --skew 0.1 --drift 0.1
"""
import argparse
from google.cloud import aiplatform
from google.cloud.aiplatform_v1beta1.services.model_monitoring_service import (
    ModelMonitoringServiceClient,
)
from google.cloud.aiplatform_v1beta1.types import model_monitoring_service


def setup_monitoring(
    project:            str,
    region:             str,
    endpoint_id:        str,
    bq_training_uri:    str,
    notification_email: str,
    skew_threshold:     float = 0.3,
    drift_threshold:    float = 0.3,
) -> dict:
    """
    Build a Vertex AI Model Monitoring configuration dict for the given endpoint.

    This function constructs the monitoring config and prints it. Vertex AI
    Model Monitoring v2 (GA as of 2024) is configured through the
    ModelMonitoringServiceClient or via the Vertex AI console. The returned
    config dict can be passed to the client's create_model_monitor() method.

    Threshold interpretation (Jensen-Shannon divergence):
      JS divergence measures the difference between two probability distributions.
      Range: 0 (identical) to 1 (completely different).
      Typical values:
        0.1 — tight: alert on subtle distributional shifts (noisy in production)
        0.3 — balanced: alert on meaningful shifts while ignoring sampling noise
        0.5 — loose: alert only on severe distributional changes

    Monitored features (subset — not all features need equal attention):
      total_orders          — key engagement signal; sudden drops = data pipeline issues
      lifetime_value        — revenue signal; spikes may indicate segment shift
      days_since_last_order — most important churn feature; drift here is critical

    Args:
        project:            GCP project ID.
        region:             Vertex AI region (must match endpoint region).
        endpoint_id:        Endpoint resource name or short numeric ID.
        bq_training_uri:    BigQuery table with training data used as the baseline.
                            Format: bq://PROJECT.DATASET.TABLE
        notification_email: Email address for monitoring alerts.
        skew_threshold:     JS divergence threshold for training-serving skew alerts.
        drift_threshold:    JS divergence threshold for prediction drift alerts.

    Returns:
        The monitoring configuration dict (pass to ModelMonitoringServiceClient).
    """
    # Initialise Vertex AI SDK — uses ADC for authentication
    aiplatform.init(project=project, location=region)

    # Resolve the endpoint resource name if a short ID was provided
    endpoint = aiplatform.Endpoint(endpoint_id)
    print(f"Setting up monitoring on: {endpoint.resource_name}")

    # ── Monitoring configuration ───────────────────────────────────────────────
    # model_monitoring_objective_configs is a list: one entry per deployed model.
    # endpoint.list_models() returns all DeployedModel objects on the endpoint.
    # In a canary deployment, there are multiple deployed models with different
    # traffic weights; we monitor all of them.
    monitoring_config = {
        "display_name": f"churn-monitoring-{endpoint_id[:8]}",
        "endpoint":     endpoint.resource_name,

        # One monitoring objective per deployed model on the endpoint
        "model_monitoring_objective_configs": [
            {
                "deployed_model_id": dm.id,  # ID of this specific DeployedModel
                "objective_config": {

                    # Training dataset: used as the baseline distribution.
                    # The monitoring job computes statistics over this table
                    # and compares them against sampled serving requests.
                    "training_dataset": {
                        "data_format":     "bigquery",
                        "bigquery_source": {"input_uri": bq_training_uri},
                        "target_field":    "churned",   # label column (excluded from skew check)
                    },

                    # Skew detection: compare training distribution vs live requests.
                    # Only the most impactful features are monitored to reduce
                    # false-positive alert volume.
                    "training_prediction_skew_detection_config": {
                        "skew_thresholds": {
                            # Key engagement feature: sharp drops indicate pipeline failure
                            "total_orders": {"value": skew_threshold},
                            # Revenue feature: spikes indicate premium customer segment shift
                            "lifetime_value": {"value": skew_threshold},
                            # Most important churn signal: monitor closely
                            "days_since_last_order": {"value": skew_threshold},
                        }
                    },

                    # Drift detection: monitor prediction score distribution over time.
                    # If the model starts predicting churn much more (or less) than before,
                    # the underlying relationship between features and labels has changed.
                    "prediction_drift_detection_config": {
                        "drift_thresholds": {
                            "total_orders":          {"value": drift_threshold},
                            "days_since_last_order": {"value": drift_threshold},
                        }
                    },
                },
            }
            for dm in endpoint.list_models()  # iterate all deployed models on the endpoint
        ],

        # Sampling: log 20% of prediction requests for monitoring.
        # Lower sampling = lower cost but less statistical power.
        # 20% is standard for moderate-traffic endpoints (~1000+ predictions/day).
        "logging_sampling_strategy": {
            "random_sample_config": {"sample_rate": 0.2}  # sample 20% of requests
        },

        # Alert configuration: send email when a threshold is violated.
        # enable_logging=True also writes alerts to Cloud Logging for dashboards.
        "model_monitoring_alert_config": {
            "email_alert_config": {"user_emails": [notification_email]},
            "enable_logging": True,   # write alerts to Cloud Logging (for dashboard/alerting)
        },

        # Schema URIs: Vertex AI can validate prediction input/output schemas.
        # Leave empty to skip schema validation (acceptable for tabular models).
        "predict_instance_schema_uri":  "",
        "analysis_instance_schema_uri": "",
    }

    # ── Summary ───────────────────────────────────────────────────────────────
    print("Model monitoring config created.")
    print(f"  Skew threshold:  {skew_threshold}  (JS divergence vs training data)")
    print(f"  Drift threshold: {drift_threshold}  (JS divergence over time)")
    print(f"  Sample rate:     20% of live predictions")
    print(f"  Alert email:     {notification_email}")
    print("")
    print("To apply via Vertex AI console:")
    print("  Vertex AI → Endpoints → your endpoint → Monitoring tab → Enable")
    print("")
    print("To apply programmatically (Vertex AI v1beta1 API):")
    print("  client = ModelMonitoringServiceClient()")
    print("  client.create_model_monitor(parent=..., model_monitor=monitoring_config)")

    return monitoring_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Set up Vertex AI Model Monitoring for the churn endpoint"
    )
    parser.add_argument("--project",   required=True,
                        help="GCP project ID")
    parser.add_argument("--region",    default="us-central1",
                        help="Vertex AI region (must match endpoint region)")
    parser.add_argument("--endpoint",  required=True,
                        help="Endpoint resource name or short numeric ID")
    parser.add_argument("--bq_uri",    required=True,
                        help="Training data BQ URI: bq://PROJECT.DATASET.TABLE")
    parser.add_argument("--email",     required=True,
                        help="Alert notification email address")
    parser.add_argument("--skew",      type=float, default=0.3,
                        help="JS divergence threshold for training-serving skew (0-1)")
    parser.add_argument("--drift",     type=float, default=0.3,
                        help="JS divergence threshold for prediction drift (0-1)")
    args = parser.parse_args()

    setup_monitoring(
        project=args.project,
        region=args.region,
        endpoint_id=args.endpoint,
        bq_training_uri=args.bq_uri,
        notification_email=args.email,
        skew_threshold=args.skew,
        drift_threshold=args.drift,
    )
