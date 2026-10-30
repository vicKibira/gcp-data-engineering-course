"""
Phase 09 — Set up Vertex AI Model Monitoring on a deployed endpoint.
Detects prediction drift, feature skew, and data quality issues.
"""
import argparse
from google.cloud import aiplatform
from google.cloud.aiplatform_v1beta1.services.model_monitoring_service import (
    ModelMonitoringServiceClient,
)
from google.cloud.aiplatform_v1beta1.types import model_monitoring_service


def setup_monitoring(
    project:         str,
    region:          str,
    endpoint_id:     str,
    bq_training_uri: str,
    notification_email: str,
    skew_threshold:  float = 0.3,
    drift_threshold: float = 0.3,
) -> None:
    aiplatform.init(project=project, location=region)

    endpoint = aiplatform.Endpoint(endpoint_id)
    print(f"Setting up monitoring on: {endpoint.resource_name}")

    # Vertex AI Model Monitoring v2 (GA) config
    monitoring_config = {
        "display_name": f"churn-monitoring-{endpoint_id[:8]}",
        "endpoint":     endpoint.resource_name,
        "model_monitoring_objective_configs": [
            {
                "deployed_model_id": dm.id,
                "objective_config": {
                    "training_dataset": {
                        "data_format":     "bigquery",
                        "bigquery_source": {"input_uri": bq_training_uri},
                        "target_field":    "churned",
                    },
                    "training_prediction_skew_detection_config": {
                        "skew_thresholds": {
                            "total_orders":           {"value": skew_threshold},
                            "lifetime_value":         {"value": skew_threshold},
                            "days_since_last_order":  {"value": skew_threshold},
                        }
                    },
                    "prediction_drift_detection_config": {
                        "drift_thresholds": {
                            "total_orders":          {"value": drift_threshold},
                            "days_since_last_order": {"value": drift_threshold},
                        }
                    },
                },
            }
            for dm in endpoint.list_models()
        ],
        "logging_sampling_strategy": {"random_sample_config": {"sample_rate": 0.2}},
        "model_monitoring_alert_config": {
            "email_alert_config": {"user_emails": [notification_email]},
            "enable_logging":     True,
        },
        "predict_instance_schema_uri": "",
        "analysis_instance_schema_uri": "",
    }

    print("Model monitoring config created.")
    print(f"  Skew threshold:  {skew_threshold}")
    print(f"  Drift threshold: {drift_threshold}")
    print(f"  Alert email:     {notification_email}")
    print("\nDeploy via Vertex AI console → Endpoints → your endpoint → Monitoring tab")
    print("Or use the Vertex AI v1beta1 API to create a ModelMonitor resource.")
    return monitoring_config


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",      required=True)
    parser.add_argument("--region",       default="us-central1")
    parser.add_argument("--endpoint",     required=True, help="Endpoint resource name or ID")
    parser.add_argument("--bq_uri",       required=True,
                        help="bq://PROJECT.DATASET.ml_features_churn")
    parser.add_argument("--email",        required=True)
    parser.add_argument("--skew",         type=float, default=0.3)
    parser.add_argument("--drift",        type=float, default=0.3)
    args = parser.parse_args()

    setup_monitoring(
        args.project, args.region, args.endpoint,
        args.bq_uri, args.email, args.skew, args.drift,
    )
