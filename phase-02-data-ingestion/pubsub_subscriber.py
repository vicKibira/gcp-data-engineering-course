"""
Phase 02 — LAB 2C: Pub/Sub Subscriber (pull-based consumer)
============================================================
Demonstrates how to consume messages from a Pub/Sub subscription using
the pull model. Use this for local testing / debugging of the event stream
before deploying the full Dataflow pipeline in Phase 04.

Two consumption models in Pub/Sub:
  Pull  (this script) — your code calls the API to fetch messages.
        Good for: batch consumers, on-demand processing, debugging.
  Push  (Dataflow/Beam) — Pub/Sub delivers messages to an HTTP endpoint.
        Good for: streaming pipelines, serverless functions.

Usage:
  # Create a subscription first (or use the one Terraform created)
  gcloud pubsub subscriptions create raw-events-sub --topic=raw-events

  # Run the subscriber
  python pubsub_subscriber.py --project my-project --subscription raw-events-sub

  # Run with a message limit (useful for integration tests)
  python pubsub_subscriber.py --project my-project \
      --subscription raw-events-sub --limit 50
"""
import json
import argparse
from datetime import datetime, UTC
from concurrent.futures import TimeoutError

from google.cloud import pubsub_v1


def process_message(message: pubsub_v1.subscriber.message.Message) -> None:
    """
    Callback invoked for every message delivered by the subscriber.

    Pub/Sub guarantees at-least-once delivery — a message may be delivered
    more than once if the previous ack timed out. Your processing logic
    should be idempotent (safe to run twice on the same message).

    Always call message.ack() or message.nack():
      ack()  — tells Pub/Sub the message was processed; it won't be redelivered.
      nack() — tells Pub/Sub to redeliver the message (e.g. after a processing error).

    Args:
        message: The Pub/Sub message object containing data and attributes.
    """
    try:
        # Decode the raw bytes back to a Python dict
        payload = json.loads(message.data.decode("utf-8"))

        # Print a human-readable summary of the event
        print(
            f"  [{datetime.now(UTC).strftime('%H:%M:%S')}]  "
            f"ID={payload.get('event_id', '?'):<15}  "
            f"TYPE={payload.get('event_type', '?'):<20}  "
            f"USER={payload.get('user_id', '?')}  "
            f"TOTAL=${payload.get('total', 0):.2f}"
        )

        # TODO (Phase 04): Replace this print with your Beam/Dataflow pipeline.
        # For now we're just consuming and acknowledging to drain the topic.

        # Acknowledge the message — removes it from the subscription backlog
        message.ack()

    except (json.JSONDecodeError, UnicodeDecodeError) as exc:
        # Malformed message — nack so it goes to the dead-letter subscription
        # if one is configured; otherwise it will be retried until expiry.
        print(f"  [ERROR] Could not parse message {message.message_id}: {exc}")
        message.nack()


def subscribe(
    project_id: str,
    subscription_id: str,
    timeout: float,
    limit: int,
) -> None:
    """
    Stream messages from a Pub/Sub subscription using the streaming pull API.

    The subscriber client opens a long-lived bidirectional gRPC stream to
    Pub/Sub and delivers messages to the `process_message` callback concurrently
    on a thread pool. The default thread count is max(10, cpu_count * 5).

    Flow control settings cap how many messages are buffered in-flight so
    the subscriber doesn't get overwhelmed if the publisher bursts.

    Args:
        project_id:       GCP project ID.
        subscription_id:  Pub/Sub subscription name (not the full path).
        timeout:          Seconds to wait for messages before exiting.
                          Use 0 for indefinite (Ctrl+C to stop).
        limit:            Stop after receiving this many messages; 0 = no limit.
    """
    subscriber = pubsub_v1.SubscriberClient()
    subscription_path = subscriber.subscription_path(project_id, subscription_id)

    # Flow control: cap in-flight messages to avoid memory pressure
    # If the pipeline falls behind, Pub/Sub will slow down delivery
    flow_control = pubsub_v1.types.FlowControl(
        max_messages=100,         # max unacknowledged messages in memory
        max_bytes=10 * 1024 * 1024,  # 10 MB max buffer size
    )

    print(f"Listening on {subscription_path}")
    print(f"  timeout={timeout or '∞'}s  |  limit={limit or '∞'} messages")
    print("  Press Ctrl+C to stop.\n")

    # Track how many messages we've received (for the --limit flag)
    received_count = [0]
    streaming_pull_future = None

    def counting_callback(message: pubsub_v1.subscriber.message.Message) -> None:
        """Wrapper that adds counting logic around process_message."""
        process_message(message)
        received_count[0] += 1
        # Cancel the streaming pull once we've hit the limit
        if limit and received_count[0] >= limit:
            streaming_pull_future.cancel()

    # Open the streaming pull — this is non-blocking; the callback runs
    # on background threads managed by the subscriber client
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=counting_callback,
        flow_control=flow_control,
    )

    try:
        if timeout:
            # Block for `timeout` seconds, then exit cleanly
            streaming_pull_future.result(timeout=timeout)
        else:
            # Block indefinitely until Ctrl+C or limit is hit
            streaming_pull_future.result()

    except (TimeoutError, KeyboardInterrupt):
        streaming_pull_future.cancel()  # close the gRPC stream
        streaming_pull_future.result()  # wait for in-flight callbacks to finish

    finally:
        subscriber.close()
        print(f"\n✓ Subscriber stopped — {received_count[0]} messages received")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Pull messages from a Pub/Sub subscription")
    parser.add_argument("--project",       required=True,            help="GCP project ID")
    parser.add_argument("--subscription",  default="raw-events-sub", help="Pub/Sub subscription name")
    parser.add_argument("--timeout",       type=float, default=0,    help="Stop after N seconds; 0 = forever")
    parser.add_argument("--limit",         type=int,   default=0,    help="Stop after N messages; 0 = forever")
    args = parser.parse_args()

    subscribe(args.project, args.subscription, args.timeout, args.limit)
