"""
Phase 02 — LAB 2B: Pub/Sub Event Publisher (real-time stream simulator)
========================================================================
Simulates a live e-commerce event stream by publishing JSON messages to
a Cloud Pub/Sub topic at a configurable rate.

Run this alongside the Dataflow streaming pipeline (Phase 04) to see the
full real-time data flow: publisher → Pub/Sub → Dataflow → BigQuery.

Architecture:
  [This script] → Pub/Sub topic → Dataflow (beam_pipeline.py) → BigQuery

Usage:
  # Publish 1 event/second indefinitely
  python pubsub_publisher.py --project my-project --topic raw-events

  # Publish 5 events/second, stop after 100 messages
  python pubsub_publisher.py --project my-project --topic raw-events \
      --rate 5 --limit 100
"""
import json
import time
import random
import argparse
from datetime import datetime, UTC
from google.cloud import pubsub_v1


# ── Product catalogue ─────────────────────────────────────────────────────────
# Simulated product inventory — events will randomly reference these items.
# In a real system this would come from a product database or catalogue API.
PRODUCT_CATALOG = [
    {"id": "P001", "name": "Widget A",       "price": 29.99,  "category": "electronics"},
    {"id": "P002", "name": "Gadget B",       "price": 59.99,  "category": "electronics"},
    {"id": "P003", "name": "Doohickey C",    "price": 14.99,  "category": "accessories"},
    {"id": "P004", "name": "Thingamajig D",  "price": 99.99,  "category": "premium"},
]


def generate_event() -> dict:
    """
    Generate a single synthetic e-commerce event.

    Event types:
      ORDER_PLACED   — customer placed an order (majority of events)
      ORDER_SHIPPED  — order dispatched from warehouse
      ORDER_CANCELLED — order cancelled before shipment

    The event schema matches the BigQuery table schema in Phase 05
    and the Beam pipeline's BQ_SCHEMA in Phase 04.
    """
    product = random.choice(PRODUCT_CATALOG)
    qty = random.randint(1, 5)

    return {
        "event_id":   f"EVT-{random.randint(100000, 999999)}",   # unique event identifier
        "event_type": random.choices(
            ["ORDER_PLACED", "ORDER_SHIPPED", "ORDER_CANCELLED"],
            weights=[70, 20, 10],   # weighted: most events are placements
        )[0],
        "timestamp":  datetime.now(UTC).isoformat(),             # ISO-8601 UTC timestamp
        "user_id":    f"USR-{random.randint(1000, 9999)}",       # anonymised user reference
        "product_id": product["id"],
        "product":    product["name"],
        "category":   product["category"],
        "quantity":   qty,
        "total":      round(product["price"] * qty, 2),          # line total in USD
    }


def publish(
    project_id: str,
    topic_id: str,
    rate: float,
    limit: int,
) -> None:
    """
    Publish synthetic events to a Pub/Sub topic.

    How it works:
      1. Creates a PublisherClient (handles batching, retries, flow control).
      2. Loops indefinitely (or up to `limit` messages).
      3. Each message carries the event JSON as bytes plus attribute metadata
         that Pub/Sub can filter on (useful for fan-out subscriptions).
      4. `future.result()` blocks until Pub/Sub acknowledges the message,
         ensuring no message is silently dropped.
      5. Ctrl+C cleanly stops the loop and prints a summary.

    Args:
        project_id: GCP project ID.
        topic_id:   Pub/Sub topic name (not the full path).
        rate:       Target publish rate in messages per second.
        limit:      Stop after this many messages; 0 = run forever.
    """
    publisher  = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)  # projects/P/topics/T

    print(f"Publishing to {topic_path}  ({rate} msg/s  |  limit={limit or '∞'})")
    print("Press Ctrl+C to stop.\n")
    count = 0

    try:
        while limit == 0 or count < limit:
            event = generate_event()

            # Pub/Sub messages are raw bytes — JSON is the conventional format
            data = json.dumps(event).encode("utf-8")

            # Publish with message attributes for potential subscription filters
            # e.g. a subscription filtered to event_type="ORDER_PLACED" only
            future = publisher.publish(
                topic_path,
                data=data,
                event_type=event["event_type"],  # attribute (not part of body)
                source="lab-publisher",           # tag for debugging
            )
            future.result()  # block until Pub/Sub acks — guarantees at-least-once delivery

            count += 1
            print(
                f"  [{count:05d}] {event['event_id']}  "
                f"{event['event_type']:<20}  "
                f"${event['total']:.2f}"
            )

            # Throttle to stay near the requested rate
            time.sleep(1 / rate)

    except KeyboardInterrupt:
        print(f"\n✓ Stopped after {count} messages published to {topic_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Publish synthetic e-commerce events to Pub/Sub")
    parser.add_argument("--project", required=True,             help="GCP project ID")
    parser.add_argument("--topic",   default="raw-events",      help="Pub/Sub topic name")
    parser.add_argument("--rate",    type=float, default=1.0,   help="Messages per second (default: 1)")
    parser.add_argument("--limit",   type=int,   default=0,     help="Max messages; 0 = run forever")
    args = parser.parse_args()

    publish(args.project, args.topic, args.rate, args.limit)
