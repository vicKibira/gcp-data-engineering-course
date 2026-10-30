"""
Phase 02 — Pub/Sub event publisher (simulates a real-time event stream).
Run this alongside pubsub_subscriber.py or the Dataflow pipeline.
"""
import json
import time
import random
import argparse
from datetime import datetime, UTC
from google.cloud import pubsub_v1


PRODUCT_CATALOG = [
    {"id": "P001", "name": "Widget A", "price": 29.99, "category": "electronics"},
    {"id": "P002", "name": "Gadget B", "price": 59.99, "category": "electronics"},
    {"id": "P003", "name": "Doohickey C", "price": 14.99, "category": "accessories"},
    {"id": "P004", "name": "Thingamajig D", "price": 99.99, "category": "premium"},
]


def generate_event() -> dict:
    product = random.choice(PRODUCT_CATALOG)
    qty = random.randint(1, 5)
    return {
        "event_id":   f"EVT-{random.randint(100000, 999999)}",
        "event_type": random.choice(["ORDER_PLACED", "ORDER_SHIPPED", "ORDER_CANCELLED"]),
        "timestamp":  datetime.now(UTC).isoformat(),
        "user_id":    f"USR-{random.randint(1000, 9999)}",
        "product_id": product["id"],
        "product":    product["name"],
        "category":   product["category"],
        "quantity":   qty,
        "total":      round(product["price"] * qty, 2),
    }


def publish(project_id: str, topic_id: str, rate: float, limit: int) -> None:
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)

    print(f"Publishing to {topic_path}  ({rate} msg/s  |  limit={limit or '∞'})")
    count = 0

    try:
        while limit == 0 or count < limit:
            event = generate_event()
            data = json.dumps(event).encode("utf-8")

            future = publisher.publish(
                topic_path,
                data=data,
                event_type=event["event_type"],
                source="lab-publisher",
            )
            future.result()  # wait for ack

            count += 1
            print(f"  [{count:05d}] {event['event_id']}  {event['event_type']}  ${event['total']:.2f}")
            time.sleep(1 / rate)

    except KeyboardInterrupt:
        print(f"\nStopped — {count} messages published")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--project",  required=True)
    parser.add_argument("--topic",    default="raw-events")
    parser.add_argument("--rate",     type=float, default=1.0,  help="Messages per second")
    parser.add_argument("--limit",    type=int,   default=0,    help="0 = run forever")
    args = parser.parse_args()

    publish(args.project, args.topic, args.rate, args.limit)
