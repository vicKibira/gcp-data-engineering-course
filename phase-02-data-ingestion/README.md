# Phase 02 — Data Ingestion

## Why this phase exists

Data always comes from somewhere outside your system — files from a partner, API calls from an app, events from users clicking around a website. Getting that data reliably into your GCP data lake is the first real engineering challenge.

This phase covers **two ingestion patterns** that every data engineer uses:

1. **Batch ingestion** (`gcs_ingest.py`) — load a CSV file, partition it by date, write Parquet to GCS. This is the "load a dump from the source system" pattern.
2. **Streaming ingestion** (`pubsub_publisher.py` + `pubsub_subscriber.py`) — publish events to a Pub/Sub topic in real time. This is the "capture every click/order/transaction as it happens" pattern.

Understanding when to use each pattern — and how GCP handles both — is a core data engineering skill.

---

## What you'll build

- A Pub/Sub topic (`raw-events`) that acts as the event bus for the course
- A publisher that simulates a live e-commerce event stream
- A pull subscriber that lets you inspect events before wiring up Dataflow
- Partitioned Parquet files in `gs://BUCKET/raw/` (the Bronze zone)

---

## Files

### `gcs_ingest.py`
Reads a CSV (local or URL), partitions the data by a date column, and uploads each day's data as a Parquet file under `gs://BUCKET/raw/DATASET/year=YYYY/month=MM/day=DD/data.parquet`.

**Why Parquet instead of CSV?**
- 5–10× smaller due to columnar compression
- BigQuery and Spark read it natively — no conversion step
- Supports partition pruning: a Spark or BQ query filtered to `year=2024/month=01` only opens that folder, skipping everything else

**Why Hive-style partitions (`year=/month=/day=`)?**
Both BigQuery external tables and Spark can detect this layout automatically and use it as a partition filter. It's the industry-standard convention.

**Key function — `upload_partitioned()`:**
```python
for date, group in df.groupby(df[date_col].dt.date):
    blob_path = f"{prefix}/year={date.year}/month={date.month:02d}/day={date.day:02d}/data.parquet"
    blob.upload_from_string(group.to_parquet(index=False), ...)
```
Groups rows by calendar date, serialises each group to Parquet in memory (no temp file), uploads to GCS. One partition file per day.

---

### `pubsub_publisher.py`
Simulates a live e-commerce event stream by publishing JSON messages to a Pub/Sub topic. This is the data source for the Dataflow streaming pipeline in Phase 04.

**Why Pub/Sub?**
Pub/Sub is GCP's managed messaging service — essentially a hosted Kafka. It handles:
- Durability (messages retained for 7 days by default)
- At-least-once delivery (every message is delivered at least once — duplicates possible)
- Fan-out (multiple subscribers can consume the same topic independently)
- Backpressure (producers are never blocked waiting for slow consumers)

**Key code patterns:**

`PublisherClient.publish()` is asynchronous — it returns a `Future`. Calling `.result()` blocks until Pub/Sub acknowledges the message:
```python
future = publisher.publish(topic_path, data=data, event_type=event["event_type"])
future.result()  # blocks until ack — guarantees at-least-once delivery
```

**Message attributes** (the `event_type=` kwarg) are metadata outside the message body. Pub/Sub subscriptions can filter on attributes — for example, a subscription that only delivers `ORDER_PLACED` events.

**Weighted random event types:**
```python
random.choices(["ORDER_PLACED", "ORDER_SHIPPED", "ORDER_CANCELLED"], weights=[70, 20, 10])
```
70% of events are placements, 20% are shipments, 10% are cancellations. This mirrors a realistic e-commerce distribution and gives your ML model a realistic class imbalance to handle.

---

### `pubsub_subscriber.py`
A pull-based consumer for the `raw-events` subscription. Use this to verify the publisher is working before deploying the full Dataflow pipeline.

**Pull vs Push:**
- **Pull** (this script) — your code asks Pub/Sub for messages. Good for debugging, batch consumers.
- **Push** (Dataflow/Beam in Phase 04) — Pub/Sub delivers messages to an endpoint. Good for streaming pipelines.

**Key code patterns:**

`StreamingPullFuture` opens a long-lived gRPC connection to Pub/Sub. The callback (`process_message`) runs on background threads:
```python
streaming_pull_future = subscriber.subscribe(subscription_path, callback=counting_callback)
streaming_pull_future.result()  # block until cancelled
```

**Ack vs Nack:**
- `message.ack()` — tells Pub/Sub the message was processed; removes it from the backlog
- `message.nack()` — tells Pub/Sub to redeliver the message (e.g. after a parse error)

Always call one or the other. A message neither acked nor nacked will be redelivered after the ack deadline (default 10 seconds).

---

## Prerequisites

- Phase 01 complete (GCS bucket exists, APIs enabled)
- Python environment: `pip install google-cloud-pubsub google-cloud-storage pandas pyarrow`
- `.env` file with `PROJECT_ID` and `BUCKET_NAME`

---

## Lab — Step by step

### Part A — Batch ingestion to GCS

#### Step 1 — Download sample data
```bash
# Use a public CSV dataset (or your own)
curl -o /tmp/sales.csv \
  "https://raw.githubusercontent.com/your-org/sample-data/main/sales_2024.csv"
```

#### Step 2 — Run the ingestion script
```bash
cd phase-02-data-ingestion
python gcs_ingest.py \
  --source /tmp/sales.csv \
  --bucket $BUCKET_NAME \
  --dataset sales \
  --date-col order_date
```
You should see one line per date partition, e.g.:
```
✓ gs://PROJECT-datalake/raw/sales/year=2024/month=01/day=15/data.parquet  (1,203 rows)
```

#### Step 3 — Verify in GCS
```bash
gsutil ls gs://$BUCKET_NAME/raw/sales/
```

---

### Part B — Pub/Sub streaming

#### Step 1 — Create the topic and subscription
```bash
gcloud pubsub topics create raw-events --project=$PROJECT_ID
gcloud pubsub subscriptions create raw-events-sub \
  --topic=raw-events \
  --project=$PROJECT_ID
```

#### Step 2 — Start the publisher (Terminal 1)
```bash
python pubsub_publisher.py \
  --project $PROJECT_ID \
  --topic raw-events \
  --rate 2 \
  --limit 100
```
You'll see 100 events published at 2/second.

#### Step 3 — Start the subscriber (Terminal 2)
```bash
python pubsub_subscriber.py \
  --project $PROJECT_ID \
  --subscription raw-events-sub \
  --limit 50
```
Watch events arrive in real time. Check that `event_id`, `event_type`, and `total` look correct.

#### Step 4 — Verify in the console
Go to **Pub/Sub → Topics → raw-events → Metrics**. You should see message publish/deliver rates.

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `gs://BUCKET/raw/sales/year=.../.../*.parquet` | Phase 03 (Spark ETL reads Bronze Parquet) |
| `raw-events` Pub/Sub topic | Phase 04 (Dataflow streaming pipeline subscribes to it) |
| `raw-events-sub` subscription | Phase 04 (Beam `ReadFromPubSub` uses this) |

---

## Key concepts covered

- **Batch vs streaming ingestion** — when to use each and the trade-offs
- **Pub/Sub topics and subscriptions** — one-to-many fan-out messaging
- **At-least-once delivery** — why deduplication is necessary downstream (Phase 03 `dropDuplicates`)
- **Parquet + Hive partitioning** — the standard format for data lakes
- **Pull vs push subscriptions** — choosing the right consumer model
- **Message attributes** — metadata-level filtering without parsing message bodies
