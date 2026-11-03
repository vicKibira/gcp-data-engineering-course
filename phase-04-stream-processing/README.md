# Phase 04 — Stream Processing (Dataflow + Apache Beam)

## Why this phase exists

The Spark ETL in Phase 03 runs once a day. But what if you need data in BigQuery within seconds of an event happening — for a real-time dashboard, a fraud alert, or an ML model that scores orders as they come in?

That's stream processing. This phase builds a **Dataflow streaming pipeline** that reads from the Pub/Sub topic continuously, validates and enriches every event, and writes it to BigQuery within seconds.

Apache Beam is the programming model and Dataflow is the managed execution engine. The same code runs locally for testing (`DirectRunner`) and on Cloud Dataflow for production (`DataflowRunner`) — no changes needed. That portability is what makes Beam the standard for GCP streaming.

---

## What you'll build

- A Beam pipeline that processes Pub/Sub events in real time
- Valid events → `bronze_events` BigQuery table
- Malformed events → `bronze_events_dead_letter` BigQuery table (nothing is silently dropped)

---

## Files

### `beam_pipeline.py`

**`BQ_SCHEMA`** — Defines the BigQuery table schema as a Python dict. This must match both the DDL in `phase-05-bigquery/sql/01_create_tables.sql` and the fields that `ParseMessage` emits. The `ingested_at` field is added by the pipeline (not present in the source event) as an audit timestamp.

**`REQUIRED_FIELDS`** — `{"event_id", "event_type", "timestamp"}`. Any message missing these goes to the dead-letter table. You define your own quality threshold here — tighten or loosen as needed.

**`ParseMessage` DoFn** — The core transformation. A `DoFn` (Do Function) processes one element at a time. This one uses `TaggedOutput` to route messages to two destinations:
```python
yield beam.pvalue.TaggedOutput(self.VALID, record)    # → main table
yield beam.pvalue.TaggedOutput(self.DEAD,  error_row) # → dead-letter table
```
Why `TaggedOutput`? A normal `DoFn` can only emit to one output PCollection. `TaggedOutput` lets a single transform fan out to multiple named outputs — every message exits through exactly one path, so nothing is silently dropped.

**Pipeline assembly in `build_pipeline()`:**
```python
parsed = (
    p
    | "ReadPubSub"    >> beam.io.ReadFromPubSub(topic=args.input_topic)
    | "ParseMessages" >> beam.ParDo(ParseMessage()).with_outputs(VALID, DEAD)
)
parsed[VALID] | "WriteToBQ"      >> WriteToBigQuery(main_table, ...)
parsed[DEAD]  | "WriteDeadLetter" >> WriteToBigQuery(dead_letter_table, ...)
```
The `>>` operator names each step. The pipeline runs as a DAG — Beam figures out the execution order from data dependencies.

**Runner portability:**
- `DirectRunner` — runs locally in a single Python process, no GCP resources consumed. Use for testing.
- `DataflowRunner` — submits the pipeline to Cloud Dataflow, which manages workers, autoscaling, and fault tolerance. Use for production.

The only difference is the `--runner` flag. The pipeline code itself is identical.

**`WriteToBigQuery` dispositions:**
```python
write_disposition  = BigQueryDisposition.WRITE_APPEND      # append to existing rows
create_disposition = BigQueryDisposition.CREATE_IF_NEEDED  # create table on first run
```
Beam creates the table automatically on first run using `BQ_SCHEMA`. After that, rows are streamed in using BigQuery's streaming insert API (sub-second latency).

---

## Prerequisites

- Phase 01 complete (BigQuery dataset exists)
- Phase 02 complete (`raw-events` Pub/Sub topic + `raw-events-sub` subscription exist)
- Python packages: `pip install apache-beam[gcp]`
- For Dataflow: `gs://BUCKET/dataflow-temp/` and `gs://BUCKET/dataflow-staging/` paths writable

---

## Lab — Step by step

### Part A — Local test with DirectRunner

#### Step 1 — Start the publisher first (Terminal 1)
```bash
python phase-02-data-ingestion/pubsub_publisher.py \
  --project $PROJECT_ID \
  --topic raw-events \
  --rate 1
```
Keep this running — it feeds the pipeline.

#### Step 2 — Run the pipeline locally (Terminal 2)
```bash
cd phase-04-stream-processing
python beam_pipeline.py \
  --runner DirectRunner \
  --project $PROJECT_ID \
  --input_topic projects/$PROJECT_ID/topics/raw-events \
  --output_table $PROJECT_ID:course_dataset.events
```
You'll see log lines as messages are processed. After ~30 seconds, stop with `Ctrl+C`.

#### Step 3 — Verify in BigQuery
```bash
bq query --use_legacy_sql=false \
  "SELECT COUNT(*) as rows, COUNT(DISTINCT event_id) as unique_events
   FROM \`$PROJECT_ID.course_dataset.events\`"
```
`rows` should equal `unique_events` — the pipeline deduplicates by design (Beam's exactly-once semantics on Dataflow).

#### Step 4 — Check dead-letter table
```bash
bq query --use_legacy_sql=false \
  "SELECT * FROM \`$PROJECT_ID.course_dataset.events_dead_letter\` LIMIT 5"
```
Should be empty if the publisher is sending valid events.

---

### Part B — Production deployment on Cloud Dataflow

#### Step 1 — Submit to Dataflow
```bash
python beam_pipeline.py \
  --runner DataflowRunner \
  --project $PROJECT_ID \
  --region us-central1 \
  --temp_location gs://$BUCKET_NAME/dataflow-temp \
  --staging_location gs://$BUCKET_NAME/dataflow-staging \
  --input_topic projects/$PROJECT_ID/topics/raw-events \
  --output_table $PROJECT_ID:course_dataset.events
```
Dataflow will:
1. Package your code and push it to GCS staging
2. Launch a job in the Dataflow service (~2 minutes to start)
3. Auto-scale workers based on Pub/Sub backlog size

#### Step 2 — Monitor the job
Go to **Dataflow → Jobs** in the console. Click your job to see:
- The pipeline graph (ReadPubSub → ParseMessages → WriteToBQ)
- Throughput metrics (elements/sec per step)
- Worker count (watch it scale up/down)

#### Step 3 — Cancel when done
```bash
# List running jobs
gcloud dataflow jobs list --region=us-central1 --status=active

# Cancel by job ID
gcloud dataflow jobs cancel JOB_ID --region=us-central1
```
**Important:** Dataflow is billed by worker-hour. Always cancel streaming jobs when done with the lab.

---

## What this produces for downstream phases

| Output | Used by |
|---|---|
| `bronze_events` BigQuery table | Phase 05 (dbt Bronze model is a view over this, SQL analytics queries it) |
| `events_dead_letter` table | Phase 05 (data quality monitoring) |

The `bronze_events` table is the starting point of the entire BigQuery analytics chain. Phase 05's dbt Bronze model is literally just `SELECT * FROM bronze_events` — it exists to document the table in the dbt lineage graph.

---

## Key concepts covered

- **Apache Beam unified model** — same code for batch and streaming, local and cloud
- **PCollection** — Beam's distributed dataset abstraction (bounded for batch, unbounded for streaming)
- **DoFn** — the unit of processing in Beam (one element in, N elements out)
- **TaggedOutput** — fan-out from a single transform to multiple named outputs
- **Dead-letter pattern** — routing malformed records to a separate table for debugging
- **DirectRunner vs DataflowRunner** — local testing vs managed production execution
- **BigQuery streaming inserts** — sub-second write latency via the streaming API
- **At-least-once vs exactly-once** — what Pub/Sub guarantees and what Dataflow adds
