"""
Phase 04 — LAB 4: Streaming Pipeline with Apache Beam / Cloud Dataflow
========================================================================
Reads JSON events from Pub/Sub in real time, validates and enriches them,
then writes valid records to BigQuery and malformed ones to a dead-letter table.

Key concepts covered:
  - Apache Beam unified batch+streaming model
  - TaggedOutput pattern for fan-out (valid vs dead-letter routing)
  - BigQuery streaming inserts via WriteToBigQuery
  - Runner portability: same code works locally (DirectRunner) and on
    Cloud Dataflow (DataflowRunner) without changes

Architecture:
  Pub/Sub topic
      │
      ▼
  ReadFromPubSub (unbounded PCollection of bytes)
      │
      ▼
  ParseMessage (DoFn with TaggedOutput)
      ├── VALID ──▶ BigQuery: dataset.events
      └── DEAD  ──▶ BigQuery: dataset.events_dead_letter

Run locally (DirectRunner) — no GCP resources needed:
  python beam_pipeline.py --runner=DirectRunner \
      --project=YOUR_PROJECT \
      --input_topic=projects/PROJECT/topics/raw-events \
      --output_table=PROJECT:DATASET.events

Run on Cloud Dataflow:
  python beam_pipeline.py --runner=DataflowRunner \
      --project=YOUR_PROJECT --region=us-central1 \
      --temp_location=gs://BUCKET/dataflow-temp \
      --staging_location=gs://BUCKET/dataflow-staging \
      --input_topic=projects/PROJECT/topics/raw-events \
      --output_table=PROJECT:DATASET.events
"""
import json
import logging
import argparse
from datetime import datetime, UTC

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


# ── BigQuery schema ───────────────────────────────────────────────────────────
# Must match the columns produced by ParseMessage.process() and the table
# DDL in phase-05-bigquery/sql/01_create_tables.sql.
# 'ingested_at' is added by the pipeline (not in the source event) as an
# audit timestamp so we know exactly when data entered the system.
BQ_SCHEMA = {
    "fields": [
        {"name": "event_id",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "event_type",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "timestamp",   "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "user_id",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "product_id",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "product",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "category",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "quantity",    "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "total",       "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},  # pipeline-added
    ]
}

# Fields that MUST be present for a record to be considered valid.
# Missing any of these → dead-letter table.
REQUIRED_FIELDS = {"event_id", "event_type", "timestamp"}


class ParseMessage(beam.DoFn):
    """
    Parse raw Pub/Sub bytes into a Python dict and route the result.

    Why TaggedOutput?
    A standard DoFn can only yield to a single output PCollection.
    TaggedOutput lets one DoFn emit to multiple named outputs ("tags").
    Here we use two tags:
      VALID      → well-formed events → BigQuery main table
      DEAD_LETTER → malformed events → BigQuery dead-letter table

    This pattern guarantees no message is silently dropped — every byte
    that enters the pipeline exits through exactly one of the two paths.
    The dead-letter table is your debugging surface for data quality issues.
    """

    VALID = "valid"
    DEAD  = "dead_letter"

    def process(self, message: bytes, timestamp=beam.DoFn.TimestampParam):
        """
        Process a single Pub/Sub message.

        Args:
            message:   Raw bytes from Pub/Sub (the message data field).
            timestamp: Beam event-time timestamp injected by the framework.
                       Not used here but available for windowing.

        Yields:
            TaggedOutput to either VALID or DEAD depending on parse result.
        """
        try:
            # Decode bytes → string → dict
            record = json.loads(message.decode("utf-8"))

            # Validate required fields are present
            missing = REQUIRED_FIELDS - record.keys()
            if missing:
                raise ValueError(f"Missing required fields: {missing}")

            # Enrich: add the pipeline ingestion timestamp
            # ISO-8601 format is accepted directly by BigQuery TIMESTAMP columns
            record["ingested_at"] = datetime.now(UTC).isoformat()

            # Emit to the VALID output — will be written to BQ main table
            yield beam.pvalue.TaggedOutput(self.VALID, record)

        except Exception as exc:
            # Any parse or validation failure → dead-letter
            # Truncate raw message to 1024 chars to stay within BQ STRING limits
            logging.warning("Dead-letter: %s  |  raw=%s", exc, message[:200])
            yield beam.pvalue.TaggedOutput(self.DEAD, {
                "raw":       message.decode("utf-8", errors="replace")[:1024],
                "error":     str(exc),
                "failed_at": datetime.now(UTC).isoformat(),
            })


def build_pipeline(args) -> None:
    """
    Assemble and run the streaming Beam pipeline.

    Pipeline graph:
      ReadFromPubSub → ParseMessage → [fork]
        ├── valid records    → WriteToBigQuery (main table)
        └── dead-letter recs → WriteToBigQuery (dead_letter table)

    The dead-letter table name is derived by replacing "events" with
    "events_dead_letter" in the output table argument. This convention
    keeps both tables in the same dataset for easy querying.

    Args:
        args: Parsed argparse namespace with all pipeline parameters.
    """
    # PipelineOptions holds all runner-specific settings (project, region, etc.)
    options = PipelineOptions(
        project          = args.project,
        region           = getattr(args, "region", "us-central1"),
        temp_location    = getattr(args, "temp_location", None),
        staging_location = getattr(args, "staging_location", None),
        streaming        = True,         # required for Pub/Sub sources
        save_main_session = True,        # serialise globals so workers get them
    )
    options.view_as(StandardOptions).streaming = True

    # Convention: dead-letter table lives beside the main table in same dataset
    dead_letter_table = args.output_table.replace("events", "events_dead_letter")

    with beam.Pipeline(options=options) as p:

        # ── Step 1: Read from Pub/Sub ─────────────────────────────────────
        # ReadFromPubSub creates an unbounded PCollection — each element is
        # the raw bytes of one Pub/Sub message. The pipeline runs forever
        # until cancelled (Dataflow) or Ctrl+C (DirectRunner).
        parsed = (
            p
            | "ReadPubSub"    >> beam.io.ReadFromPubSub(topic=args.input_topic)

            # ── Step 2: Parse and route ───────────────────────────────────
            # with_outputs() activates multi-tag mode; returns a DoOutputsTuple
            # that can be indexed by tag name: parsed["valid"], parsed["dead_letter"]
            | "ParseMessages" >> beam.ParDo(ParseMessage()).with_outputs(
                ParseMessage.VALID,
                ParseMessage.DEAD,
            )
        )

        # ── Step 3a: Write valid records to BigQuery ──────────────────────
        # WRITE_APPEND — new rows added to existing data (streaming inserts)
        # CREATE_IF_NEEDED — Beam creates the table on first run using BQ_SCHEMA
        (
            parsed[ParseMessage.VALID]
            | "WriteToBQ" >> WriteToBigQuery(
                table               = args.output_table,
                schema              = BQ_SCHEMA,
                write_disposition   = BigQueryDisposition.WRITE_APPEND,
                create_disposition  = BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # ── Step 3b: Write bad records to dead-letter table ───────────────
        # Keeps malformed data visible for debugging without polluting the
        # main table. Query dead_letter daily to catch upstream data quality issues.
        (
            parsed[ParseMessage.DEAD]
            | "WriteDeadLetter" >> WriteToBigQuery(
                table = dead_letter_table,
                schema = {
                    "fields": [
                        {"name": "raw",       "type": "STRING",    "mode": "NULLABLE"},  # original bytes (truncated)
                        {"name": "error",     "type": "STRING",    "mode": "NULLABLE"},  # exception message
                        {"name": "failed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},  # when it failed
                    ]
                },
                write_disposition  = BigQueryDisposition.WRITE_APPEND,
                create_disposition = BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser(description="Apache Beam streaming pipeline: Pub/Sub → BigQuery")
    parser.add_argument("--project",          required=True,
                        help="GCP project ID")
    parser.add_argument("--input_topic",      required=True,
                        help="Full Pub/Sub topic path: projects/PROJECT/topics/TOPIC")
    parser.add_argument("--output_table",     required=True,
                        help="BigQuery destination: PROJECT:DATASET.TABLE")
    parser.add_argument("--region",           default="us-central1",
                        help="Dataflow worker region")
    parser.add_argument("--temp_location",    default=None,
                        help="GCS path for Dataflow temp files: gs://BUCKET/temp")
    parser.add_argument("--staging_location", default=None,
                        help="GCS path for Dataflow staging: gs://BUCKET/staging")
    parser.add_argument("--runner",           default="DataflowRunner",
                        help="Beam runner: DataflowRunner (cloud) or DirectRunner (local)")

    # parse_known_args separates our args from Beam pipeline args (--num_workers etc.)
    args, pipeline_args = parser.parse_known_args()

    # Remaining args go into sys.argv so Beam's PipelineOptions can pick them up
    import sys
    sys.argv = [sys.argv[0]] + pipeline_args

    build_pipeline(args)
