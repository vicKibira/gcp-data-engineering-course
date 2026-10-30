"""
Phase 04 — LAB 4: Streaming Pipeline with Apache Beam / Cloud Dataflow
Reads from Pub/Sub, parses JSON events, validates, writes to BigQuery.

Run locally (DirectRunner):
    python beam_pipeline.py --runner=DirectRunner \
        --project=YOUR_PROJECT --input_topic=projects/PROJECT/topics/raw-events \
        --output_table=PROJECT:dataset.events

Run on Dataflow:
    python beam_pipeline.py --runner=DataflowRunner \
        --project=YOUR_PROJECT --region=us-central1 \
        --temp_location=gs://BUCKET/dataflow-temp \
        --staging_location=gs://BUCKET/dataflow-staging \
        --input_topic=projects/PROJECT/topics/raw-events \
        --output_table=PROJECT:dataset.events
"""
import json
import logging
import argparse
from datetime import datetime, UTC

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition


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
        {"name": "ingested_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
    ]
}

REQUIRED_FIELDS = {"event_id", "event_type", "timestamp"}


class ParseMessage(beam.DoFn):
    """Parse raw Pub/Sub bytes → dict, route bad messages to dead-letter."""

    VALID = "valid"
    DEAD  = "dead_letter"

    def process(self, message: bytes, timestamp=beam.DoFn.TimestampParam):
        try:
            record = json.loads(message.decode("utf-8"))
            missing = REQUIRED_FIELDS - record.keys()
            if missing:
                raise ValueError(f"Missing fields: {missing}")

            record["ingested_at"] = datetime.now(UTC).isoformat()
            yield beam.pvalue.TaggedOutput(self.VALID, record)

        except Exception as exc:
            logging.warning("Dead-letter: %s  |  raw=%s", exc, message[:200])
            yield beam.pvalue.TaggedOutput(self.DEAD, {
                "raw":      message.decode("utf-8", errors="replace")[:1024],
                "error":    str(exc),
                "failed_at": datetime.now(UTC).isoformat(),
            })


def build_pipeline(args) -> None:
    options = PipelineOptions(
        project=args.project,
        region=getattr(args, "region", "us-central1"),
        temp_location=getattr(args, "temp_location", None),
        staging_location=getattr(args, "staging_location", None),
        streaming=True,
        save_main_session=True,
    )
    options.view_as(StandardOptions).streaming = True

    dead_letter_table = args.output_table.replace("events", "events_dead_letter")

    with beam.Pipeline(options=options) as p:
        parsed = (
            p
            | "ReadPubSub"    >> beam.io.ReadFromPubSub(topic=args.input_topic)
            | "ParseMessages" >> beam.ParDo(ParseMessage()).with_outputs(
                ParseMessage.VALID, ParseMessage.DEAD
            )
        )

        # Valid records → BigQuery main table
        (
            parsed[ParseMessage.VALID]
            | "WriteToBQ" >> WriteToBigQuery(
                table=args.output_table,
                schema=BQ_SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

        # Dead-letter records → separate table for debugging
        (
            parsed[ParseMessage.DEAD]
            | "WriteDeadLetter" >> WriteToBigQuery(
                table=dead_letter_table,
                schema={
                    "fields": [
                        {"name": "raw",       "type": "STRING",    "mode": "NULLABLE"},
                        {"name": "error",     "type": "STRING",    "mode": "NULLABLE"},
                        {"name": "failed_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
                    ]
                },
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    parser = argparse.ArgumentParser()
    parser.add_argument("--project",      required=True)
    parser.add_argument("--input_topic",  required=True,
                        help="projects/PROJECT/topics/TOPIC")
    parser.add_argument("--output_table", required=True,
                        help="PROJECT:DATASET.TABLE")
    parser.add_argument("--region",       default="us-central1")
    parser.add_argument("--temp_location")
    parser.add_argument("--staging_location")
    parser.add_argument("--runner",       default="DataflowRunner")
    args, pipeline_args = parser.parse_known_args()

    import sys
    sys.argv = [sys.argv[0]] + pipeline_args
    build_pipeline(args)
