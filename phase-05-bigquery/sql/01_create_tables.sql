-- Phase 05 — LAB 5A: BigQuery table definitions (Bronze → Silver → Gold)

-- ── Bronze: raw events exactly as they arrived ───────────────────────────────
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.bronze_events`
(
  event_id    STRING    NOT NULL,
  event_type  STRING    NOT NULL,
  timestamp   TIMESTAMP NOT NULL,
  user_id     STRING,
  product_id  STRING,
  product     STRING,
  category    STRING,
  quantity    INT64,
  total       FLOAT64,
  ingested_at TIMESTAMP NOT NULL
)
PARTITION BY DATE(timestamp)
CLUSTER BY event_type, category
OPTIONS (
  require_partition_filter = FALSE,
  description = "Raw streaming events — append-only, never updated"
);

-- ── Silver: validated and enriched events ────────────────────────────────────
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.silver_events`
(
  event_id     STRING    NOT NULL,
  event_type   STRING    NOT NULL,
  event_date   DATE      NOT NULL,
  event_ts     TIMESTAMP NOT NULL,
  user_id      STRING,
  product_id   STRING,
  product      STRING,
  category     STRING,
  quantity     INT64,
  unit_price   FLOAT64,
  total        FLOAT64,
  revenue_band STRING,          -- low / medium / high
  processed_at TIMESTAMP NOT NULL
)
PARTITION BY event_date
CLUSTER BY event_type, category, revenue_band
OPTIONS (
  require_partition_filter = FALSE,
  description = "Cleaned and enriched events — one row per unique event_id"
);

-- ── Gold: daily revenue aggregates for dashboards ────────────────────────────
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.gold_daily_revenue`
(
  report_date    DATE    NOT NULL,
  category       STRING  NOT NULL,
  event_type     STRING  NOT NULL,
  order_count    INT64,
  total_quantity INT64,
  total_revenue  FLOAT64,
  avg_order_value FLOAT64,
  updated_at     TIMESTAMP
)
PARTITION BY report_date
CLUSTER BY category
OPTIONS (
  description = "Daily revenue aggregates — source of truth for BI dashboards"
);
