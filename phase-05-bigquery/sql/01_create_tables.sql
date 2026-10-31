-- Phase 05 — LAB 5A: BigQuery table definitions (Bronze → Silver → Gold)
--
-- Medallion Architecture in BigQuery:
--   Bronze  — raw events exactly as they arrived from the Pub/Sub pipeline.
--             Append-only; never updated. Partition pruning keeps queries cheap.
--   Silver  — validated, typed, deduplicated, and enriched events.
--             Primary analytic table; feeds BI tools and ML feature engineering.
--   Gold    — pre-aggregated summaries for dashboards.
--             Refreshed nightly via Composer DAG; read-heavy, small cardinality.
--
-- Run order matters: create in Bronze → Silver → Gold order (dependencies flow that way).
-- To apply: replace ${PROJECT} and ${DATASET} with real values, or use the
-- deploy_all.sh script which does the substitution automatically via sed.


-- ── Bronze: raw events exactly as they arrived ───────────────────────────────
-- Partitioned by event timestamp so daily and range queries skip irrelevant partitions.
-- Clustered by event_type + category to speed up the most common filter patterns.
-- require_partition_filter=FALSE allows exploratory queries without a date predicate.
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.bronze_events`
(
  event_id    STRING    NOT NULL,   -- unique event identifier from the publisher
  event_type  STRING    NOT NULL,   -- ORDER_PLACED | ORDER_SHIPPED | ORDER_CANCELLED
  timestamp   TIMESTAMP NOT NULL,   -- event creation time (UTC)
  user_id     STRING,               -- anonymised user reference
  product_id  STRING,               -- product SKU
  product     STRING,               -- product display name
  category    STRING,               -- product category (electronics, accessories, premium)
  quantity    INT64,                -- number of units
  total       FLOAT64,              -- line total in USD
  ingested_at TIMESTAMP NOT NULL    -- when the Dataflow pipeline wrote this row
)
PARTITION BY DATE(timestamp)        -- partition pruning: one partition per day
CLUSTER BY event_type, category     -- cluster keys match most analytic GROUP BY patterns
OPTIONS (
  require_partition_filter = FALSE,
  description = "Raw streaming events — append-only, never updated"
);


-- ── Silver: validated and enriched events ────────────────────────────────────
-- Populated by the Spark ETL job (phase-03) and dbt silver model (phase-05).
-- Partitioned by event_date (DATE) rather than event_ts (TIMESTAMP) because
-- daily dashboard queries always filter on whole dates — DATE partitions are
-- simpler and BigQuery can use them as range predicates in BETWEEN clauses.
-- Clustered on three keys to cover the most common analytic slice-and-dice patterns.
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.silver_events`
(
  event_id     STRING    NOT NULL,   -- primary key; deduplication ensures uniqueness
  event_type   STRING    NOT NULL,
  event_date   DATE      NOT NULL,   -- extracted from timestamp for partition key
  event_ts     TIMESTAMP NOT NULL,   -- original timestamp preserved for time-series analysis
  user_id      STRING,
  product_id   STRING,
  product      STRING,
  category     STRING,
  quantity     INT64,
  unit_price   FLOAT64,              -- derived: total / quantity (for per-unit analytics)
  total        FLOAT64,
  revenue_band STRING,               -- derived: low (<$50) | medium ($50-$199) | high ($200+)
  processed_at TIMESTAMP NOT NULL    -- when the ETL/dbt job wrote this row
)
PARTITION BY event_date
CLUSTER BY event_type, category, revenue_band  -- matches most GROUP BY combos in analytics queries
OPTIONS (
  require_partition_filter = FALSE,
  description = "Cleaned and enriched events — one row per unique event_id"
);


-- ── Gold: daily revenue aggregates for dashboards ────────────────────────────
-- Pre-aggregated at report_date + category + event_type granularity.
-- Refreshed nightly by the Composer DAG (phase-06) using an INSERT with
-- WHERE event_date = yesterday, so historical data is never overwritten.
-- BI tools (Looker, Data Studio) read this table for sub-second dashboard queries.
CREATE TABLE IF NOT EXISTS `${PROJECT}.${DATASET}.gold_daily_revenue`
(
  report_date     DATE    NOT NULL,   -- aggregate date (= event_date from silver)
  category        STRING  NOT NULL,   -- product category
  event_type      STRING  NOT NULL,   -- ORDER_PLACED | ORDER_SHIPPED | ORDER_CANCELLED
  order_count     INT64,              -- distinct events in this category/type/day
  total_quantity  INT64,              -- total units sold
  total_revenue   FLOAT64,            -- sum of all order totals (USD)
  avg_order_value FLOAT64,            -- mean order value for this slice
  updated_at      TIMESTAMP           -- when this row was last refreshed
)
PARTITION BY report_date
CLUSTER BY category  -- dashboards almost always filter by category first
OPTIONS (
  description = "Daily revenue aggregates — source of truth for BI dashboards"
);
