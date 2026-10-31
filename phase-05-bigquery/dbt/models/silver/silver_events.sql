-- Phase 05 — dbt Silver model: silver_events
--
-- Bronze → Silver transformation in dbt.
-- Reads raw events from the bronze_events BigQuery table (exposed via the
-- bronze source defined in sources.yml) and produces a cleaned, typed,
-- deduplicated, and enriched Silver layer.
--
-- Materialisation: incremental with merge strategy
--   - First run: full table build from all bronze rows
--   - Subsequent runs: only processes rows where timestamp > max(event_date)
--     in the existing silver table. This is the "append since last run" pattern.
--   - unique_key='event_id': if the same event_id arrives twice (Pub/Sub
--     at-least-once delivery), MERGE updates the existing row instead of
--     inserting a duplicate. This is our deduplication guarantee.
--
-- Partition & cluster match the DDL in 01_create_tables.sql:
--   PARTITION BY event_date — enables date-range partition pruning
--   CLUSTER BY event_type, category — speeds up the most common GROUP BY

{{
  config(
    materialized         = 'incremental',
    unique_key           = 'event_id',       -- deduplication key for MERGE
    incremental_strategy = 'merge',          -- MERGE statement (BQ native)
    partition_by         = {'field': 'event_date', 'data_type': 'date'},
    cluster_by           = ['event_type', 'category'],
  )
}}

-- ── Source: bronze_events ─────────────────────────────────────────────────────
-- {{ source('bronze', 'bronze_events') }} resolves to the BigQuery table defined
-- in sources.yml. Using source() instead of a raw table name:
--   1. Documents lineage (dbt docs shows the DAG edge)
--   2. Enables source freshness checks (dbt source freshness)
--   3. Lets you change the physical table name in one place
WITH source AS (
  SELECT * FROM {{ source('bronze', 'bronze_events') }}
  {% if is_incremental() %}
    -- Incremental filter: only process rows newer than the latest event already in silver.
    -- is_incremental() is False on the first run (full load) and True thereafter.
    -- DATE(timestamp) > MAX(event_date) ensures we re-process today's partition
    -- in case the daily job runs mid-day; this is safe because MERGE deduplicates.
    WHERE DATE(timestamp) > (SELECT MAX(event_date) FROM {{ this }})
  {% endif %}
),

-- ── Step 1: Clean and type-cast ──────────────────────────────────────────────
-- Enforce explicit types regardless of what the writer sent.
-- SAFE_DIVIDE avoids division-by-zero if quantity is somehow 0.
cleaned AS (
  SELECT
    event_id,
    event_type,
    DATE(timestamp)     AS event_date,               -- partition key: date portion of timestamp
    timestamp           AS event_ts,                 -- full timestamp preserved for time-series
    user_id,
    product_id,
    product,
    category,
    CAST(quantity AS INT64)   AS quantity,            -- ensure integer (not float64 from JSON)
    CAST(total    AS FLOAT64) AS total,               -- ensure float64
    -- unit_price: line-level insight (used in margin analysis, anomaly detection)
    ROUND(SAFE_DIVIDE(CAST(total AS FLOAT64), CAST(quantity AS INT64)), 2) AS unit_price,
    ingested_at                                      -- pass through the Dataflow audit timestamp
  FROM source
  -- Null filters: rows missing any required field cannot be usefully analysed.
  -- These should be rare — the Beam pipeline's validation catches most bad data.
  WHERE event_id   IS NOT NULL
    AND event_type IS NOT NULL
    AND timestamp  IS NOT NULL
),

-- ── Step 2: Enrich ────────────────────────────────────────────────────────────
-- Add derived columns for BI and ML downstream consumers.
enriched AS (
  SELECT
    *,
    -- revenue_band: low-cardinality segment column for dashboard drilldowns
    -- Thresholds: <$50 = low, $50–$199 = medium, $200+ = high
    CASE
      WHEN total < 50  THEN 'low'
      WHEN total < 200 THEN 'medium'
      ELSE 'high'
    END AS revenue_band,
    CURRENT_TIMESTAMP() AS processed_at   -- audit: when dbt wrote this row to silver
  FROM cleaned
)

-- ── Final SELECT: what goes into silver_events ───────────────────────────────
-- dbt compiles this into:
--   INSERT INTO silver_events ... SELECT * FROM enriched   (first run / full refresh)
--   MERGE INTO silver_events USING (SELECT...) ON event_id = event_id ...  (incremental)
SELECT * FROM enriched
