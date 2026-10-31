-- Phase 05 — dbt Bronze model: bronze_events
--
-- The Bronze model is a thin view over the raw BigQuery table that is
-- populated by the Dataflow streaming pipeline (phase-04).
--
-- Why a view, not a table?
--   Bronze = raw data exactly as it arrived. A view costs nothing to store
--   and is always in sync with the underlying table — no ETL needed.
--   Any query on this model reads live data from the raw table.
--
-- Why does this model exist at all if it's just SELECT *?
--   1. It puts Bronze into the dbt DAG so lineage is documented automatically.
--   2. Downstream models use {{ ref('bronze_events') }} instead of a raw table
--      name — if the physical table is ever renamed, one change propagates everywhere.
--   3. You can add column-level documentation and tests in schema.yml.
--   4. In a real project you might add SELECT <explicit column list> here to
--      enforce the Bronze schema contract even if the source table gains new columns.
--
-- This model is the entry point for the Silver incremental load.
-- DAG: bronze_events (view) → silver_events (incremental table) → gold_daily_revenue (table)

{{
  config(
    materialized = 'view',   -- zero storage cost; always reflects live raw data
  )
}}

-- Explicit column list documents the Bronze schema for dbt docs.
-- The source table is defined in sources.yml ({{ source('bronze', 'bronze_events') }}).
SELECT
  event_id,       -- unique identifier for each event (STRING, NOT NULL)
  event_type,     -- ORDER_PLACED | ORDER_SHIPPED | ORDER_CANCELLED
  timestamp,      -- event creation time (UTC TIMESTAMP)
  user_id,        -- anonymised user reference
  product_id,     -- product SKU
  product,        -- product display name
  category,       -- product category (electronics, accessories, premium)
  quantity,       -- number of units in this event
  total,          -- line total in USD
  ingested_at     -- when the Dataflow pipeline wrote this row to BQ
FROM {{ source('bronze', 'bronze_events') }}
