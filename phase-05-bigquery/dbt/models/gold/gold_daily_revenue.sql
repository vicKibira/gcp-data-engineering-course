-- Phase 05 — dbt Gold model: gold_daily_revenue
--
-- Silver → Gold aggregation in dbt.
-- Reads from the silver_events model (via {{ ref() }}) and aggregates to
-- daily granularity, partitioned by date and clustered by category.
--
-- Materialisation: table (full refresh)
--   Gold models are small (one row per day × category × event_type) so a
--   full rebuild on every dbt run is fast and keeps the logic simple.
--   We don't use incremental here because:
--     1. Aggregations can change retroactively if silver is backfilled.
--     2. The full rebuild takes seconds, not minutes, at this cardinality.
--
-- Downstream consumers:
--   - BI dashboards (Looker, Data Studio) read this table directly
--   - The Composer DAG (phase-06) calls `dbt run --select gold` nightly
--   - The Capstone deploy script calls this via the DAG

{{
  config(
    materialized = 'table',       -- rebuild from scratch on every dbt run
    partition_by = {'field': 'report_date', 'data_type': 'date'},
    cluster_by   = ['category'],  -- dashboards almost always filter by category
  )
}}

-- {{ ref('silver_events') }} resolves to the silver_events model output table.
-- Using ref() instead of a raw table name:
--   1. dbt builds the DAG: it knows gold depends on silver → runs silver first
--   2. In CI/CD dbt can target a staging schema (silver_dev) without changing SQL
--   3. dbt docs generates the lineage graph automatically
SELECT
  event_date                         AS report_date,     -- one row per day…
  category,                                              -- … per product category…
  event_type,                                            -- … per event type
  COUNT(DISTINCT event_id)           AS order_count,     -- unique events (not line items)
  SUM(quantity)                      AS total_quantity,  -- units sold
  ROUND(SUM(total), 2)               AS total_revenue,   -- gross revenue in USD
  ROUND(AVG(total), 2)               AS avg_order_value, -- mean order size

  -- Revenue per unique buyer: measures revenue concentration.
  -- NULLIF prevents division-by-zero if a category had quantity but no distinct buyers
  -- (shouldn't happen, but defensive is correct).
  ROUND(SUM(total) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS revenue_per_user,

  COUNT(DISTINCT user_id)            AS unique_buyers,   -- buyer count for this slice
  CURRENT_TIMESTAMP()                AS updated_at       -- when this aggregation was built
FROM {{ ref('silver_events') }}
WHERE event_type = 'ORDER_PLACED'   -- only revenue-generating events (exclude shipped/cancelled)
GROUP BY report_date, category, event_type
