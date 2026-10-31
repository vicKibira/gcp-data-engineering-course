-- Phase 05 — LAB 5B: Analytics queries demonstrating BigQuery features
--
-- This file covers four increasingly sophisticated BQ query patterns:
--   1. Simple aggregation with partition pruning
--   2. Cohort analysis using CTEs and JOIN
--   3. Rolling window calculations with analytic functions
--   4. Gold layer materialisation (scheduled INSERT)
--
-- Replace ${PROJECT} and ${DATASET} before running, or use the
-- deploy_all.sh script which handles substitution automatically.


-- ── 1. Revenue by category (last 30 days) ───────────────────────────────────
-- Demonstrates: partition pruning via event_date range filter,
-- clustering benefit from GROUP BY category.
-- BETWEEN DATE_SUB + CURRENT_DATE is the idiomatic BQ date range pattern.
SELECT
  category,
  COUNT(DISTINCT event_id)   AS orders,         -- distinct count avoids double-counting retries
  SUM(quantity)              AS units_sold,
  ROUND(SUM(total), 2)       AS revenue,         -- round to cents for display
  ROUND(AVG(total), 2)       AS avg_order_value
FROM `${PROJECT}.${DATASET}.silver_events`
WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  AND event_type = 'ORDER_PLACED'               -- exclude shipped/cancelled from revenue metric
GROUP BY category
ORDER BY revenue DESC;


-- ── 2. User cohort retention (window functions) ──────────────────────────────
-- Cohort analysis answers: "Of all users who first ordered in month M,
-- how many returned and ordered in month M+1, M+2, etc.?"
-- A retention curve dropping steeply signals onboarding or product issues.
--
-- Pattern: CTE-chained query
--   first_order  → each user's first purchase date and cohort month
--   orders_with_cohort → all subsequent orders annotated with the cohort
--   Final SELECT → retention count per (cohort, month_number)
WITH first_order AS (
  SELECT
    user_id,
    MIN(event_date)  AS first_order_date,
    -- DATE_TRUNC to month so users who joined at any point in the month belong to the same cohort
    DATE_TRUNC(MIN(event_date), MONTH) AS cohort_month
  FROM `${PROJECT}.${DATASET}.silver_events`
  WHERE event_type = 'ORDER_PLACED'
  GROUP BY user_id
),
orders_with_cohort AS (
  SELECT
    e.user_id,
    e.event_date,
    f.cohort_month,
    -- month_number 0 = acquisition month, 1 = first full month retained, etc.
    DATE_DIFF(DATE_TRUNC(e.event_date, MONTH), f.cohort_month, MONTH) AS month_number
  FROM `${PROJECT}.${DATASET}.silver_events` e
  JOIN first_order f USING (user_id)
  WHERE e.event_type = 'ORDER_PLACED'
)
SELECT
  cohort_month,
  month_number,
  COUNT(DISTINCT user_id)  AS active_users    -- users active in this cohort × month cell
FROM orders_with_cohort
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;


-- ── 3. 7-day rolling revenue (analytic functions) ────────────────────────────
-- Rolling averages smooth out day-of-week seasonality so trends are visible.
-- The OVER clause defines a window: "current row and 6 preceding rows" = 7 days.
-- Because we ORDER BY event_date, "preceding" means earlier dates.
--
-- Pattern:
--   SUM(total) is first aggregated per event_date (GROUP BY),
--   then AVG is applied OVER the grouped result as a window function.
--   This is the "two-level aggregation" technique for time-series smoothing.
SELECT
  event_date,
  ROUND(SUM(total), 2)  AS daily_revenue,
  ROUND(
    AVG(SUM(total)) OVER (
      ORDER BY event_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW   -- 7-day window (today + 6 prior days)
    ), 2
  ) AS rolling_7d_avg
FROM `${PROJECT}.${DATASET}.silver_events`
WHERE event_type = 'ORDER_PLACED'
GROUP BY event_date
ORDER BY event_date;


-- ── 4. Gold layer materialisation (scheduled via Composer) ───────────────────
-- This INSERT is triggered nightly by the Composer DAG (phase-06)
-- to populate gold_daily_revenue for the previous day.
-- WRITE_APPEND disposition means historical rows are never overwritten.
--
-- Why INSERT instead of MERGE?
--   The upstream silver layer is idempotent — if re-run for the same date,
--   the Composer DAG deletes the date's partition before re-inserting.
--   For the initial load pattern INSERT is simpler and cheaper.
INSERT INTO `${PROJECT}.${DATASET}.gold_daily_revenue`
  (report_date, category, event_type, order_count, total_quantity, total_revenue, avg_order_value, updated_at)
SELECT
  event_date               AS report_date,
  category,
  event_type,
  COUNT(DISTINCT event_id) AS order_count,
  SUM(quantity)            AS total_quantity,
  ROUND(SUM(total), 2)     AS total_revenue,
  ROUND(AVG(total), 2)     AS avg_order_value,
  CURRENT_TIMESTAMP()      AS updated_at
FROM `${PROJECT}.${DATASET}.silver_events`
-- Process only yesterday — this is the idiomatic "incremental load" pattern in BQ
WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY report_date, category, event_type;
