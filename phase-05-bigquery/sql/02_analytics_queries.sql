-- Phase 05 — LAB 5B: Analytics queries demonstrating BigQuery features

-- ── 1. Revenue by category (last 30 days) ───────────────────────────────────
SELECT
  category,
  COUNT(DISTINCT event_id)   AS orders,
  SUM(quantity)              AS units_sold,
  ROUND(SUM(total), 2)       AS revenue,
  ROUND(AVG(total), 2)       AS avg_order_value
FROM `${PROJECT}.${DATASET}.silver_events`
WHERE event_date BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY) AND CURRENT_DATE()
  AND event_type = 'ORDER_PLACED'
GROUP BY category
ORDER BY revenue DESC;


-- ── 2. User cohort retention (window functions) ──────────────────────────────
WITH first_order AS (
  SELECT
    user_id,
    MIN(event_date)  AS first_order_date,
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
    DATE_DIFF(DATE_TRUNC(e.event_date, MONTH), f.cohort_month, MONTH) AS month_number
  FROM `${PROJECT}.${DATASET}.silver_events` e
  JOIN first_order f USING (user_id)
  WHERE e.event_type = 'ORDER_PLACED'
)
SELECT
  cohort_month,
  month_number,
  COUNT(DISTINCT user_id)  AS active_users
FROM orders_with_cohort
GROUP BY cohort_month, month_number
ORDER BY cohort_month, month_number;


-- ── 3. 7-day rolling revenue (analytic functions) ────────────────────────────
SELECT
  event_date,
  ROUND(SUM(total), 2)  AS daily_revenue,
  ROUND(
    AVG(SUM(total)) OVER (
      ORDER BY event_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    ), 2
  ) AS rolling_7d_avg
FROM `${PROJECT}.${DATASET}.silver_events`
WHERE event_type = 'ORDER_PLACED'
GROUP BY event_date
ORDER BY event_date;


-- ── 4. Gold layer materialisation (scheduled via Composer) ───────────────────
-- Run this as a scheduled query or in a Composer DAG:
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
WHERE event_date = DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
GROUP BY report_date, category, event_type;
