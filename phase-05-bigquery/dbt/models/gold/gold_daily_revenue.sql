{{
  config(
    materialized = 'table',
    partition_by = {'field': 'report_date', 'data_type': 'date'},
    cluster_by   = ['category'],
  )
}}

SELECT
  event_date                         AS report_date,
  category,
  event_type,
  COUNT(DISTINCT event_id)           AS order_count,
  SUM(quantity)                      AS total_quantity,
  ROUND(SUM(total), 2)               AS total_revenue,
  ROUND(AVG(total), 2)               AS avg_order_value,
  ROUND(SUM(total) / NULLIF(COUNT(DISTINCT user_id), 0), 2) AS revenue_per_user,
  COUNT(DISTINCT user_id)            AS unique_buyers,
  CURRENT_TIMESTAMP()                AS updated_at
FROM {{ ref('silver_events') }}
WHERE event_type = 'ORDER_PLACED'
GROUP BY report_date, category, event_type
