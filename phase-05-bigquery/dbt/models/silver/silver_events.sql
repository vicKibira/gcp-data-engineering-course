{{
  config(
    materialized       = 'incremental',
    unique_key         = 'event_id',
    incremental_strategy = 'merge',
    partition_by       = {'field': 'event_date', 'data_type': 'date'},
    cluster_by         = ['event_type', 'category'],
  )
}}

WITH source AS (
  SELECT * FROM {{ source('bronze', 'bronze_events') }}
  {% if is_incremental() %}
    WHERE DATE(timestamp) > (SELECT MAX(event_date) FROM {{ this }})
  {% endif %}
),

cleaned AS (
  SELECT
    event_id,
    event_type,
    DATE(timestamp)     AS event_date,
    timestamp           AS event_ts,
    user_id,
    product_id,
    product,
    category,
    CAST(quantity AS INT64)   AS quantity,
    CAST(total    AS FLOAT64) AS total,
    ROUND(SAFE_DIVIDE(CAST(total AS FLOAT64), CAST(quantity AS INT64)), 2) AS unit_price,
    ingested_at
  FROM source
  WHERE event_id IS NOT NULL
    AND event_type IS NOT NULL
    AND timestamp IS NOT NULL
),

enriched AS (
  SELECT
    *,
    CASE
      WHEN total < 50  THEN 'low'
      WHEN total < 200 THEN 'medium'
      ELSE 'high'
    END AS revenue_band,
    CURRENT_TIMESTAMP() AS processed_at
  FROM cleaned
)

SELECT * FROM enriched
