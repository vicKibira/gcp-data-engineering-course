-- Phase 05 — LAB 5C: BigQuery ML — churn prediction model

-- ── Step 1: Create training features ─────────────────────────────────────────
CREATE OR REPLACE TABLE `${PROJECT}.${DATASET}.ml_features_churn` AS
WITH user_stats AS (
  SELECT
    user_id,
    COUNT(DISTINCT event_id)                     AS total_orders,
    SUM(total)                                   AS lifetime_value,
    AVG(total)                                   AS avg_order_value,
    DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) AS days_since_last_order,
    DATE_DIFF(MAX(event_date), MIN(event_date), DAY) AS customer_lifespan_days,
    COUNTIF(event_type = 'ORDER_CANCELLED') /
      NULLIF(COUNT(DISTINCT event_id), 0)        AS cancellation_rate,
    COUNT(DISTINCT category)                     AS category_diversity
  FROM `${PROJECT}.${DATASET}.silver_events`
  GROUP BY user_id
)
SELECT
  user_id,
  total_orders,
  ROUND(lifetime_value, 2)      AS lifetime_value,
  ROUND(avg_order_value, 2)     AS avg_order_value,
  days_since_last_order,
  customer_lifespan_days,
  ROUND(cancellation_rate, 4)   AS cancellation_rate,
  category_diversity,
  -- Label: churned if no order in last 60 days
  IF(days_since_last_order > 60, 1, 0) AS churned
FROM user_stats
WHERE total_orders >= 2;   -- exclude one-time buyers from training


-- ── Step 2: Train a logistic regression model ─────────────────────────────────
CREATE OR REPLACE MODEL `${PROJECT}.${DATASET}.model_churn_v1`
OPTIONS (
  model_type           = 'LOGISTIC_REG',
  input_label_cols     = ['churned'],
  auto_class_weights   = TRUE,
  max_iterations       = 50,
  l2_reg               = 0.1,
  model_registry       = 'vertex_ai',
  vertex_ai_model_id   = 'bqml-churn-v1'
) AS
SELECT
  total_orders,
  lifetime_value,
  avg_order_value,
  days_since_last_order,
  customer_lifespan_days,
  cancellation_rate,
  category_diversity,
  churned
FROM `${PROJECT}.${DATASET}.ml_features_churn`
WHERE MOD(ABS(FARM_FINGERPRINT(user_id)), 10) < 8;   -- 80% train split


-- ── Step 3: Evaluate the model ────────────────────────────────────────────────
SELECT *
FROM ML.EVALUATE(MODEL `${PROJECT}.${DATASET}.model_churn_v1`,
  (SELECT * FROM `${PROJECT}.${DATASET}.ml_features_churn`
   WHERE MOD(ABS(FARM_FINGERPRINT(user_id)), 10) >= 8)  -- 20% test split
);


-- ── Step 4: Score all active users ───────────────────────────────────────────
CREATE OR REPLACE TABLE `${PROJECT}.${DATASET}.churn_scores` AS
SELECT
  user_id,
  predicted_churned,
  predicted_churned_probs[OFFSET(1)].prob AS churn_probability,
  CURRENT_TIMESTAMP()                      AS scored_at
FROM ML.PREDICT(MODEL `${PROJECT}.${DATASET}.model_churn_v1`,
  (SELECT * FROM `${PROJECT}.${DATASET}.ml_features_churn`)
)
ORDER BY churn_probability DESC;


-- ── Step 5: Inspect feature importance ───────────────────────────────────────
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `${PROJECT}.${DATASET}.model_churn_v1`)
ORDER BY importance_gain DESC;
