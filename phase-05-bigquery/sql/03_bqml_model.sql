-- Phase 05 — LAB 5C: BigQuery ML — churn prediction model
--
-- BigQuery ML (BQML) trains and scores models using pure SQL.
-- No separate ML infrastructure needed — training happens inside BQ's compute.
-- The trained model is registered to Vertex AI Model Registry for serving.
--
-- CRISP-DM mapping:
--   Step 1 = Data Preparation (feature engineering)
--   Step 2 = Modeling       (train logistic regression)
--   Step 3 = Evaluation     (ROC, accuracy, precision/recall)
--   Step 4 = Deployment     (batch scoring → churn_scores table)
--   Step 5 = Evaluation     (feature importance for explainability)
--
-- Replace ${PROJECT} and ${DATASET} before running.


-- ── Step 1: Create training features ─────────────────────────────────────────
-- Aggregates per-user behavioural signals from the silver events table.
-- The feature set is designed for churn prediction:
--   - Recency:   days_since_last_order (higher = more likely churned)
--   - Frequency: total_orders (lower = less engaged)
--   - Monetary:  lifetime_value, avg_order_value
--   - Diversity: category_diversity (users who browse widely churn less)
--   - Behaviour: cancellation_rate (frustrated users churn faster)
--
-- Label definition: churned = 1 if no order in last 60 days.
-- 60 days is a common e-commerce threshold; tune based on your purchase cycle.
CREATE OR REPLACE TABLE `${PROJECT}.${DATASET}.ml_features_churn` AS
WITH user_stats AS (
  SELECT
    user_id,
    COUNT(DISTINCT event_id)                     AS total_orders,
    SUM(total)                                   AS lifetime_value,
    AVG(total)                                   AS avg_order_value,
    -- Recency: days between last order and today (larger → at-risk)
    DATE_DIFF(CURRENT_DATE(), MAX(event_date), DAY) AS days_since_last_order,
    -- Lifespan: how long has this user been a customer?
    DATE_DIFF(MAX(event_date), MIN(event_date), DAY) AS customer_lifespan_days,
    -- Cancellation rate: fraction of orders that were cancelled
    -- NULLIF(total_orders, 0) prevents division-by-zero for brand-new users
    COUNTIF(event_type = 'ORDER_CANCELLED') /
      NULLIF(COUNT(DISTINCT event_id), 0)        AS cancellation_rate,
    -- Category diversity: users buying across many categories are more engaged
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
  -- Binary label: 1 = churned (no order in 60 days), 0 = active
  IF(days_since_last_order > 60, 1, 0) AS churned
FROM user_stats
WHERE total_orders >= 2;   -- exclude one-time buyers; they have no "return" behaviour to model


-- ── Step 2: Train a logistic regression model ─────────────────────────────────
-- CREATE OR REPLACE MODEL replaces any previously trained version in-place.
-- The model artifact is stored in BQ and also registered to Vertex AI.
--
-- Key option choices:
--   model_type='LOGISTIC_REG'  — good baseline; interpretable; fast to train
--   auto_class_weights=TRUE    — compensates for class imbalance (churned ~20%);
--                                equivalent to sklearn's class_weight='balanced'
--   l2_reg=0.1                 — L2 regularisation prevents overfitting on small datasets
--   model_registry='vertex_ai' — makes the model deployable as a Vertex endpoint
--
-- Train/test split:
--   FARM_FINGERPRINT(user_id) produces a deterministic integer hash of the user_id.
--   MOD(..., 10) < 8 assigns ~80% of users to training deterministically:
--   the same user always goes to the same split across re-runs (no data leakage).
CREATE OR REPLACE MODEL `${PROJECT}.${DATASET}.model_churn_v1`
OPTIONS (
  model_type           = 'LOGISTIC_REG',
  input_label_cols     = ['churned'],
  auto_class_weights   = TRUE,          -- balance churned (minority) vs active (majority)
  max_iterations       = 50,            -- convergence usually reached well before 50 epochs
  l2_reg               = 0.1,           -- regularisation to reduce overfitting
  model_registry       = 'vertex_ai',   -- registers model to Vertex AI after training
  vertex_ai_model_id   = 'bqml-churn-v1'  -- model display name in Vertex AI Model Registry
) AS
SELECT
  total_orders,
  lifetime_value,
  avg_order_value,
  days_since_last_order,
  customer_lifespan_days,
  cancellation_rate,
  category_diversity,
  churned                                -- label column (named in input_label_cols above)
FROM `${PROJECT}.${DATASET}.ml_features_churn`
WHERE MOD(ABS(FARM_FINGERPRINT(user_id)), 10) < 8;   -- deterministic 80% train split


-- ── Step 3: Evaluate the model ────────────────────────────────────────────────
-- ML.EVALUATE scores the model on held-out data (the remaining 20%).
-- Returns: accuracy, precision, recall, F1, log_loss, ROC_AUC.
-- For churn: prioritise recall (catching true churners) over precision,
-- since the cost of missing a churner > cost of a false alarm.
-- Target: AUC-ROC > 0.75 is typically acceptable for a churn model.
SELECT *
FROM ML.EVALUATE(MODEL `${PROJECT}.${DATASET}.model_churn_v1`,
  (SELECT * FROM `${PROJECT}.${DATASET}.ml_features_churn`
   -- test split: the 20% of users not used in training
   WHERE MOD(ABS(FARM_FINGERPRINT(user_id)), 10) >= 8)
);


-- ── Step 4: Score all active users ───────────────────────────────────────────
-- ML.PREDICT runs inference on every row in the features table.
-- predicted_churned        = binary label (0 or 1) at default threshold 0.5
-- predicted_churned_probs  = array of [{label, prob}]; OFFSET(1) = the churn class
-- Output table is sorted by churn_probability DESC so CRM teams can prioritise
-- outreach: contact the highest-risk users first.
CREATE OR REPLACE TABLE `${PROJECT}.${DATASET}.churn_scores` AS
SELECT
  user_id,
  predicted_churned,
  -- Extract probability of the positive class (churned=1)
  predicted_churned_probs[OFFSET(1)].prob AS churn_probability,
  CURRENT_TIMESTAMP()                      AS scored_at
FROM ML.PREDICT(MODEL `${PROJECT}.${DATASET}.model_churn_v1`,
  (SELECT * FROM `${PROJECT}.${DATASET}.ml_features_churn`)
)
ORDER BY churn_probability DESC;  -- highest-risk users first for CRM prioritisation


-- ── Step 5: Inspect feature importance ───────────────────────────────────────
-- ML.FEATURE_IMPORTANCE reveals which features the model relies on most.
-- importance_gain: how much a feature reduces the loss when it's used for splitting.
-- Use this to: (a) drop low-importance features to simplify the model,
--              (b) prioritise data quality efforts on the most important features,
--              (c) explain predictions to business stakeholders.
SELECT * FROM ML.FEATURE_IMPORTANCE(MODEL `${PROJECT}.${DATASET}.model_churn_v1`)
ORDER BY importance_gain DESC;
