-- Gold: Hyper-Personalization Signals per merchant
-- Aggregates contextual signals: preferred channel, best contact time,
-- product affinity, engagement patterns, propensity indicators
-- Feeds the Personalization Agent and app recommendations

CREATE OR REPLACE MATERIALIZED VIEW gold_personalization_signals
REFRESH EVERY 1 HOUR
AS
WITH engagement AS (
  SELECT * FROM gold_engagement_metrics
),
segments AS (
  SELECT * FROM gold_segments
),
health AS (
  SELECT * FROM gold_health_score
),
nba AS (
  SELECT * FROM gold_next_best_actions
),
action_history AS (
  SELECT
    golden_id,
    COUNT(*) AS total_actions_received,
    COUNT(DISTINCT action_type) AS distinct_action_types,
    MAX(executed_at) AS last_action_date,
    FIRST_VALUE(channel) OVER (PARTITION BY golden_id ORDER BY executed_at DESC) AS last_channel_used
  FROM main.cdp.nba_action_log
  GROUP BY golden_id
),
txn_patterns AS (
  SELECT
    customer_external_id AS source_id,
    ROUND(AVG(amount), 2) AS avg_txn_amount,
    ROUND(STDDEV(amount), 2) AS txn_amount_stddev,
    COUNT(DISTINCT DATE_TRUNC('week', transaction_date)) AS active_weeks,
    CASE
      WHEN COUNT(DISTINCT DATE_TRUNC('week', transaction_date)) >= 12 THEN 'weekly'
      WHEN COUNT(DISTINCT DATE_TRUNC('month', transaction_date)) >= 6 THEN 'monthly'
      WHEN COUNT(*) >= 3 THEN 'occasional'
      ELSE 'rare'
    END AS purchase_frequency_pattern,
    MODE(DAYOFWEEK(transaction_date)) AS preferred_day_of_week,
    MODE(HOUR(transaction_date)) AS preferred_hour,
    CASE
      WHEN MODE(HOUR(transaction_date)) BETWEEN 6 AND 11 THEN 'morning'
      WHEN MODE(HOUR(transaction_date)) BETWEEN 12 AND 17 THEN 'afternoon'
      WHEN MODE(HOUR(transaction_date)) BETWEEN 18 AND 21 THEN 'evening'
      ELSE 'night'
    END AS preferred_time_slot
  FROM silver_transactions
  GROUP BY customer_external_id
),
identity_lookup AS (
  SELECT source_id, golden_id FROM gold_identity_graph
)
SELECT
  COALESCE(i.golden_id, tp.source_id) AS golden_id,
  COALESCE(s.segment, 'unassigned') AS rfm_segment,
  COALESCE(h.health_tier, 'unknown') AS health_tier,
  COALESCE(h.health_score, 0) AS health_score,

  -- Engagement velocity
  COALESCE(e.txn_count, 0) AS txn_count,
  COALESCE(e.txn_volume, 0) AS txn_volume,
  tp.avg_txn_amount,
  tp.txn_amount_stddev,

  -- Temporal preferences
  tp.purchase_frequency_pattern,
  tp.preferred_day_of_week,
  tp.preferred_hour,
  tp.preferred_time_slot,

  -- Channel affinity (from NBA + action history)
  COALESCE(nba.primary_channel, 'sfmc_email') AS recommended_channel,
  ah.last_channel_used,
  CASE
    WHEN ah.total_actions_received > 3 THEN 'high_engagement'
    WHEN ah.total_actions_received > 0 THEN 'some_engagement'
    ELSE 'no_prior_contact'
  END AS engagement_history_level,

  -- Propensity indicators (rule-based, enhanced by ML in propensity notebook)
  CASE
    WHEN s.segment = 'champions' AND e.txn_volume > 100000 THEN 0.9
    WHEN s.segment IN ('loyal', 'potential_loyalists') AND e.txn_count > 20 THEN 0.7
    WHEN s.segment = 'promising' THEN 0.5
    WHEN s.segment IN ('at_risk', 'cant_lose') THEN 0.3
    ELSE 0.2
  END AS upsell_propensity,

  CASE
    WHEN s.segment IN ('at_risk', 'cant_lose', 'hibernating') THEN 0.8
    WHEN h.health_tier IN ('critical', 'poor') THEN 0.7
    WHEN e.days_since_last_txn > 60 THEN 0.6
    WHEN s.segment = 'need_attention' THEN 0.4
    ELSE 0.1
  END AS churn_propensity,

  CASE
    WHEN s.segment = 'new_customers' AND e.txn_count < 5 THEN 0.9
    WHEN s.segment = 'promising' THEN 0.7
    WHEN e.txn_count < 10 THEN 0.5
    ELSE 0.2
  END AS activation_propensity,

  -- Content personalization hints
  CASE
    WHEN s.segment IN ('champions', 'loyal') THEN 'loyalty_reward'
    WHEN s.segment IN ('at_risk', 'cant_lose') THEN 'win_back'
    WHEN s.segment = 'new_customers' THEN 'onboarding'
    WHEN s.segment = 'promising' THEN 'activation'
    WHEN s.segment = 'potential_loyalists' THEN 'growth'
    ELSE 'general'
  END AS content_theme,

  CASE
    WHEN e.txn_volume > 100000 THEN 'premium'
    WHEN e.txn_volume > 25000 THEN 'growth'
    WHEN e.txn_volume > 5000 THEN 'standard'
    ELSE 'starter'
  END AS merchant_tier,

  nba.primary_action AS next_best_action,
  nba.urgency AS nba_urgency,
  COALESCE(nba.estimated_revenue_impact, 0) AS estimated_revenue_impact,

  CURRENT_TIMESTAMP() AS _refreshed_at
FROM txn_patterns tp
LEFT JOIN identity_lookup i ON tp.source_id = i.source_id
LEFT JOIN engagement e ON COALESCE(i.golden_id, tp.source_id) = e.golden_id
LEFT JOIN segments s ON COALESCE(i.golden_id, tp.source_id) = s.golden_id
LEFT JOIN health h ON COALESCE(i.golden_id, tp.source_id) = h.golden_id
LEFT JOIN nba ON COALESCE(i.golden_id, tp.source_id) = nba.golden_id
LEFT JOIN action_history ah ON COALESCE(i.golden_id, tp.source_id) = ah.golden_id;
