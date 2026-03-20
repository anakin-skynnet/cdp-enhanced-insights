-- PagoNxt Getnet CDP - Unity Catalog Functions for AI Agents
-- These functions are registered as tools the agents call to query
-- the golden record, engagement metrics, and segments.
-- Run this notebook once to create the functions.
-- NOTE: LIMIT in UC SQL functions must be a constant (not a parameter).

-- ═══════════════════════════════════════════════════════════════
-- ENSURE PLACEHOLDER TABLES EXIST (for ML-output tables)
-- UC SQL functions validate referenced tables at creation time.
-- These CREATE TABLE IF NOT EXISTS statements ensure the functions
-- can always be created, even before the ML jobs run.
-- ═══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_customer_ltv (
  golden_id STRING, clv_12m DOUBLE, clv_tier STRING, p_alive DOUBLE,
  predicted_purchases_12m DOUBLE, total_amount DOUBLE, frequency DOUBLE, recency DOUBLE,
  monetary_value DOUBLE, T DOUBLE, expected_profit DOUBLE,
  _model_version STRING, _scored_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_behavioral_segments (
  golden_id STRING, cluster_id INT, behavioral_segment STRING,
  txn_count DOUBLE, txn_volume DOUBLE, days_since_last_txn DOUBLE,
  ticket_count DOUBLE, health_score DOUBLE, recency_score DOUBLE,
  frequency_score DOUBLE, monetary_score DOUBLE, support_penalty DOUBLE,
  tenure_bonus DOUBLE, tenure_days DOUBLE, r_score DOUBLE,
  f_score DOUBLE, m_score DOUBLE,
  _model_version STRING, _scored_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_channel_attribution (
  channel STRING, markov_attributed_value DOUBLE, markov_attribution_share DOUBLE,
  first_touch_value DOUBLE, last_touch_value DOUBLE,
  linear_value DOUBLE, time_decay_value DOUBLE,
  markov_conversions DOUBLE, _model_version STRING, _scored_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_ad_creative_library (
  segment STRING, merchant_count BIGINT, avg_volume DOUBLE,
  avg_health DOUBLE, avg_recency_days DOUBLE,
  email_subject STRING, email_body_preview STRING,
  sms_message STRING, push_notification STRING,
  ad_headline STRING, ad_description STRING,
  tone STRING, cta STRING, generated_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_call_center_sentiment (
  conversation_id STRING, agent_id STRING, customer_external_id STRING,
  conversation_date DATE, media_type STRING, duration_seconds BIGINT,
  sentiment STRING, topic_category STRING, call_summary STRING,
  _enriched_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_churn_scores (
  golden_id STRING, churn_probability DOUBLE, churn_risk_tier STRING,
  _model_version STRING, _scored_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.gold_propensity_scores (
  golden_id STRING, propensity_type STRING, propensity_score DOUBLE,
  propensity_tier STRING, _model_version STRING, _scored_at TIMESTAMP
);

-- ═══════════════════════════════════════════════════════════════
-- CORE FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 1. Look up a merchant by golden_id, email, or name fragment
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.lookup_merchant(
  search_term STRING COMMENT 'Golden ID, email address, or merchant name fragment to search for'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  email STRING,
  phone STRING,
  industry STRING,
  country STRING,
  segment STRING,
  txn_volume DOUBLE,
  txn_count BIGINT,
  ticket_count BIGINT,
  days_since_last_txn INT,
  r_score INT,
  f_score INT,
  m_score INT
)
LANGUAGE SQL
COMMENT 'Look up a merchant by golden ID, email, or name fragment. Returns C360 profile with engagement metrics and segment.'
RETURN
  SELECT
    c.golden_id,
    COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
    c.email,
    c.phone,
    COALESCE(c.industry, '') AS industry,
    COALESCE(c.country, '') AS country,
    COALESCE(s.segment, 'unassigned') AS segment,
    COALESCE(e.txn_volume, 0) AS txn_volume,
    COALESCE(e.txn_count, 0) AS txn_count,
    COALESCE(e.ticket_count, 0) AS ticket_count,
    COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
    COALESCE(s.r_score, 0) AS r_score,
    COALESCE(s.f_score, 0) AS f_score,
    COALESCE(s.m_score, 0) AS m_score
  FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id
  WHERE c.golden_id = search_term
     OR LOWER(c.email) = LOWER(search_term)
     OR LOWER(COALESCE(c.merchant_name, c.first_name, '')) LIKE CONCAT('%', LOWER(search_term), '%')
  LIMIT 20;

-- 2. Get at-risk merchants (churn candidates)
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_at_risk_merchants(
  risk_level STRING DEFAULT 'all' COMMENT 'Filter: "high" (>90d inactive), "medium" (60-90d), "all" at-risk segments'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  email STRING,
  segment STRING,
  txn_volume DOUBLE,
  txn_count BIGINT,
  days_since_last_txn INT,
  ticket_count BIGINT
)
LANGUAGE SQL
COMMENT 'Get merchants at risk of churn. Filter by risk level: high (>90d inactive), medium (60-90d), or all at-risk segments. Returns top 50 by volume.'
RETURN
  SELECT
    c.golden_id,
    COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
    c.email,
    COALESCE(s.segment, 'unassigned') AS segment,
    COALESCE(e.txn_volume, 0) AS txn_volume,
    COALESCE(e.txn_count, 0) AS txn_count,
    COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
    COALESCE(e.ticket_count, 0) AS ticket_count
  FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
  JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id
  WHERE (
    (risk_level = 'high' AND e.days_since_last_txn > 90)
    OR (risk_level = 'medium' AND e.days_since_last_txn BETWEEN 60 AND 90)
    OR (risk_level = 'all' AND s.segment IN ('at_risk', 'cant_lose', 'hibernating'))
  )
  ORDER BY e.txn_volume DESC
  LIMIT 50;

-- 3. Get segment summary with performance metrics
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_segment_summary(
  segment_filter STRING DEFAULT 'all' COMMENT 'Segment name to filter, or "all" for all segments'
)
RETURNS TABLE(
  segment STRING,
  merchant_count BIGINT,
  total_volume DOUBLE,
  avg_volume DOUBLE,
  avg_txn_count DOUBLE,
  avg_recency DOUBLE,
  avg_tickets DOUBLE
)
LANGUAGE SQL
COMMENT 'Get segment-level summary with merchant counts, volume, and engagement metrics. Filter by segment name or use "all".'
RETURN
  SELECT
    COALESCE(s.segment, 'unassigned') AS segment,
    COUNT(DISTINCT c.golden_id) AS merchant_count,
    COALESCE(SUM(e.txn_volume), 0) AS total_volume,
    COALESCE(ROUND(AVG(e.txn_volume), 2), 0) AS avg_volume,
    COALESCE(ROUND(AVG(e.txn_count), 1), 0) AS avg_txn_count,
    COALESCE(ROUND(AVG(e.days_since_last_txn), 0), 0) AS avg_recency,
    COALESCE(ROUND(AVG(e.ticket_count), 1), 0) AS avg_tickets
  FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id
  WHERE segment_filter = 'all' OR COALESCE(s.segment, 'unassigned') = segment_filter
  GROUP BY COALESCE(s.segment, 'unassigned')
  ORDER BY total_volume DESC;

-- 4. Get merchants in a specific segment for campaign targeting
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_segment_merchants(
  target_segment STRING COMMENT 'Segment name: champions, loyal, potential_loyalists, new_customers, promising, at_risk, cant_lose, hibernating, need_attention'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  email STRING,
  txn_volume DOUBLE,
  txn_count BIGINT,
  days_since_last_txn INT,
  r_score INT,
  f_score INT,
  m_score INT
)
LANGUAGE SQL
COMMENT 'Get top 50 merchants in a specific RFM segment for campaign targeting. Returns contact info and engagement metrics.'
RETURN
  SELECT
    c.golden_id,
    COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
    c.email,
    COALESCE(e.txn_volume, 0) AS txn_volume,
    COALESCE(e.txn_count, 0) AS txn_count,
    COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
    COALESCE(s.r_score, 0) AS r_score,
    COALESCE(s.f_score, 0) AS f_score,
    COALESCE(s.m_score, 0) AS m_score
  FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
  JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
  WHERE s.segment = target_segment
  ORDER BY e.txn_volume DESC NULLS LAST
  LIMIT 50;

-- 5. Get next best actions for merchants (priority-ranked)
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_next_best_actions(
  urgency_filter STRING DEFAULT 'all' COMMENT 'Filter: "immediate", "this_week", "this_month", or "all"',
  segment_filter STRING DEFAULT 'all' COMMENT 'Filter by segment name, or "all"'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  email STRING,
  segment STRING,
  health_score INT,
  health_tier STRING,
  primary_action STRING,
  secondary_action STRING,
  primary_channel STRING,
  urgency STRING,
  priority_score DOUBLE,
  estimated_revenue_impact DOUBLE,
  txn_volume DOUBLE,
  days_since_last_txn INT,
  ticket_count BIGINT
)
LANGUAGE SQL
COMMENT 'Get top 50 priority-ranked next best actions for merchants. Filter by urgency (immediate/this_week/this_month) and/or segment.'
RETURN
  SELECT
    golden_id,
    merchant_name,
    email,
    segment,
    health_score,
    health_tier,
    primary_action,
    secondary_action,
    primary_channel,
    urgency,
    priority_score,
    estimated_revenue_impact,
    txn_volume,
    days_since_last_txn,
    ticket_count
  FROM ahs_demos_catalog.cdp_360.gold_next_best_actions
  WHERE (urgency_filter = 'all' OR urgency = urgency_filter)
    AND (segment_filter = 'all' OR segment = segment_filter)
  ORDER BY priority_score DESC
  LIMIT 50;

-- 6. Get NBA summary by action type
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_nba_summary()
RETURNS TABLE(
  primary_action STRING,
  merchant_count BIGINT,
  total_revenue_impact DOUBLE,
  avg_health_score DOUBLE,
  immediate_count BIGINT,
  this_week_count BIGINT,
  top_channel STRING
)
LANGUAGE SQL
COMMENT 'Get summary of next best actions: counts, revenue impact, and urgency breakdown per action type.'
RETURN
  SELECT
    primary_action,
    COUNT(*) AS merchant_count,
    ROUND(SUM(estimated_revenue_impact), 2) AS total_revenue_impact,
    ROUND(AVG(health_score), 1) AS avg_health_score,
    COUNT(CASE WHEN urgency = 'immediate' THEN 1 END) AS immediate_count,
    COUNT(CASE WHEN urgency = 'this_week' THEN 1 END) AS this_week_count,
    FIRST(primary_channel) AS top_channel
  FROM ahs_demos_catalog.cdp_360.gold_next_best_actions
  GROUP BY primary_action
  ORDER BY total_revenue_impact DESC;

-- 7. Get merchant health score details
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_health_scores(
  health_filter STRING DEFAULT 'all' COMMENT 'Filter: "critical", "poor", "fair", "good", "excellent", or "all"'
)
RETURNS TABLE(
  golden_id STRING,
  segment STRING,
  health_score INT,
  health_tier STRING,
  recency_score INT,
  frequency_score INT,
  monetary_score INT,
  support_penalty INT,
  tenure_bonus INT,
  txn_volume DOUBLE,
  days_since_last_txn INT,
  ticket_count BIGINT
)
LANGUAGE SQL
COMMENT 'Get merchant health scores with component breakdown. Filter by health tier (critical/poor/fair/good/excellent). Returns top 50.'
RETURN
  SELECT
    golden_id,
    segment,
    health_score,
    health_tier,
    recency_score,
    frequency_score,
    monetary_score,
    support_penalty,
    tenure_bonus,
    txn_volume,
    days_since_last_txn,
    ticket_count
  FROM ahs_demos_catalog.cdp_360.gold_health_score
  WHERE health_filter = 'all' OR health_tier = health_filter
  ORDER BY health_score ASC
  LIMIT 50;

-- 8. Action tracking table for attribution and closed-loop measurement
CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.nba_action_log (
  action_id STRING,
  golden_id STRING NOT NULL,
  action_type STRING NOT NULL,
  channel STRING,
  executed_by STRING,
  notes STRING,
  executed_at TIMESTAMP
);

-- Agent feedback tracking for evaluation loop
CREATE TABLE IF NOT EXISTS ahs_demos_catalog.cdp_360.agent_feedback_log (
  feedback_id STRING,
  message_content STRING,
  rating INT,
  comment STRING,
  created_at TIMESTAMP
);

-- 9. Get action history for a merchant
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_action_history(
  merchant_golden_id STRING COMMENT 'Golden ID of the merchant'
)
RETURNS TABLE(
  action_id STRING,
  golden_id STRING,
  action_type STRING,
  channel STRING,
  executed_by STRING,
  notes STRING,
  executed_at TIMESTAMP
)
LANGUAGE SQL
COMMENT 'Get recent action history for a merchant from the NBA action log. Useful to avoid repeating actions. Returns last 20.'
RETURN
  SELECT action_id, golden_id, action_type, channel, executed_by, notes, executed_at
  FROM ahs_demos_catalog.cdp_360.nba_action_log
  WHERE golden_id = merchant_golden_id
  ORDER BY executed_at DESC
  LIMIT 20;

-- 10. Get action log summary (for attribution reporting)
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_action_log_summary()
RETURNS TABLE(
  action_type STRING,
  total_actions BIGINT,
  unique_merchants BIGINT,
  most_recent TIMESTAMP,
  top_channel STRING
)
LANGUAGE SQL
COMMENT 'Summary of all actions taken, grouped by type. For attribution reporting and action effectiveness analysis.'
RETURN
  SELECT
    action_type,
    COUNT(*) AS total_actions,
    COUNT(DISTINCT golden_id) AS unique_merchants,
    MAX(executed_at) AS most_recent,
    FIRST(channel) AS top_channel
  FROM ahs_demos_catalog.cdp_360.nba_action_log
  GROUP BY action_type
  ORDER BY total_actions DESC;

-- 11. Get churn risk KPIs
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_churn_kpis()
RETURNS TABLE(
  total_merchants BIGINT,
  at_risk_count BIGINT,
  hibernating_count BIGINT,
  churned_90d BIGINT,
  churn_rate_pct DOUBLE,
  volume_at_risk DOUBLE
)
LANGUAGE SQL
COMMENT 'Get aggregate churn risk KPIs: total merchants, at-risk counts, churn rate, and revenue at risk.'
RETURN
  SELECT
    COUNT(DISTINCT c.golden_id) AS total_merchants,
    COUNT(DISTINCT CASE WHEN s.segment IN ('at_risk', 'cant_lose') THEN c.golden_id END) AS at_risk_count,
    COUNT(DISTINCT CASE WHEN s.segment = 'hibernating' THEN c.golden_id END) AS hibernating_count,
    COUNT(DISTINCT CASE WHEN e.days_since_last_txn > 90 THEN c.golden_id END) AS churned_90d,
    ROUND(COUNT(DISTINCT CASE WHEN e.days_since_last_txn > 90 THEN c.golden_id END) * 100.0 /
      NULLIF(COUNT(DISTINCT c.golden_id), 0), 1) AS churn_rate_pct,
    COALESCE(SUM(CASE WHEN s.segment IN ('at_risk', 'cant_lose', 'hibernating') THEN e.txn_volume ELSE 0 END), 0) AS volume_at_risk
  FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id;

-- ═══════════════════════════════════════════════════════════════
-- SOLUTION ACCELERATOR FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 12. Get Customer Lifetime Value rankings
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_clv_rankings(
  clv_tier_filter STRING DEFAULT 'all' COMMENT 'Filter: "very_high", "high", "medium", "low", "very_low", or "all"'
)
RETURNS TABLE(
  golden_id STRING,
  clv_12m DOUBLE,
  clv_tier STRING,
  p_alive DOUBLE,
  predicted_purchases_12m DOUBLE,
  total_amount DOUBLE,
  frequency INT,
  recency INT
)
LANGUAGE SQL
COMMENT 'Get top 50 merchants ranked by predicted 12-month Customer Lifetime Value. Based on BG/NBD + Gamma-Gamma models.'
RETURN
  SELECT golden_id, clv_12m, clv_tier, p_alive,
         predicted_purchases_12m, total_amount,
         CAST(frequency AS INT), CAST(recency AS INT)
  FROM ahs_demos_catalog.cdp_360.gold_customer_ltv
  WHERE clv_tier_filter = 'all' OR clv_tier = clv_tier_filter
  ORDER BY clv_12m DESC
  LIMIT 50;

-- 13. Get CLV summary by tier
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_clv_summary()
RETURNS TABLE(
  clv_tier STRING,
  merchant_count BIGINT,
  avg_clv DOUBLE,
  total_clv DOUBLE,
  avg_p_alive DOUBLE,
  avg_predicted_purchases DOUBLE
)
LANGUAGE SQL
COMMENT 'Get CLV summary by tier: merchant counts, average and total projected lifetime value.'
RETURN
  SELECT clv_tier, COUNT(*) AS merchant_count,
         ROUND(AVG(clv_12m), 2) AS avg_clv,
         ROUND(SUM(clv_12m), 2) AS total_clv,
         ROUND(AVG(p_alive), 3) AS avg_p_alive,
         ROUND(AVG(predicted_purchases_12m), 1) AS avg_predicted_purchases
  FROM ahs_demos_catalog.cdp_360.gold_customer_ltv
  GROUP BY clv_tier
  ORDER BY avg_clv DESC;

-- 14. Get channel attribution results
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_channel_attribution()
RETURNS TABLE(
  channel STRING,
  markov_attributed_value DOUBLE,
  markov_attribution_share DOUBLE,
  first_touch_value DOUBLE,
  last_touch_value DOUBLE,
  linear_value DOUBLE,
  time_decay_value DOUBLE
)
LANGUAGE SQL
COMMENT 'Get multi-touch attribution by channel. Markov Chain (data-driven) plus heuristic models.'
RETURN
  SELECT channel, markov_attributed_value, markov_attribution_share,
         first_touch_value, last_touch_value, linear_value, time_decay_value
  FROM ahs_demos_catalog.cdp_360.gold_channel_attribution
  ORDER BY markov_attributed_value DESC;

-- 15. Get behavioral segments (K-Means clusters)
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_behavioral_segments()
RETURNS TABLE(
  behavioral_segment STRING,
  merchant_count BIGINT,
  avg_health_score DOUBLE,
  avg_txn_volume DOUBLE,
  avg_ticket_count DOUBLE,
  avg_days_inactive DOUBLE
)
LANGUAGE SQL
COMMENT 'Get behavioral segments from K-Means clustering with average metrics per cluster.'
RETURN
  SELECT behavioral_segment,
         COUNT(*) AS merchant_count,
         ROUND(AVG(health_score), 1) AS avg_health_score,
         ROUND(AVG(txn_volume), 2) AS avg_txn_volume,
         ROUND(AVG(ticket_count), 1) AS avg_ticket_count,
         ROUND(AVG(days_since_last_txn), 0) AS avg_days_inactive
  FROM ahs_demos_catalog.cdp_360.gold_behavioral_segments
  GROUP BY behavioral_segment
  ORDER BY avg_txn_volume DESC;

-- ═══════════════════════════════════════════════════════════════
-- CUSTOMER SUPPORT ANALYTICS FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 16. Get support analytics per merchant
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_support_analytics(
  quality_filter STRING DEFAULT 'all' COMMENT 'Filter: "excellent", "good", "fair", "poor", or "all"'
)
RETURNS TABLE(
  golden_id STRING,
  total_tickets BIGINT,
  open_tickets BIGINT,
  resolved_tickets BIGINT,
  avg_first_response_min DOUBLE,
  avg_resolution_min DOUBLE,
  csat_score DOUBLE,
  sla_first_response_pct DOUBLE,
  sla_resolution_pct DOUBLE,
  support_quality_tier STRING
)
LANGUAGE SQL
COMMENT 'Get support analytics per merchant: TTR, CSAT, SLA compliance. Filter by quality tier. Returns top 50.'
RETURN
  SELECT golden_id, total_tickets, open_tickets, resolved_tickets,
         avg_first_response_min, avg_resolution_min, csat_score,
         sla_first_response_pct, sla_resolution_pct, support_quality_tier
  FROM ahs_demos_catalog.cdp_360.gold_support_analytics
  WHERE quality_filter = 'all' OR support_quality_tier = quality_filter
  ORDER BY total_tickets DESC
  LIMIT 50;

-- 17. Get support KPIs (aggregate)
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_support_kpis()
RETURNS TABLE(
  total_merchants_with_tickets BIGINT,
  total_tickets BIGINT,
  total_open BIGINT,
  avg_first_response_min DOUBLE,
  avg_resolution_min DOUBLE,
  overall_csat DOUBLE,
  sla_first_response_pct DOUBLE,
  sla_resolution_pct DOUBLE
)
LANGUAGE SQL
COMMENT 'Aggregate support KPIs across all merchants.'
RETURN
  SELECT
    COUNT(*) AS total_merchants_with_tickets,
    SUM(total_tickets) AS total_tickets,
    SUM(open_tickets) AS total_open,
    ROUND(AVG(avg_first_response_min), 1) AS avg_first_response_min,
    ROUND(AVG(avg_resolution_min), 1) AS avg_resolution_min,
    ROUND(AVG(csat_score), 1) AS overall_csat,
    ROUND(AVG(sla_first_response_pct), 1) AS sla_first_response_pct,
    ROUND(AVG(sla_resolution_pct), 1) AS sla_resolution_pct
  FROM ahs_demos_catalog.cdp_360.gold_support_analytics;

-- ═══════════════════════════════════════════════════════════════
-- CALL CENTER ANALYTICS FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 18. Get call center agent performance
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_call_center_agents()
RETURNS TABLE(
  entity_id STRING,
  total_interactions BIGINT,
  voice_calls BIGINT,
  chat_sessions BIGINT,
  avg_handle_time_sec BIGINT,
  avg_queue_wait_sec BIGINT,
  resolution_rate DOUBLE,
  abandonment_rate DOUBLE
)
LANGUAGE SQL
COMMENT 'Get call center agent performance metrics. Returns top 30 agents by interaction count.'
RETURN
  SELECT entity_id, total_interactions, voice_calls, chat_sessions,
         avg_handle_time_sec, avg_queue_wait_sec, resolution_rate, abandonment_rate
  FROM ahs_demos_catalog.cdp_360.gold_call_center_analytics
  WHERE metric_type = 'agent'
  ORDER BY total_interactions DESC
  LIMIT 30;

-- 19. Get call center sentiment summary
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_call_center_sentiment()
RETURNS TABLE(
  topic_category STRING,
  interaction_count BIGINT,
  positive_pct DOUBLE,
  negative_pct DOUBLE,
  avg_duration_sec BIGINT
)
LANGUAGE SQL
COMMENT 'Get call center sentiment analysis by topic category.'
RETURN
  SELECT
    topic_category,
    COUNT(*) AS interaction_count,
    ROUND(COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS positive_pct,
    ROUND(COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct,
    ROUND(AVG(duration_seconds), 0) AS avg_duration_sec
  FROM ahs_demos_catalog.cdp_360.gold_call_center_sentiment
  GROUP BY topic_category
  ORDER BY interaction_count DESC;

-- ═══════════════════════════════════════════════════════════════
-- HYPER-PERSONALIZATION FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 20. Get personalization signals for a merchant
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_personalization_signals(
  merchant_id STRING COMMENT 'Golden ID of the merchant'
)
RETURNS TABLE(
  golden_id STRING,
  rfm_segment STRING,
  health_tier STRING,
  purchase_frequency_pattern STRING,
  preferred_time_slot STRING,
  recommended_channel STRING,
  upsell_propensity DOUBLE,
  churn_propensity DOUBLE,
  activation_propensity DOUBLE,
  content_theme STRING,
  merchant_tier STRING,
  next_best_action STRING,
  nba_urgency STRING
)
LANGUAGE SQL
COMMENT 'Get hyper-personalization signals for a merchant: timing, channel, propensity, content theme.'
RETURN
  SELECT golden_id, rfm_segment, health_tier, purchase_frequency_pattern,
         preferred_time_slot, recommended_channel, upsell_propensity,
         churn_propensity, activation_propensity, content_theme,
         merchant_tier, next_best_action, nba_urgency
  FROM ahs_demos_catalog.cdp_360.gold_personalization_signals
  WHERE golden_id = merchant_id;

-- ═══════════════════════════════════════════════════════════════
-- AD CREATIVE GENERATION FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 21. Get ad creative library by segment
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_ad_creative(
  segment_filter STRING DEFAULT 'all' COMMENT 'Segment name or "all"'
)
RETURNS TABLE(
  segment STRING,
  email_subject STRING,
  sms_message STRING,
  push_notification STRING,
  ad_headline STRING,
  ad_description STRING,
  tone STRING,
  cta STRING
)
LANGUAGE SQL
COMMENT 'Get AI-generated ad creative (email, SMS, push, ad copy) per segment.'
RETURN
  SELECT segment, email_subject, sms_message, push_notification,
         ad_headline, ad_description, tone, cta
  FROM ahs_demos_catalog.cdp_360.gold_ad_creative_library
  WHERE segment_filter = 'all' OR segment = segment_filter
  ORDER BY segment;

-- ═══════════════════════════════════════════════════════════════
-- CAMPAIGN ROI FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 22. Get campaign ROI summary
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_campaign_roi(
  campaign_type_filter STRING DEFAULT 'all' COMMENT 'Filter by campaign type or "all"'
)
RETURNS TABLE(
  campaign_type STRING,
  channel STRING,
  merchants_targeted BIGINT,
  conversions BIGINT,
  conversion_rate_pct DOUBLE,
  total_post_revenue DOUBLE,
  reactivations BIGINT,
  avg_days_to_convert DOUBLE
)
LANGUAGE SQL
COMMENT 'Get campaign ROI: conversion rate, revenue lift, reactivations by campaign type and channel.'
RETURN
  SELECT
    campaign_type, channel,
    COUNT(*) AS merchants_targeted,
    SUM(converted) AS conversions,
    ROUND(SUM(converted) * 100.0 / COUNT(*), 1) AS conversion_rate_pct,
    ROUND(SUM(post_30d_volume), 0) AS total_post_revenue,
    COUNT(CASE WHEN campaign_outcome = 'reactivated' THEN 1 END) AS reactivations,
    ROUND(AVG(days_to_first_txn), 0) AS avg_days_to_convert
  FROM ahs_demos_catalog.cdp_360.gold_campaign_roi
  WHERE campaign_type_filter = 'all' OR campaign_type = campaign_type_filter
  GROUP BY campaign_type, channel
  ORDER BY total_post_revenue DESC;

-- ═══════════════════════════════════════════════════════════════
-- AUDIENCE ACTIVATION FUNCTIONS
-- ═══════════════════════════════════════════════════════════════

-- 23. Get audience for activation
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_audience(
  audience_type STRING DEFAULT 'churn_risk' COMMENT 'Audience: churn_risk, high_value, new_onboarding, winback, growth, vip, immediate_action, all'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  email STRING,
  hashed_email STRING,
  rfm_segment STRING,
  health_score INT,
  txn_volume DOUBLE,
  primary_action STRING,
  urgency STRING
)
LANGUAGE SQL
COMMENT 'Get activation-ready audience lists by use case. Returns hashed emails for ad platform matching. Top 100 by volume.'
RETURN
  SELECT golden_id, merchant_name, email, hashed_email,
         rfm_segment, health_score, txn_volume,
         primary_action, urgency
  FROM ahs_demos_catalog.cdp_360.gold_audience_exports
  WHERE (
    (audience_type = 'churn_risk' AND audience_churn_risk)
    OR (audience_type = 'high_value' AND audience_high_value)
    OR (audience_type = 'new_onboarding' AND audience_new_onboarding)
    OR (audience_type = 'winback' AND audience_winback)
    OR (audience_type = 'growth' AND audience_growth)
    OR (audience_type = 'vip' AND audience_vip)
    OR (audience_type = 'immediate_action' AND audience_immediate_action)
    OR audience_type = 'all'
  )
  ORDER BY txn_volume DESC
  LIMIT 100;


-- ────────────────────────────────────────────────────────────────
-- 24. Anomaly Alerts
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_anomaly_alerts(
  alert_type STRING DEFAULT 'all' COMMENT 'Filter: volume_drop, unexpected_inactivity, ticket_spike, health_collapse, or all'
)
RETURNS TABLE(
  golden_id STRING,
  merchant_name STRING,
  segment STRING,
  health_score INT,
  anomaly_type STRING,
  deviation_pct DOUBLE,
  current_volume DOUBLE,
  avg_volume_30d DOUBLE,
  recommended_action STRING,
  urgency STRING,
  estimated_revenue_impact DOUBLE,
  detected_at TIMESTAMP
)
LANGUAGE SQL
COMMENT 'Get real-time anomaly alerts for merchants with unusual activity patterns. Returns top 50 by deviation.'
RETURN
  SELECT golden_id, merchant_name, segment, health_score,
         anomaly_type, deviation_pct, current_volume, avg_volume_30d,
         recommended_action, urgency, estimated_revenue_impact, detected_at
  FROM ahs_demos_catalog.cdp_360.gold_anomaly_alerts
  WHERE anomaly_type IS NOT NULL
    AND (alert_type = 'all' OR anomaly_type = alert_type)
  ORDER BY ABS(deviation_pct) DESC
  LIMIT 50;


-- ────────────────────────────────────────────────────────────────
-- 25. Merchant Activity Timeline
-- ────────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION ahs_demos_catalog.cdp_360.get_merchant_timeline(
  merchant_id STRING COMMENT 'Merchant golden_id'
)
RETURNS TABLE(
  event_type STRING,
  event_date STRING,
  description STRING,
  detail STRING,
  source STRING
)
LANGUAGE SQL
COMMENT 'Get a unified activity timeline for a merchant including transactions, support tickets, NBA recommendations, executed actions, and anomaly alerts. Returns last 30 events.'
RETURN
  WITH txn_events AS (
    SELECT 'transaction' AS event_type,
           DATE_FORMAT(COALESCE(last_txn_date, _refreshed_at), 'yyyy-MM-dd') AS event_date,
           CONCAT('Transaction volume: $', ROUND(txn_volume, 0)) AS description,
           CONCAT(txn_count, ' transactions') AS detail,
           'engagement' AS source
    FROM ahs_demos_catalog.cdp_360.gold_engagement_metrics
    WHERE golden_id = merchant_id
  ),
  nba_events AS (
    SELECT 'nba_recommendation' AS event_type,
           DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS event_date,
           CONCAT('Action: ', primary_action) AS description,
           CONCAT('Channel: ', primary_channel, ' | Urgency: ', urgency) AS detail,
           'nba' AS source
    FROM ahs_demos_catalog.cdp_360.gold_next_best_actions
    WHERE golden_id = merchant_id
  ),
  action_events AS (
    SELECT 'action_taken' AS event_type,
           DATE_FORMAT(executed_at, 'yyyy-MM-dd') AS event_date,
           CONCAT('Executed: ', action_type, ' via ', channel) AS description,
           COALESCE(notes, '') AS detail,
           'action_log' AS source
    FROM ahs_demos_catalog.cdp_360.nba_action_log
    WHERE golden_id = merchant_id
  ),
  anomaly_events AS (
    SELECT 'anomaly' AS event_type,
           DATE_FORMAT(detected_at, 'yyyy-MM-dd') AS event_date,
           CONCAT('Anomaly: ', anomaly_type) AS description,
           CONCAT('Deviation: ', ROUND(deviation_pct, 1), '%') AS detail,
           'anomaly' AS source
    FROM ahs_demos_catalog.cdp_360.gold_anomaly_alerts
    WHERE golden_id = merchant_id AND anomaly_type IS NOT NULL
  )
  SELECT * FROM txn_events
  UNION ALL SELECT * FROM nba_events
  UNION ALL SELECT * FROM action_events
  UNION ALL SELECT * FROM anomaly_events
  ORDER BY event_date DESC
  LIMIT 30;
