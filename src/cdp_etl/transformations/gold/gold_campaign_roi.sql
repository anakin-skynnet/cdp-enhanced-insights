-- Gold: Marketing Campaign ROI Analysis
-- Joins action log (campaigns) with post-action merchant behavior
-- to measure lift, conversion, and revenue attribution

CREATE OR REPLACE MATERIALIZED VIEW gold_campaign_roi
REFRESH EVERY 1 HOUR
AS
WITH campaigns AS (
  SELECT
    action_type AS campaign_type,
    channel,
    golden_id,
    notes AS campaign_name,
    executed_at AS campaign_date,
    ROW_NUMBER() OVER (PARTITION BY golden_id, action_type ORDER BY executed_at DESC) AS rn
  FROM ${catalog}.${schema}.nba_action_log
),
pre_action AS (
  SELECT
    c.golden_id,
    c.campaign_type,
    c.channel,
    c.campaign_name,
    c.campaign_date,
    COALESCE(e.txn_count, 0) AS current_txn_count,
    COALESCE(e.txn_volume, 0) AS current_txn_volume,
    COALESCE(e.days_since_last_txn, -1) AS current_days_inactive,
    COALESCE(h.health_score, 0) AS current_health_score,
    COALESCE(h.health_tier, 'unknown') AS current_health_tier,
    COALESCE(s.segment, 'unassigned') AS current_segment
  FROM campaigns c
  LEFT JOIN ${catalog}.${schema}.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN ${catalog}.${schema}.gold_health_score h ON c.golden_id = h.golden_id
  LEFT JOIN ${catalog}.${schema}.gold_segments s ON c.golden_id = s.golden_id
  WHERE c.rn = 1
),
post_action_txns AS (
  SELECT
    t.customer_external_id AS source_id,
    c.golden_id,
    c.campaign_type,
    c.campaign_date,
    COUNT(*) AS post_txn_count,
    COALESCE(SUM(t.amount), 0) AS post_txn_volume,
    MIN(t.transaction_date) AS first_post_txn_date
  FROM campaigns c
  JOIN ${catalog}.${schema}.gold_identity_graph ig ON c.golden_id = ig.golden_id
  JOIN ${catalog}.${schema}.silver_transactions t
    ON ig.source_id = t.customer_external_id
    AND t.transaction_date > c.campaign_date
    AND t.transaction_date <= DATEADD(DAY, 30, c.campaign_date)
  WHERE c.rn = 1
  GROUP BY t.customer_external_id, c.golden_id, c.campaign_type, c.campaign_date
)
SELECT
  p.campaign_type,
  p.channel,
  p.campaign_name,
  p.campaign_date,
  p.golden_id,

  p.current_txn_volume AS pre_volume,
  COALESCE(pt.post_txn_volume, 0) AS post_30d_volume,
  COALESCE(pt.post_txn_count, 0) AS post_30d_txn_count,
  COALESCE(pt.post_txn_volume, 0) - p.current_txn_volume AS revenue_lift,

  CASE WHEN pt.post_txn_count > 0 THEN 1 ELSE 0 END AS converted,

  p.current_health_score,
  p.current_health_tier,
  p.current_segment,
  p.current_days_inactive,

  DATEDIFF(pt.first_post_txn_date, p.campaign_date) AS days_to_first_txn,

  CASE
    WHEN pt.post_txn_count > 0 AND p.current_days_inactive > 30 THEN 'reactivated'
    WHEN pt.post_txn_count > 0 THEN 'engaged'
    ELSE 'no_response'
  END AS campaign_outcome,

  CURRENT_TIMESTAMP() AS _refreshed_at
FROM pre_action p
LEFT JOIN post_action_txns pt
  ON p.golden_id = pt.golden_id
  AND p.campaign_type = pt.campaign_type;
