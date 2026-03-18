-- Gold: Audience Activation / Export-ready segments
-- Pre-built audiences for ad platforms (Meta, Google Ads), SFMC, and direct export
-- Each row is a merchant with all targeting attributes needed for activation

CREATE OR REPLACE MATERIALIZED VIEW gold_audience_exports
REFRESH EVERY 1 HOUR
AS
WITH base AS (
  SELECT
    c.golden_id,
    COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
    c.email,
    c.phone,
    COALESCE(s.segment, 'unassigned') AS rfm_segment,
    COALESCE(h.health_score, 0) AS health_score,
    COALESCE(h.health_tier, 'unknown') AS health_tier,
    COALESCE(e.txn_volume, 0) AS txn_volume,
    COALESCE(e.txn_count, 0) AS txn_count,
    COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
    COALESCE(e.ticket_count, 0) AS ticket_count,
    n.primary_action,
    n.urgency,
    n.primary_channel,
    COALESCE(n.estimated_revenue_impact, 0) AS estimated_revenue_impact
  FROM ${catalog}.${schema}.gold_customer_360 c
  LEFT JOIN gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN gold_segments s ON c.golden_id = s.golden_id
  LEFT JOIN gold_health_score h ON c.golden_id = h.golden_id
  LEFT JOIN gold_next_best_actions n ON c.golden_id = n.golden_id
)
SELECT
  b.*,

  -- Pre-built audience flags for common activation use cases
  CASE
    WHEN b.rfm_segment IN ('at_risk', 'cant_lose') AND b.health_tier IN ('critical', 'poor')
    THEN TRUE ELSE FALSE
  END AS audience_churn_risk,

  CASE
    WHEN b.rfm_segment IN ('champions', 'loyal') AND b.txn_volume > 50000
    THEN TRUE ELSE FALSE
  END AS audience_high_value,

  CASE
    WHEN b.rfm_segment = 'new_customers' AND b.txn_count < 5
    THEN TRUE ELSE FALSE
  END AS audience_new_onboarding,

  CASE
    WHEN b.rfm_segment IN ('hibernating', 'need_attention') AND b.days_since_last_txn > 45
    THEN TRUE ELSE FALSE
  END AS audience_winback,

  CASE
    WHEN b.rfm_segment IN ('promising', 'potential_loyalists') AND b.health_tier IN ('good', 'excellent')
    THEN TRUE ELSE FALSE
  END AS audience_growth,

  CASE
    WHEN b.rfm_segment = 'champions' AND b.txn_volume > 100000
    THEN TRUE ELSE FALSE
  END AS audience_vip,

  CASE
    WHEN b.urgency = 'immediate' THEN TRUE ELSE FALSE
  END AS audience_immediate_action,

  -- Platform-specific hashed identifiers for matching
  SHA2(LOWER(TRIM(b.email)), 256) AS hashed_email,
  SHA2(REGEXP_REPLACE(b.phone, '[^0-9]', ''), 256) AS hashed_phone,

  CURRENT_TIMESTAMP() AS _refreshed_at
FROM base b
WHERE b.email IS NOT NULL OR b.phone IS NOT NULL;
