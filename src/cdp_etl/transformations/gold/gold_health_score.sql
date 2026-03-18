-- Gold: Composite Merchant Health Score (0-100)
-- Blends transaction recency, frequency, monetary, support burden,
-- and trend signals into a single prioritization metric.
-- Higher score = healthier merchant.

CREATE OR REPLACE MATERIALIZED VIEW gold_health_score
REFRESH EVERY 1 HOUR
AS
WITH base AS (
  SELECT
    e.golden_id,
    e.txn_count,
    e.txn_volume,
    e.days_since_last_txn,
    e.ticket_count,
    e.first_txn_date,
    e.last_txn_date,
    COALESCE(s.r_score, 1) AS r_score,
    COALESCE(s.f_score, 1) AS f_score,
    COALESCE(s.m_score, 1) AS m_score,
    COALESCE(s.segment, 'unassigned') AS segment,
    COALESCE(e.tenure_days, 0) AS tenure_days
  FROM gold_engagement_metrics e
  LEFT JOIN gold_segments s ON e.golden_id = s.golden_id
),
scored AS (
  SELECT
    golden_id,
    segment,
    r_score,
    f_score,
    m_score,
    txn_count,
    txn_volume,
    days_since_last_txn,
    ticket_count,
    tenure_days,

    -- Recency component (0-30): penalize inactivity heavily
    CASE
      WHEN days_since_last_txn <= 7 THEN 30
      WHEN days_since_last_txn <= 14 THEN 26
      WHEN days_since_last_txn <= 30 THEN 22
      WHEN days_since_last_txn <= 60 THEN 15
      WHEN days_since_last_txn <= 90 THEN 8
      ELSE 0
    END AS recency_score,

    -- Frequency component (0-25): based on RFM f_score
    f_score * 5 AS frequency_score,

    -- Monetary component (0-25): based on RFM m_score
    m_score * 5 AS monetary_score,

    -- Support burden penalty (0 to -15): high ticket volume drags health down
    CASE
      WHEN ticket_count = 0 THEN 0
      WHEN ticket_count <= 2 THEN -2
      WHEN ticket_count <= 5 THEN -5
      WHEN ticket_count <= 10 THEN -10
      ELSE -15
    END AS support_penalty,

    -- Tenure bonus (0-10): longer-tenured merchants get a stability boost
    CASE
      WHEN tenure_days > 365 THEN 10
      WHEN tenure_days > 180 THEN 7
      WHEN tenure_days > 90 THEN 4
      WHEN tenure_days > 30 THEN 2
      ELSE 0
    END AS tenure_bonus

  FROM base
  WHERE txn_count > 0
)
SELECT
  golden_id,
  segment,
  r_score,
  f_score,
  m_score,
  txn_count,
  txn_volume,
  days_since_last_txn,
  ticket_count,
  tenure_days,
  recency_score,
  frequency_score,
  monetary_score,
  support_penalty,
  tenure_bonus,
  LEAST(100, GREATEST(0,
    recency_score + frequency_score + monetary_score + support_penalty + tenure_bonus
  )) AS health_score,
  CASE
    WHEN recency_score + frequency_score + monetary_score + support_penalty + tenure_bonus >= 75 THEN 'excellent'
    WHEN recency_score + frequency_score + monetary_score + support_penalty + tenure_bonus >= 55 THEN 'good'
    WHEN recency_score + frequency_score + monetary_score + support_penalty + tenure_bonus >= 35 THEN 'fair'
    WHEN recency_score + frequency_score + monetary_score + support_penalty + tenure_bonus >= 15 THEN 'poor'
    ELSE 'critical'
  END AS health_tier,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM scored;
