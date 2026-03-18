-- Gold: Merchant segments based on RFM (Recency, Frequency, Monetary)
-- For marketing activation and churn targeting

CREATE OR REPLACE MATERIALIZED VIEW gold_segments
REFRESH EVERY 1 HOUR
AS
WITH rfm AS (
  SELECT
    golden_id,
    txn_count AS frequency,
    txn_volume AS monetary,
    days_since_last_txn AS recency,
    NTILE(5) OVER (ORDER BY days_since_last_txn DESC) AS r_score,
    NTILE(5) OVER (ORDER BY txn_count) AS f_score,
    NTILE(5) OVER (ORDER BY txn_volume) AS m_score
  FROM gold_engagement_metrics
  WHERE txn_count > 0
),
segment_assign AS (
  SELECT
    golden_id,
    r_score,
    f_score,
    m_score,
    CASE
      WHEN r_score >= 4 AND f_score >= 4 AND m_score >= 4 THEN 'champions'
      WHEN r_score >= 4 AND f_score >= 2 THEN 'loyal'
      WHEN r_score >= 3 AND r_score <= 4 AND f_score <= 2 THEN 'potential_loyalists'
      WHEN r_score >= 4 AND f_score <= 1 THEN 'new_customers'
      WHEN r_score >= 3 AND f_score >= 3 AND m_score >= 3 THEN 'promising'
      WHEN r_score <= 2 AND f_score >= 2 AND m_score >= 2 THEN 'at_risk'
      WHEN r_score <= 2 AND f_score >= 3 THEN 'cant_lose'
      WHEN r_score <= 1 THEN 'hibernating'
      ELSE 'need_attention'
    END AS segment
  FROM rfm
)
SELECT
  golden_id,
  segment,
  r_score,
  f_score,
  m_score,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM segment_assign;
