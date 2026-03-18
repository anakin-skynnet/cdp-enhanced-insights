-- Anomaly detection: flags merchants with unusual activity patterns.
-- Compares recent 7-day behavior against 30-day baseline.
-- Detects volume drops, sudden ticket spikes, and inactivity signals.

CREATE OR REPLACE MATERIALIZED VIEW gold_anomaly_alerts
COMMENT 'Real-time anomaly alerts for merchant activity deviations'
REFRESH EVERY 2 HOURS
AS
WITH baseline AS (
  SELECT
    golden_id,
    AVG(txn_volume) AS avg_volume_30d,
    STDDEV(txn_volume) AS stddev_volume_30d,
    AVG(txn_count) AS avg_txn_count_30d,
    AVG(ticket_count) AS avg_tickets_30d
  FROM ${catalog}.${schema}.gold_engagement_metrics
  GROUP BY golden_id
),
current_state AS (
  SELECT
    e.golden_id,
    e.txn_volume AS current_volume,
    e.txn_count AS current_txn_count,
    e.days_since_last_txn,
    e.ticket_count AS current_tickets,
    h.health_score,
    h.health_tier,
    s.segment,
    n.primary_action,
    n.urgency,
    n.estimated_revenue_impact
  FROM ${catalog}.${schema}.gold_engagement_metrics e
  LEFT JOIN ${catalog}.${schema}.gold_health_score h ON e.golden_id = h.golden_id
  LEFT JOIN ${catalog}.${schema}.gold_segments s ON e.golden_id = s.golden_id
  LEFT JOIN ${catalog}.${schema}.gold_next_best_actions n ON e.golden_id = n.golden_id
)
SELECT
  c.golden_id,
  COALESCE(m.merchant_name, m.first_name, '') AS merchant_name,
  c.segment,
  c.health_score,
  c.health_tier,
  c.current_volume,
  b.avg_volume_30d,

  CASE
    WHEN b.stddev_volume_30d > 0
         AND c.current_volume < (b.avg_volume_30d - 2 * b.stddev_volume_30d)
    THEN 'volume_drop'
    WHEN c.days_since_last_txn > 30 AND c.segment NOT IN ('hibernating')
    THEN 'unexpected_inactivity'
    WHEN c.current_tickets > (b.avg_tickets_30d + 3)
    THEN 'ticket_spike'
    WHEN COALESCE(c.health_score, 0) < 20 AND c.segment IN ('champions', 'loyal')
    THEN 'health_collapse'
  END AS anomaly_type,

  CASE
    WHEN c.current_volume < (b.avg_volume_30d - 2 * COALESCE(b.stddev_volume_30d, 0))
    THEN ROUND((b.avg_volume_30d - c.current_volume) / NULLIF(b.avg_volume_30d, 0) * 100, 1)
    WHEN c.current_tickets > (b.avg_tickets_30d + 3)
    THEN ROUND((c.current_tickets - b.avg_tickets_30d) / NULLIF(b.avg_tickets_30d, 0) * 100, 1)
    ELSE 0
  END AS deviation_pct,

  c.primary_action AS recommended_action,
  c.urgency,
  c.estimated_revenue_impact,
  current_timestamp() AS detected_at

FROM current_state c
JOIN baseline b ON c.golden_id = b.golden_id
LEFT JOIN ${catalog}.${schema}.gold_customer_360 m ON c.golden_id = m.golden_id
WHERE
  (b.stddev_volume_30d > 0
   AND c.current_volume < (b.avg_volume_30d - 2 * b.stddev_volume_30d))
  OR (c.days_since_last_txn > 30 AND c.segment NOT IN ('hibernating'))
  OR (c.current_tickets > (b.avg_tickets_30d + 3))
  OR (COALESCE(c.health_score, 0) < 20 AND c.segment IN ('champions', 'loyal'))
