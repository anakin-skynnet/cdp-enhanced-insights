-- Gold: Customer Support Analytics per merchant
-- Aggregates ticket metrics: TTR, response velocity, CSAT, SLA compliance
-- Referenced by: Customer Support Analytics dashboard, Support Agent

CREATE OR REPLACE MATERIALIZED VIEW gold_support_analytics
SCHEDULE REFRESH EVERY 1 HOUR
AS
WITH ticket_metrics AS (
  SELECT
    customer_external_id AS source_id,
    COUNT(*) AS total_tickets,
    COUNT(CASE WHEN status IN ('open', 'pending', 'new') THEN 1 END) AS open_tickets,
    COUNT(CASE WHEN status IN ('solved', 'closed') THEN 1 END) AS resolved_tickets,
    COUNT(CASE WHEN priority IN ('high', 'urgent') THEN 1 END) AS high_priority_tickets,
    ROUND(AVG(first_response_minutes), 1) AS avg_first_response_min,
    ROUND(AVG(resolution_minutes), 1) AS avg_resolution_min,
    ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY resolution_minutes), 1) AS median_resolution_min,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY resolution_minutes), 1) AS p90_resolution_min,
    COUNT(CASE WHEN satisfaction_rating = 'good' THEN 1 END) AS csat_good,
    COUNT(CASE WHEN satisfaction_rating = 'bad' THEN 1 END) AS csat_bad,
    COUNT(CASE WHEN satisfaction_rating NOT IN ('unoffered', 'offered') THEN 1 END) AS csat_rated,
    COUNT(CASE WHEN first_response_minutes <= 60 THEN 1 END) AS sla_first_response_met,
    COUNT(CASE WHEN resolution_minutes <= 1440 THEN 1 END) AS sla_resolution_met,
    MAX(created_at) AS last_ticket_at,
    MIN(created_at) AS first_ticket_at,
    COUNT(DISTINCT DATE_TRUNC('month', created_at)) AS active_ticket_months,
    COLLECT_SET(ticket_type) AS ticket_types,
    COLLECT_SET(priority) AS priority_levels
  FROM ${catalog}.${schema}.silver_tickets
  WHERE customer_external_id IS NOT NULL
  GROUP BY customer_external_id
),
identity_lookup AS (
  SELECT source_id, golden_id FROM ${catalog}.${schema}.gold_identity_graph
)
SELECT
  COALESCE(i.golden_id, t.source_id) AS golden_id,
  t.total_tickets,
  t.open_tickets,
  t.resolved_tickets,
  t.high_priority_tickets,
  t.avg_first_response_min,
  t.avg_resolution_min,
  t.median_resolution_min,
  t.p90_resolution_min,
  CASE
    WHEN t.csat_rated > 0
    THEN ROUND(t.csat_good * 100.0 / t.csat_rated, 1)
    ELSE NULL
  END AS csat_score,
  t.csat_rated AS csat_responses,
  CASE
    WHEN t.total_tickets > 0
    THEN ROUND(t.sla_first_response_met * 100.0 / t.total_tickets, 1)
    ELSE NULL
  END AS sla_first_response_pct,
  CASE
    WHEN t.total_tickets > 0
    THEN ROUND(t.sla_resolution_met * 100.0 / t.total_tickets, 1)
    ELSE NULL
  END AS sla_resolution_pct,
  ROUND(t.total_tickets * 1.0 / GREATEST(t.active_ticket_months, 1), 1) AS tickets_per_month,
  CASE
    WHEN t.avg_resolution_min IS NULL THEN 'unknown'
    WHEN t.avg_resolution_min <= 120 THEN 'excellent'
    WHEN t.avg_resolution_min <= 480 THEN 'good'
    WHEN t.avg_resolution_min <= 1440 THEN 'fair'
    ELSE 'poor'
  END AS support_quality_tier,
  t.last_ticket_at,
  t.first_ticket_at,
  t.ticket_types,
  t.priority_levels,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM ticket_metrics t
LEFT JOIN identity_lookup i ON t.source_id = i.source_id;
