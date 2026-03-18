-- Gold: Call Center Analytics
-- Agent performance, call metrics, sentiment trends, queue analytics
-- Source: silver_interactions (Genesys) enriched with NLP via ai_query()

CREATE OR REPLACE MATERIALIZED VIEW gold_call_center_analytics
REFRESH EVERY 1 HOUR
AS
WITH interaction_enriched AS (
  SELECT
    conversation_id,
    agent_id,
    customer_external_id,
    conversation_date,
    media_type,
    direction,
    queue_name,
    duration_seconds,
    talk_time_seconds,
    hold_time_seconds,
    queue_wait_seconds,
    after_call_work_seconds,
    disposition,
    disconnect_type,
    wrap_up_code
  FROM silver_interactions
  WHERE conversation_date IS NOT NULL
),
agent_metrics AS (
  SELECT
    agent_id,
    COUNT(*) AS total_interactions,
    COUNT(CASE WHEN media_type = 'voice' THEN 1 END) AS voice_calls,
    COUNT(CASE WHEN media_type = 'chat' THEN 1 END) AS chat_sessions,
    COUNT(CASE WHEN media_type = 'email' THEN 1 END) AS email_interactions,
    ROUND(AVG(duration_seconds), 0) AS avg_handle_time_sec,
    ROUND(AVG(talk_time_seconds), 0) AS avg_talk_time_sec,
    ROUND(AVG(hold_time_seconds), 0) AS avg_hold_time_sec,
    ROUND(AVG(queue_wait_seconds), 0) AS avg_queue_wait_sec,
    ROUND(AVG(after_call_work_seconds), 0) AS avg_acw_sec,
    COUNT(CASE WHEN disposition = 'completed' THEN 1 END) AS completed_count,
    COUNT(CASE WHEN disposition IN ('abandoned', 'disconnected') THEN 1 END) AS abandoned_count,
    COUNT(CASE WHEN disconnect_type = 'customer' THEN 1 END) AS customer_disconnect_count,
    MIN(conversation_date) AS first_interaction,
    MAX(conversation_date) AS last_interaction
  FROM interaction_enriched
  GROUP BY agent_id
),
queue_metrics AS (
  SELECT
    queue_name,
    COUNT(*) AS queue_volume,
    ROUND(AVG(queue_wait_seconds), 0) AS avg_wait_sec,
    ROUND(PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY queue_wait_seconds), 0) AS p90_wait_sec,
    COUNT(CASE WHEN queue_wait_seconds <= 30 THEN 1 END) AS answered_within_30s,
    ROUND(AVG(duration_seconds), 0) AS avg_handle_time_sec,
    COUNT(CASE WHEN disposition IN ('abandoned', 'disconnected') THEN 1 END) AS abandoned
  FROM interaction_enriched
  GROUP BY queue_name
),
merchant_call_metrics AS (
  SELECT
    customer_external_id AS source_id,
    COUNT(*) AS total_calls,
    COUNT(CASE WHEN media_type = 'voice' THEN 1 END) AS voice_calls,
    ROUND(AVG(duration_seconds), 0) AS avg_call_duration_sec,
    ROUND(AVG(queue_wait_seconds), 0) AS avg_wait_time_sec,
    ROUND(AVG(hold_time_seconds), 0) AS avg_hold_sec,
    MAX(conversation_date) AS last_contact_date,
    COUNT(CASE WHEN disposition IN ('abandoned', 'disconnected') THEN 1 END) AS abandoned_calls,
    COUNT(DISTINCT agent_id) AS unique_agents
  FROM interaction_enriched
  WHERE customer_external_id IS NOT NULL
  GROUP BY customer_external_id
)
SELECT
  'agent' AS metric_type,
  agent_id AS entity_id,
  NULL AS golden_id,
  total_interactions,
  voice_calls,
  chat_sessions,
  email_interactions,
  avg_handle_time_sec,
  avg_talk_time_sec,
  avg_hold_time_sec,
  avg_queue_wait_sec,
  avg_acw_sec,
  CASE
    WHEN total_interactions > 0
    THEN ROUND(completed_count * 100.0 / total_interactions, 1)
  END AS resolution_rate,
  CASE
    WHEN total_interactions > 0
    THEN ROUND(abandoned_count * 100.0 / total_interactions, 1)
  END AS abandonment_rate,
  NULL AS queue_name,
  NULL AS service_level_pct,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM agent_metrics

UNION ALL

SELECT
  'queue' AS metric_type,
  queue_name AS entity_id,
  NULL AS golden_id,
  queue_volume AS total_interactions,
  NULL AS voice_calls,
  NULL AS chat_sessions,
  NULL AS email_interactions,
  avg_handle_time_sec,
  NULL AS avg_talk_time_sec,
  NULL AS avg_hold_time_sec,
  avg_wait_sec AS avg_queue_wait_sec,
  NULL AS avg_acw_sec,
  NULL AS resolution_rate,
  CASE
    WHEN queue_volume > 0
    THEN ROUND(abandoned * 100.0 / queue_volume, 1)
  END AS abandonment_rate,
  queue_name,
  CASE
    WHEN queue_volume > 0
    THEN ROUND(answered_within_30s * 100.0 / queue_volume, 1)
  END AS service_level_pct,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM queue_metrics;
