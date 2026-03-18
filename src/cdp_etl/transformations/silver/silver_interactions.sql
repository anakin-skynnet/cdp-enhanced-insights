-- Silver: Cleaned Genesys contact center interactions
-- Captures call metrics for Call Center Analytics (duration, queue, sentiment)

CREATE OR REFRESH STREAMING TABLE silver_interactions
CLUSTER BY (conversation_date)
AS
SELECT
  conversation_id,
  'genesys' AS source_system,
  user_id AS agent_id,
  COALESCE(customer_id, external_contact_id) AS customer_external_id,
  conversation_date,
  interaction_type,
  COALESCE(media_type, interaction_type) AS media_type,
  COALESCE(direction, 'inbound') AS direction,
  COALESCE(queue_name, 'default') AS queue_name,
  COALESCE(duration_seconds, 0) AS duration_seconds,
  COALESCE(talk_time_seconds, 0) AS talk_time_seconds,
  COALESCE(hold_time_seconds, 0) AS hold_time_seconds,
  COALESCE(queue_wait_seconds, 0) AS queue_wait_seconds,
  COALESCE(after_call_work_seconds, 0) AS after_call_work_seconds,
  COALESCE(disposition, 'completed') AS disposition,
  COALESCE(disconnect_type, 'agent') AS disconnect_type,
  transcript_text,
  COALESCE(wrap_up_code, '') AS wrap_up_code,
  _ingested_at
FROM STREAM ${catalog}.${schema}.bronze_genesys_interactions
WHERE conversation_id IS NOT NULL;
