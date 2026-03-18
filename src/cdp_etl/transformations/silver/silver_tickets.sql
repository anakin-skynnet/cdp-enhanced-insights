-- Silver: Cleaned Zendesk support tickets
-- Links to merchant/customer via requester_id
-- Captures resolution lifecycle metrics for Support Analytics

CREATE OR REFRESH STREAMING TABLE silver_tickets
CLUSTER BY (created_at)
AS
SELECT
  id AS ticket_id,
  'zendesk' AS source_system,
  requester_id AS customer_external_id,
  assignee_id,
  TRIM(subject) AS subject,
  status,
  COALESCE(priority, 'normal') AS priority,
  COALESCE(type, 'question') AS ticket_type,
  COALESCE(satisfaction_rating, 'unoffered') AS satisfaction_rating,
  tags,
  COALESCE(group_id, '') AS group_id,
  created_at,
  updated_at,
  COALESCE(first_response_at, updated_at) AS first_response_at,
  COALESCE(solved_at, CASE WHEN status IN ('solved', 'closed') THEN updated_at END) AS solved_at,
  COALESCE(closed_at, CASE WHEN status = 'closed' THEN updated_at END) AS closed_at,
  CASE
    WHEN first_response_at IS NOT NULL AND created_at IS NOT NULL
    THEN TIMESTAMPDIFF(MINUTE, created_at, first_response_at)
  END AS first_response_minutes,
  CASE
    WHEN solved_at IS NOT NULL AND created_at IS NOT NULL
    THEN TIMESTAMPDIFF(MINUTE, created_at, solved_at)
  END AS resolution_minutes,
  _ingested_at
FROM STREAM ${catalog}.${schema}.bronze_zendesk_tickets
WHERE id IS NOT NULL;
