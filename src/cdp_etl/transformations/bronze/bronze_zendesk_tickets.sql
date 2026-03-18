-- Bronze: Zendesk support tickets (Lakeflow Connect or file landing)
-- Source: Lakeflow Connect Zendesk connector or Volume

CREATE OR REPLACE STREAMING TABLE bronze_zendesk_tickets
CLUSTER BY (created_at)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  COALESCE(_metadata.file_path, 'lakeflow_connect') AS _source_file
FROM STREAM read_files(
  '/Volumes/main/cdp_dev/raw/zendesk/tickets/',
  format => 'json',
  schemaHints => 'id STRING, subject STRING, status STRING, requester_id STRING, assignee_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP',
  mode => 'PERMISSIVE'
);
