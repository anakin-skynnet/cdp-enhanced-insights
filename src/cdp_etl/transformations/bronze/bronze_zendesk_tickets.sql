-- Bronze: Zendesk support tickets (Lakeflow Connect or file landing)
-- Source: Lakeflow Connect Zendesk connector or Volume

CREATE OR REFRESH STREAMING TABLE bronze_zendesk_tickets (
  CONSTRAINT valid_id EXPECT (id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_timestamp EXPECT (created_at IS NOT NULL)
)
CLUSTER BY (created_at)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  COALESCE(_metadata.file_path, 'lakeflow_connect') AS _source_file
FROM STREAM read_files(
  '/Volumes/ahs_demos_catalog/cdp_360/raw/zendesk/tickets/',
  format => 'json',
  schemaHints => 'id STRING, subject STRING, status STRING, requester_id STRING, assignee_id STRING, created_at TIMESTAMP, updated_at TIMESTAMP',
  mode => 'PERMISSIVE'
);
