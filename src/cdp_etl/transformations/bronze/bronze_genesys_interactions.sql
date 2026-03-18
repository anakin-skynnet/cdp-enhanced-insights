-- Bronze: Genesys contact center interactions
-- Source: Airbyte/Fivetran sync to Volume
-- Path: /Volumes/{catalog}/{schema}/raw/genesys/

CREATE OR REPLACE STREAMING TABLE bronze_genesys_interactions
CLUSTER BY (conversation_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/main/cdp_dev/raw/genesys/',
  format => 'json',
  schemaHints => 'conversation_id STRING, user_id STRING, conversation_date TIMESTAMP, interaction_type STRING',
  mode => 'PERMISSIVE'
);
