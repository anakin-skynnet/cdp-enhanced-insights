-- Bronze: Salesforce Contacts (Lakeflow Connect or file landing)
-- If Lakeflow Connect: reads from connector-created table
-- If file landing: reads from Volume
-- Uses coalesce to support both patterns - configure source_path for file-based

CREATE OR REFRESH STREAMING TABLE bronze_salesforce_contacts (
  CONSTRAINT valid_id EXPECT (Id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_identity EXPECT (Email IS NOT NULL OR Phone IS NOT NULL)
)
CLUSTER BY (Id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  COALESCE(_metadata.file_path, 'lakeflow_connect') AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw/salesforce/contacts/',
  format => 'json',
  schemaHints => 'Id STRING, Email STRING, Phone STRING, FirstName STRING, LastName STRING, AccountId STRING, CreatedDate TIMESTAMP',
  mode => 'PERMISSIVE'
);
