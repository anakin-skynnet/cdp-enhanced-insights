-- Bronze: Salesforce Accounts (merchant/account data)
-- Source: Lakeflow Connect Salesforce connector

CREATE OR REPLACE STREAMING TABLE bronze_salesforce_accounts
CLUSTER BY (Id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  COALESCE(_metadata.file_path, 'lakeflow_connect') AS _source_file
FROM STREAM read_files(
  '/Volumes/ahs_demos_catalog/cdp_360/raw/salesforce/accounts/',
  format => 'json',
  schemaHints => 'Id STRING, Name STRING, Phone STRING, BillingStreet STRING, BillingCity STRING, CreatedDate TIMESTAMP',
  mode => 'PERMISSIVE'
);
