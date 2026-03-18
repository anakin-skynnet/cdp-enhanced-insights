-- Bronze: Salesforce Accounts (merchant/account data)
-- Source: Lakeflow Connect Salesforce connector

CREATE OR REFRESH STREAMING TABLE bronze_salesforce_accounts (
  CONSTRAINT valid_id EXPECT (Id IS NOT NULL) ON VIOLATION DROP ROW
)
CLUSTER BY (Id)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  COALESCE(_metadata.file_path, 'lakeflow_connect') AS _source_file
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw/salesforce/accounts/',
  format => 'json',
  schemaHints => 'Id STRING, Name STRING, Phone STRING, BillingStreet STRING, BillingCity STRING, BillingCountry STRING, Industry STRING, Type STRING, AnnualRevenue DOUBLE, NumberOfEmployees LONG, CreatedDate TIMESTAMP',
  mode => 'PERMISSIVE'
);
