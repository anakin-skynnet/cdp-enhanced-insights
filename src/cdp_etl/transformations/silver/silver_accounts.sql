-- Silver: Cleaned accounts (merchants) from Salesforce
-- Business entity for merchant 360 view

CREATE OR REPLACE STREAMING TABLE silver_accounts
CLUSTER BY (Id)
AS
SELECT
  Id AS external_id,
  'salesforce' AS source_system,
  TRIM(Name) AS account_name,
  REGEXP_REPLACE(Phone, '[^0-9]', '') AS phone_normalized,
  TRIM(BillingStreet) AS billing_street,
  TRIM(BillingCity) AS billing_city,
  CreatedDate AS created_at,
  _ingested_at
FROM stream(table(bronze_salesforce_accounts))
WHERE Id IS NOT NULL;
