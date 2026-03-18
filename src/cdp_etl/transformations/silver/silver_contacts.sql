-- Silver: Cleaned contacts from Salesforce
-- Deduplication, type casting, identity keys for entity resolution

CREATE OR REFRESH STREAMING TABLE silver_contacts
CLUSTER BY (Id)
AS
SELECT
  Id AS external_id,
  'salesforce' AS source_system,
  LOWER(TRIM(Email)) AS email,
  REGEXP_REPLACE(Phone, '[^0-9]', '') AS phone_normalized,
  TRIM(FirstName) AS first_name,
  TRIM(LastName) AS last_name,
  AccountId AS account_id,
  CreatedDate AS created_at,
  _ingested_at
FROM STREAM ${catalog}.${schema}.bronze_salesforce_contacts
WHERE Id IS NOT NULL
  AND (Email IS NOT NULL OR Phone IS NOT NULL);
