-- Silver: Cleaned accounts (merchants) from Salesforce
-- Business entity for merchant 360 view

CREATE OR REFRESH STREAMING TABLE silver_accounts
CLUSTER BY (Id)
AS
SELECT
  Id AS external_id,
  'salesforce' AS source_system,
  TRIM(Name) AS account_name,
  REGEXP_REPLACE(COALESCE(Phone, ''), '[^0-9]', '') AS phone_normalized,
  TRIM(BillingStreet) AS billing_street,
  TRIM(BillingCity) AS billing_city,
  TRIM(BillingCountry) AS billing_country,
  TRIM(Industry) AS industry,
  TRIM(Type) AS account_type,
  COALESCE(AnnualRevenue, 0) AS annual_revenue,
  COALESCE(NumberOfEmployees, 0) AS number_of_employees,
  CreatedDate AS created_at,
  _ingested_at
FROM STREAM ${catalog}.${schema}.bronze_salesforce_accounts
WHERE Id IS NOT NULL;
