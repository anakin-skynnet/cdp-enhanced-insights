-- Silver: Cleaned transactional data (Getnet payment volume)
-- Merchant-level payment metrics for RFM and churn

CREATE OR REFRESH STREAMING TABLE silver_transactions
CLUSTER BY (merchant_id, transaction_date)
AS
SELECT
  transaction_id,
  merchant_id AS customer_external_id,
  amount,
  transaction_date,
  status,
  _ingested_at
FROM STREAM ${catalog}.${schema}.bronze_transactions
WHERE merchant_id IS NOT NULL
  AND transaction_date IS NOT NULL;
