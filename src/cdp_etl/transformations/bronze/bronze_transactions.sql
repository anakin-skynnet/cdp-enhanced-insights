-- Bronze: Payment/transactional data (Getnet acquiring)
-- Source: Databricks transactional tables or Event Hub → Volume
-- Path: /Volumes/{catalog}/{schema}/raw/transactions/

CREATE OR REPLACE STREAMING TABLE bronze_transactions
CLUSTER BY (transaction_date)
CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_merchant EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW
CONSTRAINT valid_amount EXPECT (amount >= 0)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/ahs_demos_catalog/cdp_360/raw/transactions/',
  format => 'parquet',
  schemaHints => 'transaction_id STRING, merchant_id STRING, amount DECIMAL(18,2), transaction_date DATE, status STRING',
  mode => 'PERMISSIVE'
);
