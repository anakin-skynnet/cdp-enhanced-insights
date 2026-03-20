-- Bronze: Payment/transactional data (acquiring)
-- Source: Databricks transactional tables or Event Hub -> Volume
-- Path: /Volumes/{catalog}/{schema}/raw/transactions/

CREATE OR REFRESH STREAMING TABLE bronze_transactions (
  CONSTRAINT valid_txn_id EXPECT (transaction_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_merchant EXPECT (merchant_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_amount EXPECT (amount >= 0)
)
CLUSTER BY (transaction_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/${catalog}/${schema}/raw/transactions/',
  format => 'parquet',
  schemaHints => 'transaction_id STRING, merchant_id STRING, amount DECIMAL(18,2), transaction_date DATE, status STRING',
  mode => 'PERMISSIVE'
);
