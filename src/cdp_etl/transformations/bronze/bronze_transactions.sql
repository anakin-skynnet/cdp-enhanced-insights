-- Bronze: Payment/transactional data (Getnet acquiring)
-- Source: Databricks transactional tables or Event Hub → Volume
-- Path: /Volumes/{catalog}/{schema}/raw/transactions/

CREATE OR REPLACE STREAMING TABLE bronze_transactions
CLUSTER BY (transaction_date)
AS
SELECT
  *,
  current_timestamp() AS _ingested_at,
  _metadata.file_path AS _source_file,
  _metadata.file_modification_time AS _file_timestamp
FROM STREAM read_files(
  '/Volumes/main/cdp_dev/raw/transactions/',
  format => 'parquet',
  schemaHints => 'transaction_id STRING, merchant_id STRING, amount DECIMAL(18,2), transaction_date DATE, status STRING',
  mode => 'PERMISSIVE'
);
