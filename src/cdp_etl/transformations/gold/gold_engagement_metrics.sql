-- Gold: Engagement metrics per golden_id (merchant)
-- Aggregates from transactions for RFM and churn features
-- Ticket counts joined when requester_id maps to identity graph

CREATE OR REPLACE MATERIALIZED VIEW gold_engagement_metrics
SCHEDULE REFRESH EVERY 1 HOUR
AS
WITH txn_metrics AS (
  SELECT
    customer_external_id AS source_id,
    COUNT(*) AS txn_count,
    SUM(amount) AS txn_volume,
    MAX(transaction_date) AS last_txn_date,
    MIN(transaction_date) AS first_txn_date
  FROM ${catalog}.${schema}.silver_transactions
  WHERE status IN ('approved', 'APPROVED', 'completed')
  GROUP BY customer_external_id
),
ticket_counts AS (
  SELECT
    customer_external_id AS source_id,
    COUNT(*) AS ticket_count,
    MAX(created_at) AS last_ticket_at
  FROM ${catalog}.${schema}.silver_tickets
  WHERE customer_external_id IS NOT NULL
  GROUP BY customer_external_id
),
identity_lookup AS (
  SELECT source_id, golden_id FROM ${catalog}.${schema}.gold_identity_graph
)
SELECT
  COALESCE(i.golden_id, txn.source_id) AS golden_id,
  COALESCE(tc.ticket_count, 0) AS ticket_count,
  tc.last_ticket_at,
  COALESCE(txn.txn_count, 0) AS txn_count,
  COALESCE(txn.txn_volume, 0) AS txn_volume,
  txn.last_txn_date,
  txn.first_txn_date,
  DATEDIFF(CURRENT_DATE(), txn.last_txn_date) AS days_since_last_txn,
  GREATEST(0, DATEDIFF(txn.last_txn_date, txn.first_txn_date)) AS tenure_days,
  CURRENT_TIMESTAMP() AS _refreshed_at
FROM txn_metrics txn
LEFT JOIN identity_lookup i ON txn.source_id = i.source_id
LEFT JOIN ticket_counts tc ON txn.source_id = tc.source_id;
