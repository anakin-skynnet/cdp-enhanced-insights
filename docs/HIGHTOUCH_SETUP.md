# Hightouch Setup - PagoNxt Getnet CDP Activation

Hightouch enables reverse ETL from Databricks to Salesforce Marketing Cloud, Meta, Google Ads, and Power BI.

## 1. Connect Databricks

1. In Hightouch: **Sources** → **Add source** → **Databricks**
2. Configure:
   - **Host:** `https://<workspace>.azuredatabricks.net`
   - **HTTP Path:** From SQL warehouse connection details
   - **Personal Access Token:** Databricks PAT with SQL warehouse access
   - **Catalog:** `main` (or `${var.catalog}`)
   - **Schema:** `cdp` (or `${var.schema}`)

## 2. Create Models

### Model: gold_customer_360_for_sfmc

```sql
SELECT
  golden_id,
  email,
  phone,
  first_name,
  last_name,
  primary_source_id,
  primary_source_type
FROM ahs_demos_catalog.cdp_360.gold_customer_360
WHERE email IS NOT NULL
```

### Model: churn_risk_audience

```sql
SELECT
  c.golden_id,
  c.email,
  c.phone,
  e.days_since_last_txn,
  e.txn_volume,
  s.segment
FROM ahs_demos_catalog.cdp_360.gold_customer_360 c
JOIN ahs_demos_catalog.cdp_360.gold_engagement_metrics e ON c.golden_id = e.golden_id
LEFT JOIN ahs_demos_catalog.cdp_360.gold_segments s ON c.golden_id = s.golden_id
WHERE e.days_since_last_txn BETWEEN 60 AND 90
  AND s.segment IN ('at_risk', 'cant_lose')
```

## 3. Configure Syncs

### Salesforce Marketing Cloud

1. **Destination:** Salesforce Marketing Cloud
2. **Model:** gold_customer_360_for_sfmc or churn_risk_audience
3. **Sync schedule:** Every 6 hours or on-demand
4. **Field mapping:** Map golden_id → Subscriber Key, email → Email Address

### Meta Custom Audiences

1. **Destination:** Meta (Facebook) Ads
2. **Model:** churn_risk_audience
3. **Hashing:** Enable for PII (email, phone)
4. **Sync:** Daily

### Google Ads

1. **Destination:** Google Ads Customer Match
2. **Model:** churn_risk_audience
3. **Hashing:** Enable for PII

## 4. Sync Cadence

| Use Case | Model | Cadence |
|----------|-------|---------|
| Full audience refresh | gold_customer_360_for_sfmc | Every 6 hours |
| Churn prevention campaign | churn_risk_audience | Daily |
| Segment-based campaigns | Custom model per segment | On-demand or daily |
