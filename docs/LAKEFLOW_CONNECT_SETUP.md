# Lakeflow Connect Setup - PagoNxt Getnet CDP

Lakeflow Connect pipelines are created via the Databricks UI or REST API. This guide documents the required configuration for Salesforce and Zendesk connectors.

## Prerequisites

- Unity Catalog enabled
- Serverless compute enabled
- `CREATE CONNECTION` privileges on the metastore
- `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE` on target locations

## 1. Salesforce (Argentina org - MVP)

### Connection

```sql
CREATE CONNECTION salesforce_ar
  TYPE salesforce
  OPTIONS (
    client_id 'your_connected_app_client_id',
    client_secret 'your_client_secret'
  );
```

### Ingestion Pipeline

Create ingestion pipeline targeting:
- **Catalog:** main (or ${var.catalog})
- **Schema:** cdp_dev (or ${var.schema})
- **Objects:** Contact, Account, Opportunity, Lead, Case (Service Cloud)

Salesforce Marketing Cloud data is supported via the same Salesforce connector when SFMC is linked to the org.

## 2. Zendesk Support

### Connection

```sql
CREATE CONNECTION zendesk_support
  TYPE zendesk
  OPTIONS (
    subdomain 'your_subdomain',
    api_token 'your_api_token'
  );
```

### Ingestion Pipeline

Create ingestion pipeline for:
- **Tables:** tickets, users, organizations, ticket_comments

## 3. Target Tables

Lakeflow Connect writes to streaming tables. Expected Bronze tables:

| Source | Table | Description |
|--------|-------|-------------|
| Salesforce | bronze_salesforce_contacts | Contact records |
| Salesforce | bronze_salesforce_accounts | Account records |
| Salesforce | bronze_salesforce_opportunities | Opportunity records |
| Zendesk | bronze_zendesk_tickets | Support tickets |
| Zendesk | bronze_zendesk_users | Zendesk users |

## 4. Post-Setup

After connectors are configured and running, the Bronze SDP pipeline (`cdp_ingestion`) can reference these tables for downstream Silver processing. If Lakeflow Connect writes to a different schema, update the Bronze pipeline to read from the correct source tables.
