# Airbyte / Fivetran Setup - Bank Payment Platform CDP

Third-party connectors for Genesys, Apian, Oracle Siebel, and Qualtrics. Data lands in Unity Catalog volumes or tables for Bronze pipeline consumption.

## Genesys (Priority for MVP)

### Airbyte

1. Create Airbyte connection: Genesys (source) → Databricks (destination)
2. OAuth2 authentication with Genesys
3. Select streams: Users, Telephony, Stations, Routing, Locations, Analytics Conversation Details
4. Configure sync frequency (e.g., hourly)
5. Destination: Databricks Lakehouse - write to `ahs_demos_catalog.cdp_360_dev.airbyte_raw_genesys` or Volume path

### Fivetran

1. Create connector: Genesys Lite
2. Supports SaaS and Hybrid (Enterprise/Business Critical)
3. Incremental sync for most tables; re-import for ANALYTICS_CONVERSATION_DETAILS
4. Destination: Databricks - schema `cdp_dev`

### Expected Output

Bronze pipeline expects Genesys data at:
- **Table:** `bronze_genesys_interactions` (from Airbyte/Fivetran sync)
- **Or Volume:** `/Volumes/ahs_demos_catalog/cdp_360/raw/genesys/`

## Apian (Post-MVP)

- API-based connector or custom REST job
- Land in `/Volumes/ahs_demos_catalog/cdp_360/raw/apian/`

## Oracle Siebel (Chile - Post-MVP)

- Fivetran Oracle connector or JDBC via Databricks
- CDC or batch extraction

## Qualtrics (Post-MVP)

- Qualtrics API → Scheduled job → Delta table
- Survey responses, NPS, CSAT
