# Genie Space Setup - PagoNxt Getnet CDP

Create a Genie Space for natural language analytics on the Customer 360 golden record.

## Create via Databricks UI

1. Navigate to **AI/BI** → **Genie** → **Spaces**
2. Click **Create Space**
3. **Title:** Getnet Merchant 360 Analytics
4. **Description:** Ask questions about merchant data, churn risk, segments, and engagement
5. **SQL Warehouse:** Select shared warehouse
6. **Tables:** Add:
   - `ahs_demos_catalog.cdp_360.gold_customer_360`
   - `ahs_demos_catalog.cdp_360.gold_engagement_metrics`
   - `ahs_demos_catalog.cdp_360.gold_segments`
   - `ahs_demos_catalog.cdp_360.gold_identity_graph`
7. **Sample questions:**
   - "Show me churn risk by segment"
   - "Top 10 merchants by payment volume"
   - "How many merchants are in the at_risk segment?"
   - "What is the distribution of segments?"

## Create via API

```python
import requests

url = "https://<workspace>.azuredatabricks.net/api/2.0/genie/spaces"
headers = {"Authorization": f"Bearer {token}"}
payload = {
    "warehouse_id": "<warehouse_id>",
    "title": "Getnet Merchant 360 Analytics",
    "description": "Ask questions about merchant data, churn risk, segments, and engagement",
    "serialized_space": {
        "version": 1,
        "config": {
            "tables": [
                {"catalog": "main", "schema": "cdp", "table": "gold_customer_360"},
                {"catalog": "main", "schema": "cdp", "table": "gold_engagement_metrics"},
                {"catalog": "main", "schema": "cdp", "table": "gold_segments"}
            ],
            "sample_questions": [
                "Show me churn risk by segment",
                "Top 10 merchants by payment volume",
                "How many merchants are in the at_risk segment?"
            ]
        }
    }
}
response = requests.post(url, headers=headers, json=payload)
```

## DABs Resource (if supported)

Add to `resources/genie_spaces.yml`:

```yaml
resources:
  genie_spaces:
    getnet_merchant_360:
      title: "[${bundle.target}] Getnet Merchant 360 Analytics"
      description: "Ask questions about merchant data, churn risk, segments, and engagement"
      warehouse_id: ${var.warehouse_id}
      # serialized_space from API or UI export
      permissions:
        - level: CAN_VIEW
          group_name: "users"
```
