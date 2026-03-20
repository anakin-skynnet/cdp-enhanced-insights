# SQL Alerts Setup - Bank Payment Platform CDP

Create SQL alerts manually in the Databricks workspace for pipeline failure monitoring.

## Pipeline Failure Alert

1. Go to **SQL** → **Alerts** → **Create Alert**
2. **Name:** CDP Pipeline Failure Alert
3. **Query:**
```sql
SELECT COALESCE(COUNT(*), 0) AS failure_count
FROM system.operational_data.pipeline_runs
WHERE status = 'FAILED'
  AND start_time > CURRENT_TIMESTAMP() - INTERVAL 24 HOURS
```
4. **Condition:** failure_count > 0
5. **Schedule:** Every 6 hours
6. **Notifications:** Email to relevant users

## Reference

See [resources/alerts.yml](../resources/alerts.yml) for the intended YAML configuration (may require CLI updates for full DAB support).
