# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform - Marketing Campaign ROI Analysis
# MAGIC
# MAGIC Analyzes campaign effectiveness by measuring:
# MAGIC - **Conversion rate** (did merchant transact post-campaign?)
# MAGIC - **Revenue lift** (incremental revenue within 30 days)
# MAGIC - **Reactivation rate** (inactive merchants who returned)
# MAGIC - **ROI by channel** and **ROI by action type**
# MAGIC
# MAGIC Uses `gold_campaign_roi` materialized view and produces summary tables.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)
        return v if v else default
    except Exception:
        return default

catalog = _widget("catalog", "ahs_demos_catalog")
schema = _widget("schema", "cdp_360")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Campaign Performance Summary

# COMMAND ----------

campaign_summary = spark.sql(f"""
  SELECT
    campaign_type,
    channel,
    COUNT(*) AS merchants_targeted,
    SUM(converted) AS conversions,
    ROUND(SUM(converted) * 100.0 / COUNT(*), 1) AS conversion_rate_pct,
    ROUND(SUM(post_30d_volume), 0) AS total_post_revenue,
    ROUND(AVG(post_30d_volume), 0) AS avg_post_revenue,
    ROUND(AVG(post_30d_txn_count), 1) AS avg_post_txns,
    COUNT(CASE WHEN campaign_outcome = 'reactivated' THEN 1 END) AS reactivations,
    ROUND(AVG(days_to_first_txn), 0) AS avg_days_to_convert,
    ROUND(AVG(current_health_score), 1) AS avg_target_health
  FROM {catalog}.{schema}.gold_campaign_roi
  GROUP BY campaign_type, channel
  ORDER BY total_post_revenue DESC
""")

display(campaign_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. ROI by Channel

# COMMAND ----------

channel_roi = spark.sql(f"""
  SELECT
    channel,
    COUNT(*) AS total_targeted,
    SUM(converted) AS total_conversions,
    ROUND(SUM(converted) * 100.0 / COUNT(*), 1) AS conversion_rate,
    ROUND(SUM(post_30d_volume), 0) AS total_revenue,
    COUNT(CASE WHEN campaign_outcome = 'reactivated' THEN 1 END) AS reactivations,
    ROUND(SUM(post_30d_volume) / NULLIF(COUNT(*), 0), 0) AS revenue_per_target
  FROM {catalog}.{schema}.gold_campaign_roi
  GROUP BY channel
  ORDER BY total_revenue DESC
""")

display(channel_roi)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Outcome Distribution

# COMMAND ----------

outcome_dist = spark.sql(f"""
  SELECT
    campaign_outcome,
    COUNT(*) AS merchant_count,
    ROUND(AVG(post_30d_volume), 0) AS avg_revenue,
    ROUND(AVG(current_health_score), 1) AS avg_health,
    ROUND(AVG(days_to_first_txn), 0) AS avg_days_to_convert
  FROM {catalog}.{schema}.gold_campaign_roi
  GROUP BY campaign_outcome
  ORDER BY merchant_count DESC
""")

display(outcome_dist)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Write Summary to Gold Table

# COMMAND ----------

campaign_summary.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.gold_campaign_performance_summary"
)

channel_roi.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.gold_channel_roi_summary"
)

print("Campaign ROI analysis complete.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log to MLflow

# COMMAND ----------

import mlflow

mlflow.set_experiment("/Shared/cdp_campaign_roi")

with mlflow.start_run(run_name="campaign_roi_analysis"):
    summary_pd = campaign_summary.toPandas()
    if not summary_pd.empty:
        mlflow.log_metric("total_campaigns_analyzed", len(summary_pd))
        mlflow.log_metric("overall_conversion_rate", float(summary_pd["conversion_rate_pct"].mean()))
        mlflow.log_metric("total_post_campaign_revenue", float(summary_pd["total_post_revenue"].sum()))
        mlflow.log_metric("total_reactivations", int(summary_pd["reactivations"].sum()))

print("Campaign ROI analysis logged to MLflow.")
