# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Segmentation Refresh
# MAGIC
# MAGIC Refreshes merchant segments (RFM-based). Gold pipeline materialized view
# MAGIC handles the core logic; this job runs post-identity-resolution to ensure
# MAGIC gold_engagement_metrics and gold_segments are refreshed.
# MAGIC
# MAGIC **Note:** gold_segments is a materialized view refreshed by the pipeline.
# MAGIC This job can trigger a manual refresh or run validation.

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_dev"

# COMMAND ----------

# Refresh materialized views
spark.sql(f"REFRESH MATERIALIZED VIEW {catalog}.{schema}.gold_engagement_metrics")
spark.sql(f"REFRESH MATERIALIZED VIEW {catalog}.{schema}.gold_segments")

# COMMAND ----------

# Validation: segment distribution
spark.table(f"{catalog}.{schema}.gold_segments").groupBy("segment").count().orderBy("count", ascending=False).show()
