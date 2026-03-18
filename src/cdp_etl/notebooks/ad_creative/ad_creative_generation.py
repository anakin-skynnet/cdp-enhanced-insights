# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - AI Ad Creative Generation
# MAGIC
# MAGIC Uses Databricks Foundation Model API to generate personalized marketing content
# MAGIC at scale for each merchant segment:
# MAGIC - **Email subject lines** and body copy
# MAGIC - **SMS messages** for Zender
# MAGIC - **Push notification** copy for Getnet+ app
# MAGIC - **Ad headlines** for Meta/Google Ads
# MAGIC
# MAGIC Inspired by: [Building a Generative AI Workflow for Personalized Marketing Content](https://www.databricks.com/blog/building-generative-ai-workflow-creation-more-personalized-marketing-content)

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_dev"
llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Segment Profiles for Content Generation

# COMMAND ----------

from pyspark.sql import functions as F

segment_profiles = spark.sql(f"""
  SELECT
    s.segment,
    COUNT(DISTINCT c.golden_id) AS merchant_count,
    ROUND(AVG(e.txn_volume), 0) AS avg_volume,
    ROUND(AVG(e.txn_count), 0) AS avg_txn_count,
    ROUND(AVG(h.health_score), 0) AS avg_health,
    ROUND(AVG(e.days_since_last_txn), 0) AS avg_recency_days,
    ROUND(AVG(e.ticket_count), 1) AS avg_tickets
  FROM {catalog}.{schema}.gold_customer_360 c
  JOIN {catalog}.{schema}.gold_segments s ON c.golden_id = s.golden_id
  LEFT JOIN {catalog}.{schema}.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN {catalog}.{schema}.gold_health_score h ON c.golden_id = h.golden_id
  GROUP BY s.segment
  ORDER BY avg_volume DESC
""")

display(segment_profiles)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Generate Ad Creative per Segment using ai_query()

# COMMAND ----------

creative_df = spark.sql(f"""
  SELECT
    segment,
    merchant_count,
    avg_volume,
    avg_health,
    avg_recency_days,
    ai_query(
      '{llm_endpoint}',
      CONCAT(
        'You are a marketing copywriter for Getnet, a payment platform for merchants. ',
        'Generate personalized marketing content for the "', segment, '" segment. ',
        'This segment has ', CAST(merchant_count AS STRING), ' merchants, ',
        'avg payment volume $', CAST(avg_volume AS STRING), ', ',
        'avg health score ', CAST(avg_health AS STRING), '/100, ',
        'avg days since last transaction ', CAST(avg_recency_days AS STRING), '. ',
        'Generate exactly this JSON (no markdown, no extra text): ',
        '{{"email_subject": "...", "email_body_preview": "...(50 words max)", ',
        '"sms_message": "...(160 chars max)", ',
        '"push_notification": "...(80 chars max)", ',
        '"ad_headline": "...(30 chars max)", ',
        '"ad_description": "...(90 chars max)", ',
        '"tone": "...", "cta": "..."}}'
      )
    ) AS creative_json
  FROM (
    SELECT segment, merchant_count, avg_volume, avg_health, avg_recency_days
    FROM {catalog}.{schema}.gold_segments s
    JOIN (
      SELECT
        COALESCE(s2.segment, 'unassigned') AS segment,
        COUNT(DISTINCT c.golden_id) AS merchant_count,
        ROUND(AVG(e.txn_volume), 0) AS avg_volume,
        ROUND(AVG(h.health_score), 0) AS avg_health,
        ROUND(AVG(e.days_since_last_txn), 0) AS avg_recency_days
      FROM {catalog}.{schema}.gold_customer_360 c
      LEFT JOIN {catalog}.{schema}.gold_segments s2 ON c.golden_id = s2.golden_id
      LEFT JOIN {catalog}.{schema}.gold_engagement_metrics e ON c.golden_id = e.golden_id
      LEFT JOIN {catalog}.{schema}.gold_health_score h ON c.golden_id = h.golden_id
      GROUP BY COALESCE(s2.segment, 'unassigned')
    ) agg ON TRUE
    GROUP BY segment, merchant_count, avg_volume, avg_health, avg_recency_days
  )
""")

display(creative_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Parse and Save Creative Library

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

creative_schema = StructType([
    StructField("email_subject", StringType()),
    StructField("email_body_preview", StringType()),
    StructField("sms_message", StringType()),
    StructField("push_notification", StringType()),
    StructField("ad_headline", StringType()),
    StructField("ad_description", StringType()),
    StructField("tone", StringType()),
    StructField("cta", StringType()),
])

parsed = creative_df.withColumn(
    "creative", F.from_json(F.col("creative_json"), creative_schema)
).select(
    "segment",
    "merchant_count",
    "avg_volume",
    "avg_health",
    "avg_recency_days",
    F.col("creative.email_subject").alias("email_subject"),
    F.col("creative.email_body_preview").alias("email_body_preview"),
    F.col("creative.sms_message").alias("sms_message"),
    F.col("creative.push_notification").alias("push_notification"),
    F.col("creative.ad_headline").alias("ad_headline"),
    F.col("creative.ad_description").alias("ad_description"),
    F.col("creative.tone").alias("tone"),
    F.col("creative.cta").alias("cta"),
    F.current_timestamp().alias("generated_at"),
)

parsed.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_ad_creative_library")

print(f"Generated creative for {parsed.count()} segments")
display(parsed)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Log to MLflow

# COMMAND ----------

import mlflow

mlflow.set_experiment("/Shared/getnet_cdp_ad_creative")

with mlflow.start_run(run_name="ad_creative_generation"):
    mlflow.log_param("llm_endpoint", llm_endpoint)
    mlflow.log_param("segments_processed", parsed.count())
    mlflow.log_param("content_types", "email,sms,push,ad_headline,ad_description")

    for row in parsed.select("segment", "email_subject", "sms_message").collect():
        mlflow.log_param(f"sample_{row['segment']}_subject", row["email_subject"][:100] if row["email_subject"] else "")

print("Ad creative generation complete.")
