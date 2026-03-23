# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform - AI Ad Creative Generation
# MAGIC
# MAGIC Uses Databricks Foundation Model API to generate personalized marketing content
# MAGIC at scale for each merchant segment.
# MAGIC
# MAGIC **Output**: `gold_ad_creative_library` with email, SMS, push, and ad copy per segment.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType
import mlflow

# COMMAND ----------

def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)
        return v if v else default
    except Exception:
        return default

catalog = _widget("catalog", "ahs_demos_catalog")
schema = _widget("schema", "cdp_360")
llm_endpoint = _widget("llm_endpoint", "databricks-gpt-5-4-nano")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Build Segment Profiles

# COMMAND ----------

segment_profiles = spark.sql(f"""
  SELECT
    COALESCE(s.segment, 'unassigned') AS segment,
    COUNT(DISTINCT c.golden_id) AS merchant_count,
    ROUND(AVG(e.txn_volume), 0) AS avg_volume,
    ROUND(AVG(e.txn_count), 0) AS avg_txn_count,
    ROUND(AVG(h.health_score), 0) AS avg_health,
    ROUND(AVG(e.days_since_last_txn), 0) AS avg_recency_days,
    ROUND(AVG(e.ticket_count), 1) AS avg_tickets
  FROM {catalog}.{schema}.gold_customer_360 c
  LEFT JOIN {catalog}.{schema}.gold_segments s ON c.golden_id = s.golden_id
  LEFT JOIN {catalog}.{schema}.gold_engagement_metrics e ON c.golden_id = e.golden_id
  LEFT JOIN {catalog}.{schema}.gold_health_score h ON c.golden_id = h.golden_id
  GROUP BY COALESCE(s.segment, 'unassigned')
  HAVING COUNT(DISTINCT c.golden_id) > 0
  ORDER BY avg_volume DESC
""")

print(f"Segment profiles: {segment_profiles.count()} segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generate Ad Creative per Segment using ai_query()

# COMMAND ----------

try:
    creative_df = segment_profiles.selectExpr(
        "segment", "merchant_count", "avg_volume", "avg_health", "avg_recency_days",
        f"""ai_query(
          '{llm_endpoint}',
          CONCAT(
            'You are a marketing copywriter for a payment platform for merchants. ',
            'Generate personalized marketing content for the "', segment, '" segment. ',
            'This segment has ', CAST(merchant_count AS STRING), ' merchants, ',
            'avg payment volume $', CAST(COALESCE(avg_volume, 0) AS STRING), ', ',
            'avg health score ', CAST(COALESCE(avg_health, 0) AS STRING), '/100, ',
            'avg days since last transaction ', CAST(COALESCE(avg_recency_days, 0) AS STRING), '. ',
            'Generate exactly this JSON (no markdown, no extra text): ',
            '{{"email_subject": "...", "sms_message": "...(160 chars max)", ',
            '"push_notification": "...(80 chars max)", ',
            '"ad_headline": "...(30 chars max)", ',
            '"ad_description": "...(90 chars max)", ',
            '"tone": "...", "cta": "..."}}'
          )
        ) AS creative_json"""
    )

    creative_schema = StructType([
        StructField("email_subject", StringType()),
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
        "segment", "merchant_count", "avg_volume", "avg_health", "avg_recency_days",
        F.col("creative.email_subject").alias("email_subject"),
        F.lit("").alias("email_body_preview"),
        F.col("creative.sms_message").alias("sms_message"),
        F.col("creative.push_notification").alias("push_notification"),
        F.col("creative.ad_headline").alias("ad_headline"),
        F.col("creative.ad_description").alias("ad_description"),
        F.col("creative.tone").alias("tone"),
        F.col("creative.cta").alias("cta"),
        F.current_timestamp().alias("generated_at"),
    )

    parsed.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_ad_creative_library")
    row_count = spark.table(f"{catalog}.{schema}.gold_ad_creative_library").count()
    print(f"Generated creative for {row_count} segments using {llm_endpoint}")

except Exception as e:
    print(f"LLM endpoint '{llm_endpoint}' not available: {e}")
    print("Generating fallback template-based creative...")

    templates = {
        "champions": ("Your loyalty deserves more", "Exclusive perks for our top merchants!", "You earned a reward!", "Top Merchant Perks", "Unlock exclusive benefits for our champions.", "celebratory", "Claim Your Reward"),
        "loyal": ("Thanks for your continued partnership", "Upgrade your plan for more benefits!", "Loyalty bonus inside!", "Loyal Merchant Bonus", "Grow faster with premium platform features.", "warm", "Upgrade Now"),
        "potential_loyalists": ("You're on track for great things", "See how other merchants grew 3x with us", "Growth tips inside!", "Grow Your Business", "Get personalized tips to boost your sales.", "encouraging", "See Tips"),
        "at_risk": ("We miss you — let's reconnect", "Special offer: 50% off fees this month", "Come back & save!", "Welcome Back Deal", "Return and save on fees.", "empathetic", "Reactivate Now"),
        "hibernating": ("It's been a while — check what's new", "New features since you last visited", "New tools await!", "What's New", "Discover powerful new payment features.", "re-engaging", "Explore Now"),
        "need_attention": ("Quick check-in on your account", "Optimize your setup in 5 minutes", "Quick optimization tip", "Optimize Your Setup", "Simple steps to get more from our platform.", "helpful", "Optimize Now"),
    }
    default = ("Grow your business with us", "Discover how we can help you grow", "New features available!", "Grow With Us", "Payment solutions that help you succeed.", "professional", "Learn More")

    from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, TimestampType

    template_map = F.create_map(
        *[item for seg, t in templates.items() for item in (F.lit(seg), F.array(*[F.lit(v) for v in t]))]
    )
    default_arr = F.array(*[F.lit(v) for v in default])

    fallback_df = segment_profiles.select(
        F.col("segment"),
        F.col("merchant_count"),
        F.coalesce(F.col("avg_volume"), F.lit(0.0)).alias("avg_volume"),
        F.coalesce(F.col("avg_health"), F.lit(0.0)).alias("avg_health"),
        F.coalesce(F.col("avg_recency_days"), F.lit(0.0)).alias("avg_recency_days"),
    ).withColumn(
        "_t", F.coalesce(template_map[F.col("segment")], default_arr)
    ).select(
        "segment", "merchant_count", "avg_volume", "avg_health", "avg_recency_days",
        F.col("_t")[0].alias("email_subject"),
        F.lit("").alias("email_body_preview"),
        F.col("_t")[1].alias("sms_message"),
        F.col("_t")[2].alias("push_notification"),
        F.col("_t")[3].alias("ad_headline"),
        F.col("_t")[4].alias("ad_description"),
        F.col("_t")[5].alias("tone"),
        F.col("_t")[6].alias("cta"),
        F.current_timestamp().alias("generated_at"),
    )

    fallback_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_ad_creative_library")
    row_count = spark.table(f"{catalog}.{schema}.gold_ad_creative_library").count()
    print(f"Saved {row_count} template-based creative entries (LLM fallback)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log to MLflow

# COMMAND ----------

mlflow.set_experiment(f"/Shared/cdp_ad_creative")

with mlflow.start_run(run_name="ad_creative_generation"):
    mlflow.log_param("llm_endpoint", llm_endpoint)
    row_count = spark.table(f"{catalog}.{schema}.gold_ad_creative_library").count()
    mlflow.log_param("segments_processed", row_count)
    mlflow.log_param("content_types", "email,sms,push,ad_headline,ad_description")

print("Ad creative generation complete.")
