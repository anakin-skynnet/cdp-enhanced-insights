# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform - Call Center Analytics: Speech-to-Text & Sentiment Analysis
# MAGIC
# MAGIC Uses Databricks Foundation Model API (`ai_query`) to enrich call center interactions:
# MAGIC - **Sentiment Analysis** on transcripts/notes
# MAGIC - **Topic Classification** for call categorization
# MAGIC - **Summarization** for agent assist
# MAGIC
# MAGIC **Output**: `gold_call_center_sentiment` with enriched interactions.

# COMMAND ----------

from pyspark.sql import functions as F
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
# MAGIC ## 1. Load Call Center Interactions

# COMMAND ----------

interactions = spark.table(f"{catalog}.{schema}.silver_interactions")
valid_interactions = interactions.filter(
    F.col("transcript_text").isNotNull() & (F.length(F.trim(F.col("transcript_text"))) > 20)
)
total = valid_interactions.count()
print(f"Interactions with transcripts: {total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Sentiment Analysis and Topic Classification

# COMMAND ----------

try:
    sentiment_df = spark.sql(f"""
      SELECT
        conversation_id,
        agent_id,
        customer_external_id,
        conversation_date,
        media_type,
        duration_seconds,
        transcript_text,
        ai_analyze_sentiment(transcript_text) AS sentiment,
        ai_query(
          '{llm_endpoint}',
          CONCAT(
            'Classify this customer support interaction into ONE of these categories: ',
            'billing, technical_issue, onboarding, account_inquiry, complaint, fraud, ',
            'feature_request, cancellation, positive_feedback, other. ',
            'Return ONLY the category name. Text: ',
            LEFT(transcript_text, 2000)
          )
        ) AS topic_category,
        ai_query(
          '{llm_endpoint}',
          CONCAT(
            'Summarize this customer support interaction in 1-2 sentences. ',
            'Focus on the issue and resolution. Text: ',
            LEFT(transcript_text, 3000)
          )
        ) AS call_summary
      FROM {catalog}.{schema}.silver_interactions
      WHERE transcript_text IS NOT NULL
        AND LENGTH(TRIM(transcript_text)) > 20
    """)

    enriched = sentiment_df.select(
        "conversation_id", "agent_id", "customer_external_id",
        "conversation_date", "media_type", "duration_seconds",
        "sentiment",
        F.trim(F.lower(F.col("topic_category"))).alias("topic_category"),
        "call_summary",
        F.current_timestamp().alias("_enriched_at"),
    )

    enriched.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_call_center_sentiment")
    row_count = spark.table(f"{catalog}.{schema}.gold_call_center_sentiment").count()
    print(f"Wrote {row_count:,} enriched interactions using LLM endpoint")

except Exception as e:
    print(f"LLM endpoint '{llm_endpoint}' not available: {e}")
    print("Generating rule-based sentiment enrichment...")

    import hashlib

    enriched_fallback = valid_interactions.select(
        "conversation_id", "agent_id", "customer_external_id",
        "conversation_date", "media_type", "duration_seconds",
        F.expr("""
          CASE
            WHEN LOWER(transcript_text) LIKE '%thank%' OR LOWER(transcript_text) LIKE '%great%'
                 OR LOWER(transcript_text) LIKE '%excellent%' OR LOWER(transcript_text) LIKE '%happy%'
            THEN 'positive'
            WHEN LOWER(transcript_text) LIKE '%frustrat%' OR LOWER(transcript_text) LIKE '%angry%'
                 OR LOWER(transcript_text) LIKE '%terrible%' OR LOWER(transcript_text) LIKE '%complain%'
            THEN 'negative'
            WHEN LOWER(transcript_text) LIKE '%issue%' OR LOWER(transcript_text) LIKE '%problem%'
            THEN 'mixed'
            ELSE 'neutral'
          END
        """).alias("sentiment"),
        F.expr("""
          CASE
            WHEN LOWER(transcript_text) LIKE '%bill%' OR LOWER(transcript_text) LIKE '%charge%'
                 OR LOWER(transcript_text) LIKE '%payment%' THEN 'billing'
            WHEN LOWER(transcript_text) LIKE '%error%' OR LOWER(transcript_text) LIKE '%crash%'
                 OR LOWER(transcript_text) LIKE '%bug%' THEN 'technical_issue'
            WHEN LOWER(transcript_text) LIKE '%setup%' OR LOWER(transcript_text) LIKE '%onboard%'
                 OR LOWER(transcript_text) LIKE '%start%' THEN 'onboarding'
            WHEN LOWER(transcript_text) LIKE '%fraud%' OR LOWER(transcript_text) LIKE '%unauthoriz%'
            THEN 'fraud'
            WHEN LOWER(transcript_text) LIKE '%cancel%' THEN 'cancellation'
            WHEN LOWER(transcript_text) LIKE '%feature%' OR LOWER(transcript_text) LIKE '%request%'
            THEN 'feature_request'
            ELSE 'account_inquiry'
          END
        """).alias("topic_category"),
        F.expr("LEFT(transcript_text, 200)").alias("call_summary"),
        F.current_timestamp().alias("_enriched_at"),
    )

    enriched_fallback.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.gold_call_center_sentiment")
    row_count = spark.table(f"{catalog}.{schema}.gold_call_center_sentiment").count()
    print(f"Wrote {row_count:,} rule-based enriched interactions (LLM fallback)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log to MLflow

# COMMAND ----------

mlflow.set_experiment(f"/Shared/cdp_call_center_nlp")

with mlflow.start_run(run_name="call_center_sentiment_enrichment"):
    result_count = spark.table(f"{catalog}.{schema}.gold_call_center_sentiment").count()
    mlflow.log_param("llm_endpoint", llm_endpoint)
    mlflow.log_param("catalog", catalog)
    mlflow.log_param("schema", schema)
    mlflow.log_metric("total_enriched_interactions", result_count)

    sentiment_dist = spark.table(f"{catalog}.{schema}.gold_call_center_sentiment") \
        .groupBy("sentiment").count().toPandas()
    for _, row in sentiment_dist.iterrows():
        mlflow.log_metric(f"sentiment_{row['sentiment']}", int(row["count"]))

print("Call Center NLP enrichment complete.")
