# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Call Center Analytics: Speech-to-Text & Sentiment Analysis
# MAGIC
# MAGIC Uses Databricks Foundation Model API (`ai_query`) to enrich call center interactions:
# MAGIC - **Sentiment Analysis** on transcripts/notes
# MAGIC - **Topic Classification** for call categorization
# MAGIC - **Summarization** for agent assist
# MAGIC - **Entity Extraction** for compliance and routing
# MAGIC
# MAGIC Inspired by: [Databricks Call Centre Analytics](https://community.databricks.com/t5/technical-blog/ai-powered-call-centre-analytics-from-call-transcripts-to/ba-p/121951)

# COMMAND ----------

# MAGIC %pip install mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_360"
llm_endpoint = "databricks-meta-llama-3-3-70b-instruct"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Call Center Interactions

# COMMAND ----------

from pyspark.sql import functions as F

interactions = spark.table(f"{catalog}.{schema}.silver_interactions")

display(interactions.groupBy("media_type").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Sentiment Analysis using ai_query()
# MAGIC
# MAGIC Databricks `ai_analyze_sentiment()` provides a simple built-in for sentiment,
# MAGIC and `ai_query()` enables richer custom prompts for topic extraction, summarization, etc.

# COMMAND ----------

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

display(sentiment_df.limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Enriched Interactions to Gold Table

# COMMAND ----------

enriched = sentiment_df.select(
    "conversation_id",
    "agent_id",
    "customer_external_id",
    "conversation_date",
    "media_type",
    "duration_seconds",
    "sentiment",
    F.trim(F.lower(F.col("topic_category"))).alias("topic_category"),
    "call_summary",
    F.current_timestamp().alias("_enriched_at"),
)

enriched.write.mode("overwrite").saveAsTable(
    f"{catalog}.{schema}.gold_call_center_sentiment"
)

print(f"Wrote {enriched.count()} enriched interactions to gold_call_center_sentiment")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Aggregate Sentiment KPIs

# COMMAND ----------

sentiment_kpis = spark.sql(f"""
  SELECT
    topic_category,
    COUNT(*) AS interaction_count,
    ROUND(COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS positive_pct,
    ROUND(COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_pct,
    ROUND(COUNT(CASE WHEN sentiment = 'neutral' THEN 1 END) * 100.0 / COUNT(*), 1) AS neutral_pct,
    ROUND(COUNT(CASE WHEN sentiment = 'mixed' THEN 1 END) * 100.0 / COUNT(*), 1) AS mixed_pct,
    ROUND(AVG(duration_seconds), 0) AS avg_duration_sec
  FROM {catalog}.{schema}.gold_call_center_sentiment
  GROUP BY topic_category
  ORDER BY interaction_count DESC
""")

display(sentiment_kpis)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Agent Performance with Sentiment

# COMMAND ----------

agent_sentiment = spark.sql(f"""
  SELECT
    s.agent_id,
    COUNT(*) AS total_calls,
    ROUND(AVG(s.duration_seconds), 0) AS avg_handle_time,
    ROUND(COUNT(CASE WHEN s.sentiment = 'positive' THEN 1 END) * 100.0 / COUNT(*), 1) AS positive_rate,
    ROUND(COUNT(CASE WHEN s.sentiment = 'negative' THEN 1 END) * 100.0 / COUNT(*), 1) AS negative_rate,
    COUNT(DISTINCT s.topic_category) AS topic_breadth
  FROM {catalog}.{schema}.gold_call_center_sentiment s
  GROUP BY s.agent_id
  HAVING COUNT(*) >= 5
  ORDER BY positive_rate DESC
""")

display(agent_sentiment)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Log to MLflow

# COMMAND ----------

import mlflow

mlflow.set_experiment(f"/Shared/getnet_cdp_call_center_nlp")

with mlflow.start_run(run_name="call_center_sentiment_enrichment"):
    total = enriched.count()
    mlflow.log_param("llm_endpoint", llm_endpoint)
    mlflow.log_param("catalog", catalog)
    mlflow.log_param("schema", schema)
    mlflow.log_metric("total_enriched_interactions", total)

    sentiment_dist = enriched.groupBy("sentiment").count().toPandas()
    for _, row in sentiment_dist.iterrows():
        mlflow.log_metric(f"sentiment_{row['sentiment']}", row["count"])

    topic_dist = enriched.groupBy("topic_category").count().toPandas()
    for _, row in topic_dist.iterrows():
        mlflow.log_metric(f"topic_{row['topic_category']}", row["count"])

print("Call Center NLP enrichment complete.")
