# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform CDP - Advanced Behavioral Segmentation
# MAGIC
# MAGIC Rule-based behavioral clustering using percentile bands on engagement features.
# MAGIC Avoids Spark ML / sklearn dependencies for maximum serverless compatibility.
# MAGIC
# MAGIC **Output**: `gold_behavioral_segments` with cluster labels and profiles.

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.window import Window

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
# MAGIC ## Step 1: Build Deduplicated Feature Matrix

# COMMAND ----------

engagement = (
    spark.table(f"{catalog}.{schema}.gold_engagement_metrics")
    .groupBy("golden_id")
    .agg(
        F.max("txn_count").alias("txn_count"),
        F.max("txn_volume").alias("txn_volume"),
        F.min("days_since_last_txn").alias("days_since_last_txn"),
        F.max("ticket_count").alias("ticket_count"),
    )
)
health = (
    spark.table(f"{catalog}.{schema}.gold_health_score")
    .groupBy("golden_id")
    .agg(
        F.max("health_score").alias("health_score"),
        F.max("tenure_days").alias("tenure_days"),
    )
)
segments = (
    spark.table(f"{catalog}.{schema}.gold_segments")
    .groupBy("golden_id")
    .agg(
        F.max("r_score").alias("r_score"),
        F.max("f_score").alias("f_score"),
        F.max("m_score").alias("m_score"),
    )
)

features = (
    engagement.alias("e")
    .join(health.alias("h"), "golden_id", "inner")
    .join(segments.alias("s"), "golden_id", "left")
    .select(
        F.col("e.golden_id"),
        F.col("e.txn_count").cast("double"),
        F.col("e.txn_volume").cast("double"),
        F.col("e.days_since_last_txn").cast("double"),
        F.col("e.ticket_count").cast("double"),
        F.col("h.health_score").cast("double"),
        F.col("h.tenure_days").cast("double"),
        F.coalesce(F.col("s.r_score"), F.lit(0)).cast("double").alias("r_score"),
        F.coalesce(F.col("s.f_score"), F.lit(0)).cast("double").alias("f_score"),
        F.coalesce(F.col("s.m_score"), F.lit(0)).cast("double").alias("m_score"),
    )
    .fillna(0)
)

count = features.count()
print(f"Feature matrix: {count:,} merchants x 9 features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Assign Behavioral Segments (Percentile-Based)

# COMMAND ----------

features_pct = (
    features
    .withColumn("vol_pct", F.percent_rank().over(Window.orderBy("txn_volume")))
    .withColumn("health_pct", F.percent_rank().over(Window.orderBy("health_score")))
    .withColumn("tenure_pct", F.percent_rank().over(Window.orderBy("tenure_days")))
    .withColumn("recency_pct", F.percent_rank().over(Window.orderBy(F.col("days_since_last_txn").desc())))
)

clustered = features_pct.withColumn(
    "cluster_id",
    F.when((F.col("health_pct") >= 0.7) & (F.col("vol_pct") >= 0.7), F.lit(0))
    .when(F.col("health_pct") < 0.2, F.lit(1))
    .when(F.col("tenure_pct") < 0.25, F.lit(2))
    .when(F.col("vol_pct") >= 0.75, F.lit(3))
    .when(F.col("health_pct") >= 0.4, F.lit(4))
    .otherwise(F.lit(5))
).withColumn(
    "behavioral_segment",
    F.when(F.col("cluster_id") == 0, F.lit("high_value_active"))
    .when(F.col("cluster_id") == 1, F.lit("at_risk_declining"))
    .when(F.col("cluster_id") == 2, F.lit("new_low_engagement"))
    .when(F.col("cluster_id") == 3, F.lit("premium_loyal"))
    .when(F.col("cluster_id") == 4, F.lit("stable_mid_tier"))
    .otherwise(F.lit("growth_potential"))
)

for row in clustered.groupBy("cluster_id", "behavioral_segment").count().orderBy("cluster_id").collect():
    print(f"  Cluster {row['cluster_id']} ({row['behavioral_segment']}) - {row['count']:,} merchants")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Save to Gold Table

# COMMAND ----------

result = (
    clustered
    .select(
        "golden_id", "cluster_id", "behavioral_segment",
        "txn_count", "txn_volume", "days_since_last_txn",
        "ticket_count", "health_score", "tenure_days",
        "r_score", "f_score", "m_score",
    )
    .withColumn("_model_version", F.lit("percentile_rules_v1"))
    .withColumn("_scored_at", F.current_timestamp())
)

result.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_behavioral_segments")
row_count = spark.table(f"{catalog}.{schema}.gold_behavioral_segments").count()
print(f"Saved {row_count:,} rows to {catalog}.{schema}.gold_behavioral_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Log to MLflow

# COMMAND ----------

import mlflow

with mlflow.start_run(run_name="behavioral_segmentation_rules"):
    mlflow.log_params({"model_type": "percentile_rules", "n_segments": 6, "n_features": 9, "merchant_count": count})

print("Behavioral segmentation complete.")
