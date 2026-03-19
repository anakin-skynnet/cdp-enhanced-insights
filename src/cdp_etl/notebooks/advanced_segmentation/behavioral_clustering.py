# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet CDP - Advanced Behavioral Segmentation
# MAGIC
# MAGIC Implements the Databricks [Customer Segmentation Solution Accelerator](https://www.databricks.com/solutions/accelerators/customer-segmentation)
# MAGIC adapted for PagoNxt Getnet.
# MAGIC
# MAGIC Goes beyond rule-based RFM by using **K-Means clustering** on a rich
# MAGIC behavioral feature set to discover natural merchant groupings.
# MAGIC
# MAGIC **Features used**:
# MAGIC - RFM scores (recency, frequency, monetary)
# MAGIC - Support intensity (ticket count, recency of last ticket)
# MAGIC - Transaction trend (volume change over time)
# MAGIC - Tenure
# MAGIC - Health score components
# MAGIC
# MAGIC **Output**: `gold_behavioral_segments` with cluster labels and profiles.

# COMMAND ----------

# MAGIC %pip install scikit-learn mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import silhouette_score
import pyspark.sql.functions as F
import mlflow
import mlflow.sklearn

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if "schema" in [w.name for w in dbutils.widgets.getAll()] else "cdp_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Feature Matrix
# MAGIC
# MAGIC Combine engagement metrics, health scores, and CLV predictions
# MAGIC into a multi-dimensional feature set for clustering.

# COMMAND ----------

engagement = spark.table(f"{catalog}.{schema}.gold_engagement_metrics")
health = spark.table(f"{catalog}.{schema}.gold_health_score")
segments = spark.table(f"{catalog}.{schema}.gold_segments")

ltv_exists = spark.catalog.tableExists(f"{catalog}.{schema}.gold_customer_ltv")
if ltv_exists:
    ltv = spark.table(f"{catalog}.{schema}.gold_customer_ltv").select(
        "golden_id", "clv_12m", "p_alive", "predicted_purchases_12m"
    )

features = (
    engagement.alias("e")
    .join(health.alias("h"), "golden_id", "inner")
    .join(segments.alias("s"), "golden_id", "left")
    .select(
        F.col("e.golden_id"),
        F.col("e.txn_count"),
        F.col("e.txn_volume"),
        F.col("e.days_since_last_txn"),
        F.col("e.ticket_count"),
        F.col("h.health_score"),
        F.col("h.recency_score"),
        F.col("h.frequency_score"),
        F.col("h.monetary_score"),
        F.col("h.support_penalty"),
        F.col("h.tenure_bonus"),
        F.col("h.tenure_days"),
        F.coalesce(F.col("s.r_score"), F.lit(0)).alias("r_score"),
        F.coalesce(F.col("s.f_score"), F.lit(0)).alias("f_score"),
        F.coalesce(F.col("s.m_score"), F.lit(0)).alias("m_score"),
    )
)

if ltv_exists:
    features = features.join(ltv, "golden_id", "left").fillna(0, subset=["clv_12m", "p_alive", "predicted_purchases_12m"])

features_pd = features.toPandas()
print(f"Feature matrix: {features_pd.shape[0]:,} merchants x {features_pd.shape[1] - 1} features")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Scale Features and Find Optimal K

# COMMAND ----------

feature_cols = [c for c in features_pd.columns if c != "golden_id"]
X = features_pd[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0).values

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

k_range = range(3, 10)
silhouette_scores = {}

for k in k_range:
    km = KMeans(n_clusters=k, random_state=42, n_init=10, max_iter=300)
    labels = km.fit_predict(X_scaled)
    score = silhouette_score(X_scaled, labels, sample_size=min(10000, len(X_scaled)))
    silhouette_scores[k] = score
    print(f"K={k}: silhouette={score:.4f}")

optimal_k = max(silhouette_scores, key=silhouette_scores.get)
print(f"\nOptimal K: {optimal_k} (silhouette={silhouette_scores[optimal_k]:.4f})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Fit Final Model and Profile Clusters

# COMMAND ----------

final_km = KMeans(n_clusters=optimal_k, random_state=42, n_init=20, max_iter=500)
features_pd["cluster_id"] = final_km.fit_predict(X_scaled)

cluster_profiles = features_pd.groupby("cluster_id")[feature_cols].mean().round(2)
cluster_sizes = features_pd["cluster_id"].value_counts().sort_index()

CLUSTER_LABELS = {
    0: "high_value_active",
    1: "growth_potential",
    2: "stable_mid_tier",
    3: "new_low_engagement",
    4: "at_risk_declining",
    5: "dormant",
    6: "support_heavy",
    7: "premium_loyal",
    8: "occasional",
}

def label_cluster(row):
    """Assign a descriptive label based on cluster centroid characteristics."""
    profile = cluster_profiles.loc[row["cluster_id"]]
    if profile.get("health_score", 0) >= 65 and profile.get("txn_volume", 0) > cluster_profiles["txn_volume"].median():
        return "high_value_active"
    elif profile.get("health_score", 0) < 25:
        return "at_risk_declining"
    elif profile.get("tenure_days", 0) < cluster_profiles["tenure_days"].median() * 0.5:
        return "new_low_engagement"
    elif profile.get("ticket_count", 0) > cluster_profiles["ticket_count"].quantile(0.75):
        return "support_heavy"
    elif profile.get("txn_volume", 0) > cluster_profiles["txn_volume"].quantile(0.75):
        return "premium_loyal"
    elif profile.get("health_score", 0) >= 45:
        return "stable_mid_tier"
    elif profile.get("days_since_last_txn", 0) > cluster_profiles["days_since_last_txn"].quantile(0.75):
        return "dormant"
    else:
        return "growth_potential"

features_pd["behavioral_segment"] = features_pd.apply(label_cluster, axis=1)

print("\nCluster Profiles:")
for cid in sorted(features_pd["cluster_id"].unique()):
    subset = features_pd[features_pd["cluster_id"] == cid]
    label = subset["behavioral_segment"].iloc[0]
    print(f"\n  Cluster {cid} ({label}) - {len(subset):,} merchants")
    print(f"    Avg health: {subset['health_score'].mean():.1f}")
    print(f"    Avg volume: {subset['txn_volume'].mean():,.0f}")
    print(f"    Avg recency: {subset['days_since_last_txn'].mean():.0f} days")
    print(f"    Avg tickets: {subset['ticket_count'].mean():.1f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Save to Gold Table

# COMMAND ----------

result_spark = spark.createDataFrame(
    features_pd[["golden_id", "cluster_id", "behavioral_segment"] + feature_cols]
)
result_spark = (
    result_spark
    .withColumn("_model_version", F.lit(f"kmeans_k{optimal_k}_v1"))
    .withColumn("_scored_at", F.current_timestamp())
)

result_spark.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_behavioral_segments")
print(f"Saved {result_spark.count():,} rows to {catalog}.{schema}.gold_behavioral_segments")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Log to MLflow

# COMMAND ----------

with mlflow.start_run(run_name="behavioral_segmentation_kmeans"):
    mlflow.log_params({
        "model_type": "KMeans",
        "optimal_k": optimal_k,
        "features": str(feature_cols),
        "merchant_count": len(features_pd),
    })
    mlflow.log_metrics({
        "silhouette_score": silhouette_scores[optimal_k],
        "n_clusters": optimal_k,
    })
    for cid in sorted(features_pd["cluster_id"].unique()):
        subset = features_pd[features_pd["cluster_id"] == cid]
        label = subset["behavioral_segment"].iloc[0]
        mlflow.log_metric(f"cluster_{cid}_size", len(subset))
        mlflow.log_metric(f"cluster_{cid}_avg_health", float(subset["health_score"].mean()))
    mlflow.sklearn.log_model(final_km, "kmeans_model")
    mlflow.sklearn.log_model(scaler, "scaler")

print("Behavioral segmentation complete.")
