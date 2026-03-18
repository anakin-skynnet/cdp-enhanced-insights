# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet CDP - Customer Lifetime Value Model
# MAGIC
# MAGIC Implements the Databricks [Customer Lifetime Value Solution Accelerator](https://www.databricks.com/solutions/accelerators/customer-lifetime-value)
# MAGIC adapted for PagoNxt Getnet's merchant payment data.
# MAGIC
# MAGIC **Method**: BG/NBD model (purchase probability) + Gamma-Gamma model (monetary value)
# MAGIC from the `btyd` library, following the *Buy 'til You Die* (BTYD) framework.
# MAGIC
# MAGIC **Output**: `gold_customer_ltv` table with 12-month projected CLV per merchant.

# COMMAND ----------

# MAGIC %pip install btyd==0.1a1 mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import timedelta
from btyd.fitters.beta_geo_fitter import BetaGeoFitter
from btyd import GammaGammaFitter
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType, IntegerType
import mlflow

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "main"
schema = dbutils.widgets.get("schema") if "schema" in [w.name for w in dbutils.widgets.getAll()] else "cdp"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Prepare RFM summary from transaction data
# MAGIC
# MAGIC The BG/NBD model requires per-customer summary:
# MAGIC - **frequency**: number of repeat purchases (total purchases - 1)
# MAGIC - **recency**: time between first and last purchase (in days)
# MAGIC - **T**: customer age = time between first purchase and analysis date (in days)
# MAGIC - **monetary_value**: average transaction value (excluding first purchase)

# COMMAND ----------

txn_df = spark.table(f"{catalog}.{schema}.silver_transactions").filter(
    F.col("status").isin("approved", "APPROVED", "completed")
)

analysis_date = txn_df.agg(F.max("transaction_date")).collect()[0][0]

rfm_spark = (
    txn_df
    .groupBy("customer_external_id")
    .agg(
        F.count("*").alias("total_purchases"),
        F.min("transaction_date").alias("first_purchase"),
        F.max("transaction_date").alias("last_purchase"),
        F.avg("amount").alias("avg_amount"),
        F.sum("amount").alias("total_amount"),
    )
    .filter(F.col("total_purchases") > 1)
    .withColumn("frequency", F.col("total_purchases") - 1)
    .withColumn("recency", F.datediff(F.col("last_purchase"), F.col("first_purchase")))
    .withColumn("T", F.datediff(F.lit(analysis_date), F.col("first_purchase")))
    .withColumn("monetary_value", F.col("total_amount") / F.col("total_purchases"))
    .select(
        "customer_external_id",
        "frequency",
        "recency",
        "T",
        "monetary_value",
        "total_purchases",
        "total_amount",
    )
)

rfm_pd = rfm_spark.toPandas()
print(f"Merchants with repeat purchases: {len(rfm_pd):,}")
rfm_pd.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Fit BG/NBD Model (Purchase Probability)
# MAGIC
# MAGIC The BG/NBD model estimates:
# MAGIC - Probability that a merchant is still "alive" (active)
# MAGIC - Expected number of future transactions in a given period

# COMMAND ----------

bgf = BetaGeoFitter(penalizer_coef=0.001)
bgf.fit(rfm_pd["frequency"], rfm_pd["recency"], rfm_pd["T"])
print(bgf.summary)

# COMMAND ----------

PROJECTION_DAYS = 365

rfm_pd["p_alive"] = bgf.conditional_probability_alive(
    rfm_pd["frequency"], rfm_pd["recency"], rfm_pd["T"]
)

rfm_pd["predicted_purchases_12m"] = bgf.conditional_expected_number_of_purchases_up_to_time(
    PROJECTION_DAYS,
    rfm_pd["frequency"],
    rfm_pd["recency"],
    rfm_pd["T"],
)

print(f"Avg P(alive): {rfm_pd['p_alive'].mean():.3f}")
print(f"Avg predicted purchases (12m): {rfm_pd['predicted_purchases_12m'].mean():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Fit Gamma-Gamma Model (Monetary Value)
# MAGIC
# MAGIC The Gamma-Gamma model estimates the expected average transaction value per merchant.
# MAGIC Combined with BG/NBD predicted purchases, it gives projected CLV.

# COMMAND ----------

ggf = GammaGammaFitter(penalizer_coef=0.001)
ggf.fit(rfm_pd["frequency"], rfm_pd["monetary_value"])
print(ggf.summary)

# COMMAND ----------

rfm_pd["predicted_monetary"] = ggf.conditional_expected_average_profit(
    rfm_pd["frequency"], rfm_pd["monetary_value"]
)

rfm_pd["clv_12m"] = ggf.customer_lifetime_value(
    bgf,
    rfm_pd["frequency"],
    rfm_pd["recency"],
    rfm_pd["T"],
    rfm_pd["monetary_value"],
    time=12,  # months
    discount_rate=0.01,  # monthly discount rate
)

print(f"Avg CLV (12m): {rfm_pd['clv_12m'].mean():,.2f}")
print(f"Median CLV (12m): {rfm_pd['clv_12m'].median():,.2f}")
print(f"Top-10 CLV merchants account for: {rfm_pd.nlargest(10, 'clv_12m')['clv_12m'].sum():,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Assign CLV Tiers and Save to Gold Table

# COMMAND ----------

rfm_pd["clv_tier"] = pd.qcut(
    rfm_pd["clv_12m"], q=5, labels=["very_low", "low", "medium", "high", "very_high"]
).astype(str)

clv_spark = spark.createDataFrame(
    rfm_pd[[
        "customer_external_id", "frequency", "recency", "T",
        "monetary_value", "total_purchases", "total_amount",
        "p_alive", "predicted_purchases_12m", "predicted_monetary",
        "clv_12m", "clv_tier",
    ]]
)

identity_graph = spark.table(f"{catalog}.{schema}.gold_identity_graph")
clv_with_golden = (
    clv_spark
    .join(identity_graph, clv_spark.customer_external_id == identity_graph.source_id, "left")
    .withColumn("golden_id", F.coalesce(F.col("golden_id"), F.col("customer_external_id")))
    .withColumn("_model_version", F.lit("bgnbd_gg_v1"))
    .withColumn("_scored_at", F.current_timestamp())
    .select(
        "golden_id", "customer_external_id",
        "frequency", "recency", "T",
        "monetary_value", "total_purchases", "total_amount",
        "p_alive", "predicted_purchases_12m", "predicted_monetary",
        "clv_12m", "clv_tier",
        "_model_version", "_scored_at",
    )
)

clv_with_golden.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_customer_ltv")
print(f"Saved {clv_with_golden.count():,} rows to {catalog}.{schema}.gold_customer_ltv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Log Models to MLflow

# COMMAND ----------

with mlflow.start_run(run_name="clv_bgnbd_gammagamma"):
    mlflow.log_params({
        "model_type": "BG/NBD + Gamma-Gamma",
        "projection_months": 12,
        "discount_rate": 0.01,
        "bgf_penalizer": 0.001,
        "ggf_penalizer": 0.001,
        "merchant_count": len(rfm_pd),
    })
    mlflow.log_metrics({
        "avg_p_alive": float(rfm_pd["p_alive"].mean()),
        "avg_predicted_purchases_12m": float(rfm_pd["predicted_purchases_12m"].mean()),
        "avg_clv_12m": float(rfm_pd["clv_12m"].mean()),
        "median_clv_12m": float(rfm_pd["clv_12m"].median()),
        "total_projected_clv": float(rfm_pd["clv_12m"].sum()),
    })

print("CLV model training complete.")
