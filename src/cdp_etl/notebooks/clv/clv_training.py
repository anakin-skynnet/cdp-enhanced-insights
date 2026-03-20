# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform CDP - Customer Lifetime Value Model
# MAGIC
# MAGIC Implements the Databricks [Customer Lifetime Value Solution Accelerator](https://www.databricks.com/solutions/accelerators/customer-lifetime-value)
# MAGIC adapted for the Bank Payment Platform's merchant payment data.
# MAGIC
# MAGIC **Method**: BG/NBD model (purchase probability) + Gamma-Gamma model (monetary value)
# MAGIC implemented natively with scipy (no external BTYD dependency for serverless compatibility).
# MAGIC
# MAGIC **Output**: `gold_customer_ltv` table with 12-month projected CLV per merchant.

# COMMAND ----------

# MAGIC %pip install scipy mlflow --quiet

# COMMAND ----------

import numpy as np
import pandas as pd
from datetime import timedelta
from scipy.optimize import minimize
from scipy.special import gammaln, betaln
import pyspark.sql.functions as F
import mlflow

print(f"scipy version: {__import__('scipy').__version__}")
print(f"numpy version: {np.__version__}")
print(f"mlflow version: {mlflow.__version__}")

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
# MAGIC ## Step 1: Prepare RFM summary from transaction data

# COMMAND ----------

txn_df = (
    spark.table(f"{catalog}.{schema}.silver_transactions")
    .filter(F.col("status").isin("approved", "APPROVED", "completed"))
    .withColumn("amount", F.col("amount").cast("double"))
)
print(f"Loaded transactions: {txn_df.count():,} rows")

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
for col in ["frequency", "recency", "T", "monetary_value", "total_amount"]:
    rfm_pd[col] = pd.to_numeric(rfm_pd[col], errors="coerce").astype(float)

rfm_pd["T"] = rfm_pd["T"].clip(lower=1)
rfm_pd["recency"] = rfm_pd["recency"].clip(lower=0)
rfm_pd["frequency"] = rfm_pd["frequency"].clip(lower=1)
rfm_pd["monetary_value"] = rfm_pd["monetary_value"].clip(lower=0.01)

print(f"Merchants with repeat purchases: {len(rfm_pd):,}")
rfm_pd.describe()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: BG/NBD Model (Purchase Probability)
# MAGIC
# MAGIC Native scipy implementation of the BG/NBD model for serverless compatibility.

# COMMAND ----------

def _bgnbd_ll(params, x, t_x, T):
    """Closed-form BG/NBD log-likelihood (Fader et al. 2005, Eq. 7)."""
    r, alpha, a, b = np.abs(params) + 1e-7
    ln_A = (
        gammaln(r + x) - gammaln(r)
        + r * np.log(alpha) - (r + x) * np.log(alpha + T)
        + betaln(a, b + x) - betaln(a, b)
    )
    delta = np.where(
        x > 0,
        np.log(a) - np.log(b + x - 1) + (r + x) * (np.log(alpha + T) - np.log(alpha + t_x)),
        0,
    )
    ll = np.where(
        x > 0,
        np.logaddexp(ln_A, ln_A + delta),
        ln_A,
    )
    return -np.sum(ll[np.isfinite(ll)])


def _bgnbd_p_alive(params, x, t_x, T):
    r, alpha, a, b = np.abs(params) + 1e-7
    A = (a / np.maximum(b + x - 1, 1e-7)) * ((alpha + T) / (alpha + t_x)) ** (r + x)
    A = np.where(np.isfinite(A), A, 0)
    return 1.0 / (1.0 + A)


def _bgnbd_expected(params, t, x, t_x, T):
    p = _bgnbd_p_alive(params, x, t_x, T)
    r, alpha, a, b = np.abs(params) + 1e-7
    ratio = (a + b + x - 1) / np.maximum(a - 1, 0.01)
    hyp = 1 - ((alpha + T) / (alpha + T + t)) ** (r + x)
    return np.where(a > 1, ratio * hyp * p, (x / np.maximum(T, 1)) * t * p)

freq_arr = rfm_pd["frequency"].values
rec_arr = rfm_pd["recency"].values
T_arr = rfm_pd["T"].values

result = minimize(
    _bgnbd_ll, x0=[0.5, 5.0, 0.5, 5.0],
    args=(freq_arr, rec_arr, T_arr),
    method="L-BFGS-B",
    bounds=[(1e-5, 100)] * 4,
    options={"maxiter": 200},
)
bgnbd_params = np.abs(result.x)
print(f"BG/NBD params (r, alpha, a, b): {bgnbd_params}")

PROJECTION_DAYS = 365
rfm_pd["p_alive"] = _bgnbd_p_alive(bgnbd_params, freq_arr, rec_arr, T_arr).clip(0, 1)
rfm_pd["predicted_purchases_12m"] = _bgnbd_expected(bgnbd_params, PROJECTION_DAYS, freq_arr, rec_arr, T_arr).clip(0)

print(f"Avg P(alive): {rfm_pd['p_alive'].mean():.3f}")
print(f"Avg predicted purchases (12m): {rfm_pd['predicted_purchases_12m'].mean():.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Gamma-Gamma Model (Monetary Value)

# COMMAND ----------

def _gg_log_likelihood(params, freq, monetary):
    p, q, v = params
    p, q, v = max(p, 1e-5), max(q, 1e-5), max(v, 1e-5)
    x = freq
    m = monetary
    ll = (
        gammaln(p * x + q) - gammaln(p * x) - gammaln(q)
        + q * np.log(v) + (p * x - 1) * np.log(m)
        + (p * x) * np.log(x) - (p * x + q) * np.log(x * m + v)
    )
    return -np.sum(ll)


def _gg_expected_profit(params, freq, monetary):
    p, q, v = params
    return (q - 1) / (p * freq + q - 1) * v + p * freq / (p * freq + q - 1) * monetary

monetary_arr = rfm_pd["monetary_value"].values

gg_result = minimize(
    _gg_log_likelihood, x0=[1.0, 1.0, 10.0],
    args=(freq_arr, monetary_arr),
    method="Nelder-Mead",
    options={"maxiter": 5000, "xatol": 1e-5, "fatol": 1e-5},
)
gg_params = np.abs(gg_result.x)
print(f"Gamma-Gamma params (p, q, v): {gg_params}")

rfm_pd["predicted_monetary"] = _gg_expected_profit(gg_params, freq_arr, monetary_arr)
rfm_pd["predicted_monetary"] = rfm_pd["predicted_monetary"].clip(lower=0.01)

MONTHLY_DISCOUNT = 0.01
rfm_pd["clv_12m"] = rfm_pd["predicted_monetary"] * rfm_pd["predicted_purchases_12m"]
discount_factor = sum(1 / (1 + MONTHLY_DISCOUNT) ** m for m in range(1, 13)) / 12
rfm_pd["clv_12m"] = rfm_pd["clv_12m"] * discount_factor

print(f"Avg CLV (12m): {rfm_pd['clv_12m'].mean():,.2f}")
print(f"Median CLV (12m): {rfm_pd['clv_12m'].median():,.2f}")
print(f"Top-10 CLV merchants account for: {rfm_pd.nlargest(10, 'clv_12m')['clv_12m'].sum():,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Assign CLV Tiers and Save to Gold Table

# COMMAND ----------

tier_labels = ["very_low", "low", "medium", "high", "very_high"]
try:
    rfm_pd["clv_tier"] = pd.qcut(
        rfm_pd["clv_12m"], q=5, labels=tier_labels, duplicates="drop",
    ).astype(str)
except ValueError:
    n_unique = rfm_pd["clv_12m"].nunique()
    actual_bins = min(n_unique, 5)
    if actual_bins < 2:
        rfm_pd["clv_tier"] = "medium"
    else:
        rfm_pd["clv_tier"] = pd.qcut(
            rfm_pd["clv_12m"], q=actual_bins,
            labels=tier_labels[:actual_bins], duplicates="drop",
        ).astype(str)
    print(f"Adjusted CLV tiers to {actual_bins} bins (data has {n_unique} unique CLV values)")

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

import json
import tempfile
import os

try:
    with mlflow.start_run(run_name="clv_bgnbd_gammagamma"):
        mlflow.log_params({
            "model_type": "BG/NBD + Gamma-Gamma (native scipy)",
            "projection_months": 12,
            "discount_rate": 0.01,
            "bgnbd_params": str(bgnbd_params.tolist()),
            "gg_params": str(gg_params.tolist()),
            "merchant_count": len(rfm_pd),
        })
        mlflow.log_metrics({
            "avg_p_alive": float(rfm_pd["p_alive"].mean()),
            "avg_predicted_purchases_12m": float(rfm_pd["predicted_purchases_12m"].mean()),
            "avg_clv_12m": float(rfm_pd["clv_12m"].mean()),
            "median_clv_12m": float(rfm_pd["clv_12m"].median()),
            "total_projected_clv": float(rfm_pd["clv_12m"].sum()),
        })
        params_payload = {
            "bgnbd": bgnbd_params.tolist(),
            "gamma_gamma": gg_params.tolist(),
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
            json.dump(params_payload, f)
            params_path = f.name
        mlflow.log_artifact(params_path, "models")
        os.unlink(params_path)
    print("MLflow logging complete.")
except Exception as e:
    print(f"MLflow logging failed (non-fatal): {e}")

print("CLV model training complete.")
