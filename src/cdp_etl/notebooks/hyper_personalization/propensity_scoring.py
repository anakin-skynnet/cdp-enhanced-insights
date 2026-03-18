# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Hyper-Personalization: Propensity Scoring
# MAGIC
# MAGIC Trains multi-output propensity models (upsell, churn, activation) using XGBoost
# MAGIC on the gold feature set. Writes propensity scores to `gold_propensity_scores`
# MAGIC for real-time personalization signals.
# MAGIC
# MAGIC Inspired by: [Databricks Customer Segmentation for Personalization](https://www.databricks.com/solutions/accelerators/customer-segmentation-sv)

# COMMAND ----------

# MAGIC %pip install xgboost mlflow scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Build Feature Matrix

# COMMAND ----------

from pyspark.sql import functions as F

features_df = spark.sql(f"""
  SELECT
    e.golden_id,
    COALESCE(e.txn_count, 0) AS txn_count,
    COALESCE(e.txn_volume, 0) AS txn_volume,
    COALESCE(e.ticket_count, 0) AS ticket_count,
    COALESCE(e.days_since_last_txn, 0) AS days_since_last_txn,
    DATEDIFF(e.last_txn_date, e.first_txn_date) AS tenure_days,
    COALESCE(h.health_score, 50) AS health_score,
    COALESCE(s.r_score, 3) AS r_score,
    COALESCE(s.f_score, 3) AS f_score,
    COALESCE(s.m_score, 3) AS m_score,
    -- Labels derived from behavioral signals
    CASE WHEN e.days_since_last_txn > 90 THEN 1 ELSE 0 END AS label_churn,
    CASE
      WHEN s.segment IN ('champions', 'loyal') AND e.txn_volume > 50000 THEN 1
      ELSE 0
    END AS label_upsell,
    CASE
      WHEN s.segment IN ('new_customers', 'promising') AND e.txn_count < 5 THEN 1
      ELSE 0
    END AS label_activation
  FROM {catalog}.{schema}.gold_engagement_metrics e
  LEFT JOIN {catalog}.{schema}.gold_health_score h ON e.golden_id = h.golden_id
  LEFT JOIN {catalog}.{schema}.gold_segments s ON e.golden_id = s.golden_id
  WHERE e.txn_count > 0
""")

display(features_df.describe())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Train Propensity Models

# COMMAND ----------

import mlflow
import mlflow.sklearn
import xgboost as xgb
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score, f1_score
from sklearn.preprocessing import StandardScaler

pdf = features_df.toPandas()

feature_cols = [
    "txn_count", "txn_volume", "ticket_count", "days_since_last_txn",
    "tenure_days", "health_score", "r_score", "f_score", "m_score",
]

X = pdf[feature_cols].fillna(0)
scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

mlflow.set_experiment("/Shared/getnet_cdp_propensity")

propensity_results = {}
for label_col, model_name in [
    ("label_churn", "churn_propensity"),
    ("label_upsell", "upsell_propensity"),
    ("label_activation", "activation_propensity"),
]:
    y = pdf[label_col].fillna(0)
    stratify_param = y if y.sum() > 5 and (y == 0).sum() > 5 else None
    X_train, X_test, y_train, y_test = train_test_split(
        X_scaled, y, test_size=0.2, random_state=42, stratify=stratify_param
    )

    with mlflow.start_run(run_name=model_name):
        model = xgb.XGBClassifier(
            n_estimators=150, max_depth=5, learning_rate=0.08,
            random_state=42, eval_metric="logloss",
            scale_pos_weight=max(1, (y == 0).sum() / max((y == 1).sum(), 1)),
        )
        cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
        cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc")
        mlflow.log_metric("cv_auc_mean", round(float(np.mean(cv_scores)), 4))
        mlflow.log_metric("cv_auc_std", round(float(np.std(cv_scores)), 4))
        model.fit(X_train, y_train)
        preds_proba = model.predict_proba(X_test)[:, 1]

        auc = roc_auc_score(y_test, preds_proba) if len(np.unique(y_test)) > 1 else 0.0
        f1 = f1_score(y_test, (preds_proba > 0.5).astype(int), zero_division=0)

        mlflow.log_param("features", feature_cols)
        mlflow.log_metric("roc_auc", auc)
        mlflow.log_metric("f1_score", f1)
        mlflow.log_metric("positive_rate", float(y.mean()))
        mlflow.sklearn.log_model(scaler, "scaler")
        mlflow.sklearn.log_model(model, "model", registered_model_name=f"getnet_{model_name}")

        pdf[f"{model_name}_score"] = model.predict_proba(X_scaled)[:, 1]
        propensity_results[model_name] = {"auc": auc, "f1": f1}
        print(f"{model_name}: AUC={auc:.4f}, F1={f1:.4f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Write Propensity Scores to Gold Table

# COMMAND ----------

result_cols = ["golden_id", "churn_propensity_score", "upsell_propensity_score", "activation_propensity_score"]
result_pdf = pdf[result_cols].copy()

result_pdf["propensity_tier"] = result_pdf.apply(
    lambda row: (
        "high_churn_risk" if row["churn_propensity_score"] > 0.7
        else "upsell_ready" if row["upsell_propensity_score"] > 0.6
        else "activation_candidate" if row["activation_propensity_score"] > 0.6
        else "stable"
    ),
    axis=1,
)

result_spark = spark.createDataFrame(result_pdf)
result_spark.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_propensity_scores")

print(f"Wrote {result_spark.count()} propensity scores to gold_propensity_scores")
display(result_spark.groupBy("propensity_tier").count().orderBy("count", ascending=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Summary

# COMMAND ----------

for name, metrics in propensity_results.items():
    print(f"  {name}: AUC={metrics['auc']:.4f}, F1={metrics['f1']:.4f}")
