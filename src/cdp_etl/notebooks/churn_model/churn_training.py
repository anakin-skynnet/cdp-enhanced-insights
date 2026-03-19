# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Merchant Churn Model Training
# MAGIC
# MAGIC Trains XGBoost churn prediction model on gold_customer_360 + gold_engagement_metrics.
# MAGIC Registers model to MLflow for Model Serving deployment.

# COMMAND ----------

# MAGIC %pip install xgboost mlflow scikit-learn

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_360"
model_name = "getnet_merchant_churn"
CHURN_THRESHOLD_DAYS = 90

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Training Data

# COMMAND ----------

from pyspark.sql import functions as F

engagement = spark.table(f"{catalog}.{schema}.gold_engagement_metrics")
customers = spark.table(f"{catalog}.{schema}.gold_customer_360")
segments = spark.table(f"{catalog}.{schema}.gold_segments")

training_df = (
    customers
    .join(engagement, "golden_id", "inner")
    .join(segments.select("golden_id", "r_score", "f_score", "m_score", "segment"), "golden_id", "left")
    .withColumn("churn_label", F.when(F.col("days_since_last_txn") > CHURN_THRESHOLD_DAYS, 1).otherwise(0))
)

display(training_df.groupBy("churn_label").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Engineering

# COMMAND ----------

feature_cols = [
    "txn_count",
    "txn_volume",
    "ticket_count",
    "days_since_last_txn",
    "tenure_days",
    "r_score",
    "f_score",
    "m_score",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train with MLflow

# COMMAND ----------

import pandas as pd
import mlflow
import mlflow.sklearn
import numpy as np
from sklearn.model_selection import train_test_split, StratifiedKFold, cross_val_score
from sklearn.metrics import roc_auc_score, f1_score, precision_score, recall_score, classification_report
import xgboost as xgb

pdf = training_df.select("golden_id", "churn_label", *feature_cols).toPandas()
for c in feature_cols:
    if c not in pdf.columns:
        pdf[c] = 0

X = pdf[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
y = pdf["churn_label"].astype(int)

churn_rate = y.mean()
scale_pos = (1 - churn_rate) / max(churn_rate, 0.01)

stratify_param = y if y.sum() >= 5 and (y == 0).sum() >= 5 else None
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=stratify_param,
)

mlflow.set_experiment("/Shared/getnet_cdp_churn")

with mlflow.start_run(run_name="xgb_churn_optimized"):
    params = {
        "n_estimators": 200,
        "max_depth": 5,
        "learning_rate": 0.08,
        "subsample": 0.8,
        "colsample_bytree": 0.8,
        "scale_pos_weight": round(scale_pos, 2),
        "random_state": 42,
        "eval_metric": "auc",
    }
    mlflow.log_params(params)
    mlflow.log_param("churn_threshold_days", CHURN_THRESHOLD_DAYS)
    mlflow.log_param("feature_count", len(feature_cols))
    mlflow.log_param("features", ",".join(feature_cols))
    mlflow.log_param("train_rows", len(X_train))
    mlflow.log_param("test_rows", len(X_test))
    mlflow.log_param("churn_rate", round(churn_rate, 4))
    mlflow.log_param("catalog", catalog)
    mlflow.log_param("schema", schema)

    model = xgb.XGBClassifier(**params)

    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc")
    mlflow.log_metric("cv_auc_mean", round(np.mean(cv_scores), 4))
    mlflow.log_metric("cv_auc_std", round(np.std(cv_scores), 4))

    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    preds_proba = model.predict_proba(X_test)[:, 1]
    preds = model.predict(X_test)
    auc = roc_auc_score(y_test, preds_proba)
    f1 = f1_score(y_test, preds)
    precision = precision_score(y_test, preds)
    recall = recall_score(y_test, preds)

    mlflow.log_metric("roc_auc", round(auc, 4))
    mlflow.log_metric("f1_score", round(f1, 4))
    mlflow.log_metric("precision", round(precision, 4))
    mlflow.log_metric("recall", round(recall, 4))

    importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
    for feat, imp in importance.items():
        mlflow.log_metric(f"fi_{feat}", round(imp, 4))

    mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)

    print(f"AUC: {auc:.4f} | F1: {f1:.4f} | CV AUC: {np.mean(cv_scores):.4f} +/- {np.std(cv_scores):.4f}")
    print(f"Feature importance: {importance}")
    print(classification_report(y_test, preds, target_names=["active", "churned"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Score All Merchants and Persist

# COMMAND ----------

pdf["churn_probability"] = model.predict_proba(X[feature_cols].fillna(0))[:, 1]
scored_df = spark.createDataFrame(pdf[["golden_id", "churn_probability"]])
scored_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_churn_scores")
print(f"Scored {scored_df.count()} merchants -> {catalog}.{schema}.gold_churn_scores")
