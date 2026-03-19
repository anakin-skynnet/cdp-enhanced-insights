# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Merchant Churn Model Training
# MAGIC
# MAGIC Trains XGBoost churn prediction model on gold_customer_360 + gold_engagement_metrics.
# MAGIC Registers model to MLflow for Model Serving deployment.

# COMMAND ----------

# MAGIC %pip install xgboost mlflow scikit-learn
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)
        return v if v else default
    except Exception:
        return default

catalog = _widget("catalog", "ahs_demos_catalog")
schema = _widget("schema", "cdp_360")
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
from mlflow.models import infer_signature
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
n_classes = y.nunique()
has_both_classes = n_classes >= 2

if not has_both_classes:
    print(f"WARNING: only {n_classes} class(es) found (churn_rate={churn_rate:.4f}). "
          "Injecting synthetic minority samples so the model can learn both classes.")
    minority_label = 0 if churn_rate == 1.0 else 1
    n_synthetic = max(int(len(X) * 0.15), 10)
    synthetic_X = X.sample(n=n_synthetic, replace=True, random_state=42).copy()
    for c in synthetic_X.columns:
        noise = np.random.RandomState(42).normal(0, synthetic_X[c].std() * 0.3, n_synthetic)
        synthetic_X[c] = synthetic_X[c] + noise
    synthetic_y = pd.Series([minority_label] * n_synthetic)
    X = pd.concat([X, synthetic_X], ignore_index=True)
    y = pd.concat([y, synthetic_y], ignore_index=True)
    churn_rate = y.mean()
    print(f"  Added {n_synthetic} synthetic class-{minority_label} rows. New churn_rate={churn_rate:.4f}")

scale_pos = (1 - churn_rate) / max(churn_rate, 0.01)

stratify_param = y if y.value_counts().min() >= 5 else None
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
    mlflow.log_param("synthetic_minority_injected", not has_both_classes)

    model = xgb.XGBClassifier(**params)

    if y_train.nunique() >= 2:
        cv = StratifiedKFold(n_splits=min(5, y_train.value_counts().min()), shuffle=True, random_state=42)
        cv_scores = cross_val_score(model, X_train, y_train, cv=cv, scoring="roc_auc")
        mlflow.log_metric("cv_auc_mean", round(np.mean(cv_scores), 4))
        mlflow.log_metric("cv_auc_std", round(np.std(cv_scores), 4))
    else:
        cv_scores = np.array([0.0])
        mlflow.log_metric("cv_auc_mean", 0.0)
        mlflow.log_metric("cv_auc_std", 0.0)

    model.fit(X_train, y_train, eval_set=[(X_test, y_test)], verbose=False)

    preds_proba = model.predict_proba(X_test)[:, 1]
    preds = model.predict(X_test)

    if y_test.nunique() >= 2:
        auc = roc_auc_score(y_test, preds_proba)
        f1 = f1_score(y_test, preds, zero_division=0)
        precision = precision_score(y_test, preds, zero_division=0)
        recall = recall_score(y_test, preds, zero_division=0)
    else:
        auc = f1 = precision = recall = 0.0
        print("WARNING: test set has only one class — AUC/F1 metrics set to 0.")

    mlflow.log_metric("roc_auc", round(auc, 4))
    mlflow.log_metric("f1_score", round(f1, 4))
    mlflow.log_metric("precision", round(precision, 4))
    mlflow.log_metric("recall", round(recall, 4))

    importance = dict(zip(feature_cols, model.feature_importances_.tolist()))
    for feat, imp in importance.items():
        mlflow.log_metric(f"fi_{feat}", round(imp, 4))

    signature = infer_signature(X_train, model.predict(X_train))
    mlflow.sklearn.log_model(model, "model", signature=signature, registered_model_name=model_name)

    print(f"AUC: {auc:.4f} | F1: {f1:.4f} | CV AUC: {np.mean(cv_scores):.4f} +/- {np.std(cv_scores):.4f}")
    print(f"Feature importance: {importance}")
    present_labels = sorted(y_test.unique())
    label_names = {0: "active", 1: "churned"}
    print(classification_report(
        y_test, preds,
        labels=present_labels,
        target_names=[label_names[l] for l in present_labels],
        zero_division=0,
    ))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Score All Merchants and Persist

# COMMAND ----------

original_X = pdf[feature_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
pdf["churn_probability"] = model.predict_proba(original_X)[:, 1]
scored_df = spark.createDataFrame(pdf[["golden_id", "churn_probability"]])
scored_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_churn_scores")
print(f"Scored {scored_df.count()} merchants -> {catalog}.{schema}.gold_churn_scores")
