# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Merchant Churn Model Training
# MAGIC
# MAGIC Trains XGBoost churn prediction model on gold_customer_360 + gold_engagement_metrics.
# MAGIC Registers model to MLflow for Model Serving deployment.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "main"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_dev"
model_name = "getnet_merchant_churn"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Training Data

# COMMAND ----------

from pyspark.sql import functions as F

# Join golden record with engagement metrics
engagement = spark.table(f"{catalog}.{schema}.gold_engagement_metrics")
customers = spark.table(f"{catalog}.{schema}.gold_customer_360")

training_df = (
    customers.join(engagement, "golden_id", "inner")
    .withColumn("churn_label", F.when(F.col("days_since_last_txn") > 90, 1).otherwise(0))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Feature Engineering

# COMMAND ----------

# Select features for churn prediction (from gold_engagement_metrics)
feature_cols = [
    "txn_count",
    "txn_volume",
    "ticket_count",
    "days_since_last_txn",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Train with MLflow

# COMMAND ----------

import mlflow
import mlflow.sklearn
from sklearn.model_selection import train_test_split
from sklearn.metrics import roc_auc_score, classification_report
import xgboost as xgb

# Convert to pandas for XGBoost (or use Spark ML for scale)
pdf = training_df.select("golden_id", "churn_label", *[c for c in feature_cols if c in training_df.columns]).toPandas()

# Handle missing columns
for c in feature_cols:
    if c not in pdf.columns:
        pdf[c] = 0

X = pdf[[c for c in feature_cols if c in pdf.columns]].fillna(0)
y = pdf["churn_label"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment("/Shared/getnet_cdp_churn")

with mlflow.start_run():
    model = xgb.XGBClassifier(
        n_estimators=100,
        max_depth=6,
        learning_rate=0.1,
        random_state=42,
    )
    model.fit(X_train, y_train)

    preds = model.predict_proba(X_test)[:, 1]
    auc = roc_auc_score(y_test, preds)

    mlflow.log_metric("roc_auc", auc)
    mlflow.sklearn.log_model(model, "model", registered_model_name=model_name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Register for Model Serving

# COMMAND ----------

# Model is registered. Deploy via:
# - Databricks UI: Serving > Create endpoint
# - Or: databricks model-serving create-endpoint
# Use: mlflow.sklearn.load_model("models:/getnet_merchant_churn/latest")
