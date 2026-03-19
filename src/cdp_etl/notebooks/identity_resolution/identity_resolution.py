# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet - Customer Entity Resolution
# MAGIC
# MAGIC Produces `gold_customer_360` (golden record) and `gold_identity_graph` (source_id → golden_id mappings).
# MAGIC Based on Databricks Customer Entity Resolution Solution Accelerator patterns.
# MAGIC
# MAGIC **Input:** silver_contacts, silver_accounts, silver_tickets, silver_transactions
# MAGIC **Output:** gold_customer_360, gold_identity_graph

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Configuration

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if dbutils.widgets.get("catalog") else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if dbutils.widgets.get("schema") else "cdp_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Collect Source Records for Identity Resolution

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Union contacts and accounts into a single identity source
# Use email, phone as blocking keys for matching

contacts_df = spark.table(f"{catalog}.{schema}.silver_contacts").select(
    F.col("external_id").alias("source_id"),
    F.lit("salesforce_contact").alias("source_type"),
    F.col("email"),
    F.col("phone_normalized").alias("phone"),
    F.col("first_name"),
    F.col("last_name"),
    F.col("account_id"),
    F.col("created_at"),
    F.lit(None).cast("string").alias("account_name"),
    F.lit(None).cast("string").alias("industry"),
    F.lit(None).cast("string").alias("billing_country"),
    F.lit(None).cast("string").alias("billing_city"),
)

_sa = spark.table(f"{catalog}.{schema}.silver_accounts")
accounts_df = _sa.select(
    _sa["external_id"].alias("source_id"),
    F.lit("salesforce_account").alias("source_type"),
    F.lit(None).cast("string").alias("email"),
    _sa["phone_normalized"].alias("phone"),
    _sa["account_name"].alias("first_name"),
    F.lit(None).cast("string").alias("last_name"),
    _sa["external_id"].alias("account_id"),
    _sa["created_at"],
    _sa["account_name"],
    _sa["industry"],
    _sa["billing_country"],
    _sa["billing_city"],
)

# Union and add hash for blocking
identity_sources = contacts_df.unionByName(accounts_df, allowMissingColumns=True)

# Blocking key: email or phone hash for candidate pair generation
identity_with_block = identity_sources.withColumn(
    "block_key",
    F.coalesce(
        F.when(F.col("email").isNotNull() & (F.col("email") != ""), F.sha2(F.lower(F.trim("email")), 256)),
        F.when(F.col("phone").isNotNull() & (F.length("phone") >= 8), F.sha2("phone", 256)),
        F.sha2(F.concat_ws("|", "source_id", "source_type"), 256),
    ),
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Assign Golden IDs (Simplified Rule-Based Matching)

# COMMAND ----------

# For MVP: assign golden_id = block_key (same block = same entity)
# Production: use Zingg or ML model for probabilistic matching

golden_assignments = (
    identity_with_block
    .withColumn("golden_id", F.col("block_key"))
    .select("source_id", "source_type", "golden_id", "email", "phone", "first_name", "last_name", "account_id", "created_at", "account_name", "industry", "billing_country", "billing_city")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Build Identity Graph (source_id → golden_id)

# COMMAND ----------

gold_identity_graph = golden_assignments.select(
    "source_id",
    "source_type",
    "golden_id",
    F.current_timestamp().alias("_resolved_at"),
)

gold_identity_graph.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_identity_graph")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Merge into Golden Record (gold_customer_360)

# COMMAND ----------

# Surviving attributes: take best non-null from each source per golden_id
# For MVP: one row per golden_id with coalesced attributes

window_spec = Window.partitionBy("golden_id").orderBy(F.desc("created_at"))

golden_record = (
    golden_assignments
    .withColumn("rn", F.row_number().over(window_spec))
    .filter(F.col("rn") == 1)
    .drop("rn")
    .select(
        F.col("golden_id"),
        F.col("email"),
        F.col("phone"),
        F.col("first_name"),
        F.col("last_name"),
        F.col("account_id"),
        F.coalesce(F.col("account_name"), F.concat_ws(" ", F.col("first_name"), F.col("last_name"))).alias("merchant_name"),
        F.col("industry"),
        F.col("billing_country").alias("country"),
        F.col("billing_city").alias("city"),
        F.col("source_id").alias("primary_source_id"),
        F.col("source_type").alias("primary_source_type"),
        F.col("created_at"),
        F.current_timestamp().alias("_updated_at"),
    )
)

golden_record.write.format("delta").mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_customer_360")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Summary

# COMMAND ----------

print(f"Golden records created: {golden_record.count()}")
print(f"Identity graph mappings: {gold_identity_graph.count()}")
