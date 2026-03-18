# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet CDP - Multi-Touch Attribution
# MAGIC
# MAGIC Implements the Databricks [Multi-Touch Attribution Solution Accelerator](https://www.databricks.com/solutions/accelerators/multi-touch-attribution)
# MAGIC adapted for PagoNxt Getnet's marketing touchpoint data.
# MAGIC
# MAGIC **Methods**:
# MAGIC - Heuristic: first-touch, last-touch, linear, time-decay
# MAGIC - Data-driven: Markov Chain transition probabilities
# MAGIC
# MAGIC **Output**: `gold_channel_attribution` table with per-channel credit allocation.

# COMMAND ----------

# MAGIC %pip install pymc3==3.11.5 mlflow
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd
import numpy as np
from itertools import combinations
from collections import defaultdict
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import mlflow

# COMMAND ----------

catalog = dbutils.widgets.get("catalog") if "catalog" in [w.name for w in dbutils.widgets.getAll()] else "ahs_demos_catalog"
schema = dbutils.widgets.get("schema") if "schema" in [w.name for w in dbutils.widgets.getAll()] else "cdp_360"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Build Touchpoint Journeys
# MAGIC
# MAGIC We construct merchant journeys from interactions across channels:
# MAGIC Salesforce campaigns, Zendesk tickets, Genesys calls, email opens, etc.
# MAGIC Each touchpoint has a channel and timestamp. A "conversion" is defined as
# MAGIC a transaction within 30 days of the last touchpoint.

# COMMAND ----------

interactions = spark.table(f"{catalog}.{schema}.silver_interactions")
tickets = spark.table(f"{catalog}.{schema}.silver_tickets")
transactions = spark.table(f"{catalog}.{schema}.silver_transactions").filter(
    F.col("status").isin("approved", "APPROVED", "completed")
)

touchpoints = (
    interactions
    .select(
        F.col("customer_external_id").alias("merchant_id"),
        F.col("conversation_date").alias("touchpoint_time"),
        F.coalesce(F.col("media_type"), F.col("queue_name"), F.lit("genesys")).alias("channel"),
        F.lit("impression").alias("interaction_type"),
    )
    .unionByName(
        tickets.select(
            F.col("customer_external_id").alias("merchant_id"),
            F.col("created_at").alias("touchpoint_time"),
            F.lit("support").alias("channel"),
            F.lit("impression").alias("interaction_type"),
        )
    )
)

conversions = (
    transactions
    .select(
        F.col("customer_external_id").alias("merchant_id"),
        F.col("transaction_date").alias("conversion_time"),
        F.col("amount").alias("conversion_value"),
    )
    .groupBy("merchant_id")
    .agg(
        F.min("conversion_time").alias("first_conversion"),
        F.count("*").alias("conversion_count"),
        F.sum("conversion_value").alias("total_conversion_value"),
    )
)

print(f"Touchpoints: {touchpoints.count():,}")
print(f"Merchants with conversions: {conversions.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Build Journey Paths
# MAGIC
# MAGIC For each merchant, create an ordered sequence of channels they touched
# MAGIC before their first conversion (or end of observation window).

# COMMAND ----------

w = Window.partitionBy("merchant_id").orderBy("touchpoint_time")

journeys = (
    touchpoints
    .withColumn("channel_order", F.row_number().over(w))
    .groupBy("merchant_id")
    .agg(
        F.collect_list(F.struct("touchpoint_time", "channel")).alias("touchpoint_list"),
        F.count("*").alias("path_length"),
    )
    .join(conversions, "merchant_id", "left")
    .withColumn("converted", F.when(F.col("first_conversion").isNotNull(), 1).otherwise(0))
)

journeys_pd = journeys.select(
    "merchant_id", "touchpoint_list", "path_length", "converted",
    "total_conversion_value",
).toPandas()

def extract_path(touchpoint_list):
    if touchpoint_list is None:
        return []
    sorted_tp = sorted(touchpoint_list, key=lambda x: str(x["touchpoint_time"]))
    return [tp["channel"] for tp in sorted_tp]

journeys_pd["path"] = journeys_pd["touchpoint_list"].apply(extract_path)
journeys_pd["path_str"] = journeys_pd["path"].apply(lambda p: " > ".join(p) if p else "direct")

print(f"Total journeys: {len(journeys_pd):,}")
print(f"Converted: {journeys_pd['converted'].sum():,}")
print(f"Conversion rate: {journeys_pd['converted'].mean():.2%}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Heuristic Attribution Models
# MAGIC
# MAGIC Calculate channel credit using first-touch, last-touch, linear, and time-decay.

# COMMAND ----------

channels = set()
for path in journeys_pd["path"]:
    channels.update(path)
channels = sorted(channels)

first_touch = defaultdict(float)
last_touch = defaultdict(float)
linear = defaultdict(float)
time_decay = defaultdict(float)

converted_journeys = journeys_pd[journeys_pd["converted"] == 1]

for _, row in converted_journeys.iterrows():
    path = row["path"]
    value = row["total_conversion_value"] or 1
    if not path:
        continue

    first_touch[path[0]] += value

    last_touch[path[-1]] += value

    credit_per = value / len(path)
    for ch in path:
        linear[ch] += credit_per

    total_weight = sum(2 ** i for i in range(len(path)))
    for i, ch in enumerate(path):
        weight = (2 ** i) / total_weight
        time_decay[ch] += value * weight

heuristic_results = []
for ch in channels:
    heuristic_results.append({
        "channel": ch,
        "first_touch_value": round(first_touch.get(ch, 0), 2),
        "last_touch_value": round(last_touch.get(ch, 0), 2),
        "linear_value": round(linear.get(ch, 0), 2),
        "time_decay_value": round(time_decay.get(ch, 0), 2),
    })

heuristic_df = pd.DataFrame(heuristic_results)
print("Heuristic Attribution:")
print(heuristic_df.to_string(index=False))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Markov Chain Attribution
# MAGIC
# MAGIC Build a transition probability matrix between channels, then calculate
# MAGIC removal effects to determine each channel's contribution.
# MAGIC
# MAGIC The Markov Chain approach:
# MAGIC 1. Build state transitions: start -> channel -> ... -> conversion/null
# MAGIC 2. Calculate baseline conversion probability
# MAGIC 3. For each channel, remove it and recalculate conversion probability
# MAGIC 4. Removal effect = baseline - probability_without_channel

# COMMAND ----------

def build_transitions(journeys_df):
    """Build transition counts from journey paths."""
    transitions = defaultdict(lambda: defaultdict(int))
    for _, row in journeys_df.iterrows():
        path = row["path"]
        converted = row["converted"]
        if not path:
            continue
        full_path = ["start"] + path + (["conversion"] if converted else ["null"])
        for i in range(len(full_path) - 1):
            transitions[full_path[i]][full_path[i + 1]] += 1
    return transitions


def transition_matrix(transitions):
    """Convert transition counts to probabilities."""
    matrix = {}
    for state, targets in transitions.items():
        total = sum(targets.values())
        matrix[state] = {t: c / total for t, c in targets.items()}
    return matrix


def calculate_conversion_probability(trans_matrix, max_steps=50):
    """Simulate conversion probability from start through the Markov chain."""
    states = {}
    states["start"] = 1.0
    for _ in range(max_steps):
        new_states = defaultdict(float)
        for state, prob in states.items():
            if state in ("conversion", "null"):
                new_states[state] += prob
                continue
            if state not in trans_matrix:
                new_states["null"] += prob
                continue
            for next_state, trans_prob in trans_matrix[state].items():
                new_states[next_state] += prob * trans_prob
        states = dict(new_states)
    return states.get("conversion", 0)


transitions = build_transitions(journeys_pd)
trans_matrix = transition_matrix(transitions)
baseline_conv = calculate_conversion_probability(trans_matrix)
print(f"Baseline conversion probability: {baseline_conv:.4f}")

removal_effects = {}
for channel in channels:
    modified = {
        s: {t: p for t, p in targets.items() if t != channel}
        for s, targets in trans_matrix.items()
        if s != channel
    }
    for s in modified:
        total = sum(modified[s].values())
        if total > 0:
            modified[s] = {t: p / total for t, p in modified[s].items()}

    conv_without = calculate_conversion_probability(modified)
    removal_effects[channel] = max(0, baseline_conv - conv_without)

total_effect = sum(removal_effects.values())
total_conversion_value = converted_journeys["total_conversion_value"].sum()

markov_attribution = {}
for ch, effect in removal_effects.items():
    share = effect / total_effect if total_effect > 0 else 0
    markov_attribution[ch] = {
        "removal_effect": round(effect, 6),
        "attribution_share": round(share, 4),
        "attributed_value": round(share * total_conversion_value, 2),
    }

print("\nMarkov Chain Attribution:")
for ch, vals in sorted(markov_attribution.items(), key=lambda x: -x[1]["attributed_value"]):
    print(f"  {ch}: share={vals['attribution_share']:.2%}, value={vals['attributed_value']:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Save Combined Attribution to Gold Table

# COMMAND ----------

combined = []
for ch in channels:
    row = {
        "channel": ch,
        "first_touch_value": first_touch.get(ch, 0),
        "last_touch_value": last_touch.get(ch, 0),
        "linear_value": linear.get(ch, 0),
        "time_decay_value": time_decay.get(ch, 0),
        "markov_removal_effect": markov_attribution.get(ch, {}).get("removal_effect", 0),
        "markov_attribution_share": markov_attribution.get(ch, {}).get("attribution_share", 0),
        "markov_attributed_value": markov_attribution.get(ch, {}).get("attributed_value", 0),
    }
    combined.append(row)

attribution_spark = spark.createDataFrame(pd.DataFrame(combined))
attribution_spark = attribution_spark.withColumn("_model_version", F.lit("markov_v1"))
attribution_spark = attribution_spark.withColumn("_scored_at", F.current_timestamp())
attribution_spark = attribution_spark.withColumn("baseline_conversion_rate", F.lit(baseline_conv))
attribution_spark = attribution_spark.withColumn("total_conversion_value", F.lit(float(total_conversion_value)))

attribution_spark.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.gold_channel_attribution")
print(f"Saved to {catalog}.{schema}.gold_channel_attribution")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Log to MLflow

# COMMAND ----------

with mlflow.start_run(run_name="multi_touch_attribution_markov"):
    mlflow.log_params({
        "model_type": "Markov Chain + Heuristics",
        "channels": str(channels),
        "journey_count": len(journeys_pd),
        "converted_count": int(journeys_pd["converted"].sum()),
    })
    mlflow.log_metrics({
        "baseline_conversion_rate": baseline_conv,
        "total_conversion_value": float(total_conversion_value),
    })
    for ch, vals in markov_attribution.items():
        safe_ch = ch.replace(" ", "_").lower()
        mlflow.log_metric(f"markov_share_{safe_ch}", vals["attribution_share"])

print("Multi-touch attribution complete.")
