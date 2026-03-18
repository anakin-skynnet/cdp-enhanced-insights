# Databricks notebook source
# MAGIC %md
# MAGIC # PagoNxt Getnet CDP - Deploy AI Agents
# MAGIC
# MAGIC Logs and deploys **four** agents to Databricks Model Serving:
# MAGIC 1. **CDP Supervisor** (primary entry point - orchestrates all specialists + Genie)
# MAGIC 2. Churn Prevention specialist
# MAGIC 3. Segment Campaign specialist
# MAGIC 4. Next Best Action specialist

# COMMAND ----------

# MAGIC %pip install mlflow==3.6.0 databricks-langchain langgraph==0.3.4 databricks-agents pydantic
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction, DatabricksGenieSpace

mlflow.set_registry_uri("databricks-uc")

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

base_resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_ENDPOINT),
]

uc_functions_churn = [
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_churn_kpis"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_at_risk_merchants"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.lookup_merchant"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_segment_summary"),
]

uc_functions_campaign = [
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_segment_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_segment_merchants"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.lookup_merchant"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_churn_kpis"),
]

uc_functions_nba = [
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_next_best_actions"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_nba_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_health_scores"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.lookup_merchant"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_action_history"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_action_log_summary"),
]

ALL_UC_FUNCTIONS = [
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.lookup_merchant"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_at_risk_merchants"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_segment_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_segment_merchants"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_next_best_actions"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_nba_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_health_scores"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_action_history"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_action_log_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_churn_kpis"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_clv_rankings"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_clv_summary"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_channel_attribution"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_behavioral_segments"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_support_analytics"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_support_kpis"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_call_center_agents"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_call_center_sentiment"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_personalization_signals"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_ad_creative"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_campaign_roi"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_audience"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_anomaly_alerts"),
    DatabricksFunction(function_name="ahs_demos_catalog.cdp_360.get_merchant_timeline"),
]

pip_requirements = [
    "mlflow==3.6.0",
    "databricks-langchain",
    "langgraph==0.3.4",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Log Churn Prevention Agent

# COMMAND ----------

with mlflow.start_run(run_name="churn_prevention_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="churn_prevention_agent",
        python_model="agents/churn_prevention/agent.py",
        resources=base_resources + uc_functions_churn,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Show me the top at-risk merchants by revenue"}]
        },
        registered_model_name="ahs_demos_catalog.cdp_360.churn_prevention_agent",
    )
    print(f"Churn agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Log Segment Campaign Agent

# COMMAND ----------

with mlflow.start_run(run_name="segment_campaign_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="segment_campaign_agent",
        python_model="agents/segment_campaign/agent.py",
        resources=base_resources + uc_functions_campaign,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Design a campaign to re-engage hibernating merchants"}]
        },
        registered_model_name="ahs_demos_catalog.cdp_360.segment_campaign_agent",
    )
    print(f"Campaign agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log Next Best Action Agent

# COMMAND ----------

with mlflow.start_run(run_name="next_best_action_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="next_best_action_agent",
        python_model="agents/next_best_action/agent.py",
        resources=base_resources + uc_functions_nba,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "What are the top priority actions for this week?"}]
        },
        registered_model_name="ahs_demos_catalog.cdp_360.next_best_action_agent",
    )
    print(f"NBA agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log CDP Supervisor Agent (Multi-Agent Orchestrator)

# COMMAND ----------

genie_space_id = spark.conf.get("spark.cdp.genie_space_id", "")
supervisor_resources = base_resources + ALL_UC_FUNCTIONS
if genie_space_id:
    supervisor_resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))

with mlflow.start_run(run_name="cdp_supervisor_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="cdp_supervisor_agent",
        python_model="agents/cdp_supervisor/agent.py",
        resources=supervisor_resources,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Give me a complete overview of our merchant health and top priorities this week"}]
        },
        registered_model_name="ahs_demos_catalog.cdp_360.cdp_supervisor_agent",
    )
    print(f"Supervisor agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deploy All Agents to Model Serving

# COMMAND ----------

from databricks import agents

agents.deploy(
    "ahs_demos_catalog.cdp_360.cdp_supervisor_agent",
    version="1",
    tags={"use_case": "cdp_supervisor", "cdp": "getnet", "role": "primary"},
)

agents.deploy(
    "ahs_demos_catalog.cdp_360.churn_prevention_agent",
    version="1",
    tags={"use_case": "churn_prevention", "cdp": "getnet"},
)

agents.deploy(
    "ahs_demos_catalog.cdp_360.segment_campaign_agent",
    version="1",
    tags={"use_case": "segment_campaigns", "cdp": "getnet"},
)

agents.deploy(
    "ahs_demos_catalog.cdp_360.next_best_action_agent",
    version="1",
    tags={"use_case": "next_best_action", "cdp": "getnet"},
)

print("All four agents deployed. Supervisor is the primary entry point.")
