# Databricks notebook source
# MAGIC %md
# MAGIC # Bank Payment Platform CDP - Deploy AI Agents
# MAGIC
# MAGIC Logs and deploys **four** agents to Databricks Model Serving:
# MAGIC 1. **CDP Supervisor** (primary entry point - orchestrates all specialists + Genie)
# MAGIC 2. Churn Prevention specialist
# MAGIC 3. Segment Campaign specialist
# MAGIC 4. Next Best Action specialist

# COMMAND ----------

import mlflow
from mlflow.models.resources import DatabricksServingEndpoint, DatabricksFunction, DatabricksGenieSpace

mlflow.set_registry_uri("databricks-uc")

def _widget(name, default):
    try:
        v = dbutils.widgets.get(name)
        return v if v else default
    except Exception:
        return default

catalog = _widget("catalog", "ahs_demos_catalog")
schema = _widget("schema", "cdp_360")
UC_PREFIX = f"{catalog}.{schema}"

LLM_SUPERVISOR = "databricks-claude-sonnet-4-6"
LLM_SPECIALIST = "databricks-gpt-5-4-mini"

def _ucf(name: str) -> DatabricksFunction:
    return DatabricksFunction(function_name=f"{UC_PREFIX}.{name}")

specialist_resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_SPECIALIST),
]

supervisor_base_resources = [
    DatabricksServingEndpoint(endpoint_name=LLM_SUPERVISOR),
    DatabricksServingEndpoint(endpoint_name=LLM_SPECIALIST),
]

uc_functions_churn = [
    _ucf("get_churn_kpis"),
    _ucf("get_at_risk_merchants"),
    _ucf("lookup_merchant"),
    _ucf("get_segment_summary"),
]

uc_functions_campaign = [
    _ucf("get_segment_summary"),
    _ucf("get_segment_merchants"),
    _ucf("lookup_merchant"),
    _ucf("get_churn_kpis"),
]

uc_functions_nba = [
    _ucf("get_next_best_actions"),
    _ucf("get_nba_summary"),
    _ucf("get_health_scores"),
    _ucf("lookup_merchant"),
    _ucf("get_action_history"),
    _ucf("get_action_log_summary"),
]

ALL_UC_FUNCTIONS = [
    _ucf("lookup_merchant"),
    _ucf("get_at_risk_merchants"),
    _ucf("get_segment_summary"),
    _ucf("get_segment_merchants"),
    _ucf("get_next_best_actions"),
    _ucf("get_nba_summary"),
    _ucf("get_health_scores"),
    _ucf("get_action_history"),
    _ucf("get_action_log_summary"),
    _ucf("get_churn_kpis"),
    _ucf("get_clv_rankings"),
    _ucf("get_clv_summary"),
    _ucf("get_channel_attribution"),
    _ucf("get_behavioral_segments"),
    _ucf("get_support_analytics"),
    _ucf("get_support_kpis"),
    _ucf("get_call_center_agents"),
    _ucf("get_call_center_sentiment"),
    _ucf("get_personalization_signals"),
    _ucf("get_ad_creative"),
    _ucf("get_campaign_roi"),
    _ucf("get_audience"),
    _ucf("get_anomaly_alerts"),
    _ucf("get_merchant_timeline"),
]

pip_requirements = [
    "mlflow",
    "databricks-langchain",
    "langgraph",
    "psycopg[binary]>=3.0",
]

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Log Churn Prevention Agent

# COMMAND ----------

with mlflow.start_run(run_name="churn_prevention_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="churn_prevention_agent",
        python_model="churn_prevention/agent.py",
        resources=specialist_resources + uc_functions_churn,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Show me the top at-risk merchants by revenue"}]
        },
        registered_model_name=f"{UC_PREFIX}.churn_prevention_agent",
    )
    print(f"Churn agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Log Segment Campaign Agent

# COMMAND ----------

with mlflow.start_run(run_name="segment_campaign_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="segment_campaign_agent",
        python_model="segment_campaign/agent.py",
        resources=specialist_resources + uc_functions_campaign,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Design a campaign to re-engage hibernating merchants"}]
        },
        registered_model_name=f"{UC_PREFIX}.segment_campaign_agent",
    )
    print(f"Campaign agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Log Next Best Action Agent

# COMMAND ----------

with mlflow.start_run(run_name="next_best_action_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="next_best_action_agent",
        python_model="next_best_action/agent.py",
        resources=specialist_resources + uc_functions_nba,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "What are the top priority actions for this week?"}]
        },
        registered_model_name=f"{UC_PREFIX}.next_best_action_agent",
    )
    print(f"NBA agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Log CDP Supervisor Agent (Multi-Agent Orchestrator)

# COMMAND ----------

try:
    genie_space_id = spark.conf.get("spark.cdp.genie_space_id")
except Exception:
    genie_space_id = ""

from mlflow.models.resources import DatabricksLakebase

supervisor_resources = supervisor_base_resources + ALL_UC_FUNCTIONS
supervisor_resources.append(DatabricksLakebase(database_instance_name="cdp-360-ops"))
if genie_space_id:
    supervisor_resources.append(DatabricksGenieSpace(genie_space_id=genie_space_id))

with mlflow.start_run(run_name="cdp_supervisor_agent"):
    model_info = mlflow.pyfunc.log_model(
        name="cdp_supervisor_agent",
        python_model="cdp_supervisor/agent.py",
        resources=supervisor_resources,
        pip_requirements=pip_requirements,
        input_example={
            "input": [{"role": "user", "content": "Give me a complete overview of our merchant health and top priorities this week"}]
        },
        registered_model_name=f"{UC_PREFIX}.cdp_supervisor_agent",
    )
    print(f"Supervisor agent logged: {model_info.model_uri}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Deploy All Agents to Model Serving

# COMMAND ----------

from databricks import agents
from mlflow import MlflowClient

client = MlflowClient()

def _latest_version(model_name: str) -> str:
    versions = client.search_model_versions(f"name='{model_name}'")
    if not versions:
        return "1"
    return str(max(int(v.version) for v in versions))

_AGENT_MODELS = [
    f"{UC_PREFIX}.cdp_supervisor_agent",
    f"{UC_PREFIX}.churn_prevention_agent",
    f"{UC_PREFIX}.segment_campaign_agent",
    f"{UC_PREFIX}.next_best_action_agent",
]

for model_name in _AGENT_MODELS:
    ver = _latest_version(model_name)
    print(f"Deploying {model_name} v{ver}")
    agents.deploy(model_name, ver)

print("All four agents deployed. Supervisor is the primary entry point.")
