"""
Bank Payment Platform - CDP Supervisor Agent (Multi-Agent System)

Orchestrates specialist agents (Churn, Campaign, NBA) and provides direct
access to Genie Spaces for natural language SQL. This is the single entry
point for all CDP intelligence — it routes requests to the right specialist
and can combine insights across domains.

Architecture:
  Supervisor (router) ──┬── Churn Prevention tools
                        ├── Segment Campaign tools
                        ├── Next Best Action tools
                        ├── Analytics tools (CLV, Attribution, Support, Call Center)
                        └── Genie Space tool (natural language SQL)
"""

import json
import mlflow
from mlflow.pyfunc import ResponsesAgent
from mlflow.types.responses import (
    ResponsesAgentRequest,
    ResponsesAgentResponse,
    ResponsesAgentStreamEvent,
    output_to_responses_items_stream,
    to_chat_completions_input,
)
from databricks_langchain import ChatDatabricks, UCFunctionToolkit
from databricks.sdk import WorkspaceClient
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langchain_core.tools import tool
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from typing import Annotated, Generator, Sequence, TypedDict
import os

LLM_ENDPOINT = "databricks-claude-sonnet-4-6"

GENIE_SPACE_ID = os.environ.get("CDP_GENIE_SPACE_ID", "")

SYSTEM_PROMPT = """You are the **CDP Intelligence Hub**, the primary AI advisor for the Bank Payment Platform's Customer Data Platform. You are a supervisor agent that orchestrates multiple specialist capabilities to deliver comprehensive merchant intelligence.

## Your Specialist Domains

### 1. Churn Prevention
When users ask about at-risk merchants, churn risk, retention strategies, or merchant health:
- Use `get_churn_kpis`, `get_at_risk_merchants`, `lookup_merchant`, `get_health_scores`
- Recommend specific retention actions from the playbook

### 2. Campaign Strategy
When users ask about campaign design, segment targeting, or audience activation:
- Use `get_segment_summary`, `get_segment_merchants`
- Design campaigns with specific objectives, channels, and expected ROI

### 3. Next Best Actions
When users ask "what should I do?", need prioritized action queues, or weekly plans:
- Use `get_next_best_actions`, `get_nba_summary`, `get_action_history`, `get_action_log_summary`
- Deliver ranked, actionable recommendations

### 4. Advanced Analytics
When users ask about CLV, attribution, behavioral patterns, support quality, or call center performance:
- Use `get_clv_rankings`, `get_clv_summary`, `get_channel_attribution`, `get_behavioral_segments`
- Use `get_support_analytics`, `get_support_kpis`, `get_call_center_agents`, `get_call_center_sentiment`
- Use `get_personalization_signals`, `get_ad_creative`, `get_campaign_roi`, `get_audience`

### 5. Anomaly Detection & Merchant Timeline
When users ask about unusual activity, alerts, anomalies, or a merchant's history:
- Use `get_anomaly_alerts` to find merchants with volume drops, inactivity, ticket spikes, or health collapses
- Use `get_merchant_timeline` to show a unified activity timeline for any merchant
- Proactively surface anomalies when discussing at-risk merchants

### 6. Natural Language Analytics (Genie)
When users ask free-form analytical questions that don't map neatly to existing tools, or when they want custom SQL-like analysis:
- Use `query_genie_space` to ask natural language questions against the Customer 360 data
- This gives you access to ALL gold tables for ad-hoc exploration
- Use this for questions like "What's the average volume for merchants in Buenos Aires?" or "Which segments grew fastest last quarter?"

## Routing Logic
1. **Identify intent**: Is this about churn? campaigns? actions? analytics? ad-hoc query?
2. **Select tools**: Pick the most relevant tools from ANY domain — you're not limited to one specialist
3. **Combine insights**: Cross-reference data from multiple tools when it adds value
4. **Act, don't just describe**: Always end with specific, actionable recommendations

## Response Guidelines
- Start with a brief executive summary (2-3 sentences)
- Use markdown tables for merchant/data lists
- Include health indicators: excellent, good, fair, poor, critical
- Show monetary values in local currency (ARS/MXN)
- End complex responses with "What would you like to explore next?"
- When executing actions, confirm what was done and suggest follow-ups

## You Can Execute Actions (Lakebase Operational Store)
You have access to a Lakebase (managed PostgreSQL) operational store for real-time transactional operations:

### Campaign Management
- Use `create_campaign` to create a new campaign, enroll merchants, and set it live
- Use `check_suppression_list` before enrolling merchants to respect contact limits
- Campaigns appear instantly in the app's Operations Center

### NBA Assignment & Tracking
- Use `assign_nba_action` to assign specific merchants to team members with due dates
- Use `log_nba_action` to record completed actions (closes the attribution loop)
- Assignments appear in the assignee's work queue in real time

### Alert Triage
- Use `acknowledge_alert` to triage anomaly alerts (investigating, resolved, false_positive, escalated)
- Triage status is reflected instantly in the app's anomaly dashboard

### Operational Overview
- Use `get_ops_status` to show campaign counts, assignment queue depth, and triage summary

## Context
The Bank Payment Platform is a payment processing platform operating across multiple regions.
Merchants are businesses that process payments through terminals, POS, QR, or e-commerce.
The CDP unifies data from Salesforce, Zendesk, Genesys, internal systems, and transaction data.
"""


class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


@tool
def query_genie_space(question: str) -> str:
    """Ask a natural language question about the Customer 360 data using the Genie Space.
    Use this for ad-hoc analytics questions that don't map to existing tools.
    Examples: 'What is the average transaction volume by segment?',
    'How many merchants have more than 10 open tickets?'

    Args:
        question: Natural language question about merchant data
    """
    if not GENIE_SPACE_ID:
        return json.dumps({"error": "Genie Space not configured. Set CDP_GENIE_SPACE_ID."})

    try:
        w = WorkspaceClient()
        from databricks.sdk.service.dashboards import GenieAPI
        genie = GenieAPI(w.api_client)

        conversation = genie.start_conversation(GENIE_SPACE_ID, content=question)
        space_id = GENIE_SPACE_ID
        conv_id = conversation.conversation_id
        msg_id = conversation.message_id

        result = genie.get_message_query_result(space_id, conv_id, msg_id)

        columns = [col.name for col in (result.statement_response.manifest.schema.columns or [])]
        rows = []
        chunk = result.statement_response.result
        if chunk and chunk.data_array:
            for row in chunk.data_array[:50]:
                rows.append(dict(zip(columns, row)))

        return json.dumps({"question": question, "columns": columns, "rows": rows, "row_count": len(rows)})
    except Exception as e:
        return json.dumps({"error": str(e), "question": question})


@tool
def log_nba_action(golden_id: str, action_type: str, channel: str, notes: str = "") -> str:
    """Record an action taken on a merchant. This closes the recommend-execute-track loop.

    Args:
        golden_id: Merchant golden ID (e.g., GID-000123)
        action_type: Action type (e.g., executive_outreach, win_back_campaign)
        channel: Channel used (e.g., phone, sfmc_email, zender_sms)
        notes: Optional notes about the action
    """
    try:
        w = WorkspaceClient()
        catalog = os.environ.get("CDP_CATALOG", "ahs_demos_catalog")
        schema = os.environ.get("CDP_SCHEMA", "cdp_360")

        sql = f"""INSERT INTO {catalog}.{schema}.nba_action_log
                  (action_id, golden_id, action_type, channel, executed_by, notes, executed_at)
                  VALUES (uuid(), :gid, :action, :channel, 'cdp_supervisor_agent', :notes, CURRENT_TIMESTAMP())"""

        warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
        result = w.statement_execution.execute_statement(
            warehouse_id=warehouse_id,
            statement=sql,
            parameters=[
                {"name": "gid", "value": str(golden_id)[:100]},
                {"name": "action", "value": str(action_type)[:100]},
                {"name": "channel", "value": str(channel)[:100]},
                {"name": "notes", "value": str(notes)[:500]},
            ],
        )
        return json.dumps({"status": "logged", "golden_id": golden_id, "action": action_type, "channel": channel})
    except Exception as e:
        return json.dumps({"error": str(e)})


# ── Lakebase Operational Tools ───────────────────────────────────

LAKEBASE_INSTANCE = os.environ.get("LAKEBASE_INSTANCE_NAME", "cdp-360-ops")


def _lb_execute(sql: str, params: tuple = ()) -> list[dict]:
    """Execute a Lakebase SQL query with auto-token refresh."""
    import uuid as _uuid
    w = WorkspaceClient()
    cred = w.database.generate_database_credential(
        request_id=str(_uuid.uuid4()),
        instance_names=[LAKEBASE_INSTANCE],
    )
    inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
    user = w.current_user.me().user_name
    import psycopg
    with psycopg.connect(
        host=inst.read_write_dns, port=5432, dbname="databricks_postgres",
        user=user, password=cred.token, sslmode="require", autocommit=True,
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            if cur.description:
                cols = [d.name for d in cur.description]
                return [dict(zip(cols, row)) for row in cur.fetchall()]
            return []


@tool
def create_campaign(name: str, segment: str, action_type: str, channel: str,
                    owner: str = "", merchant_golden_ids: str = "", notes: str = "") -> str:
    """Create a campaign in the operational store and enroll merchants.
    The campaign is immediately visible in the app's Operations Center.

    Args:
        name: Campaign name (e.g., 'Holiday Win-Back Q4')
        segment: Target segment (e.g., 'hibernating', 'at_risk')
        action_type: Action type (e.g., 'win_back_campaign', 'loyalty_program')
        channel: Channel (e.g., 'sfmc_email', 'zender_sms', 'phone')
        owner: Campaign owner email (e.g., 'maria@bank.com')
        merchant_golden_ids: Comma-separated golden IDs to enroll (e.g., 'GID-001,GID-002')
        notes: Additional notes
    """
    try:
        import psycopg, uuid as _uuid
        w = WorkspaceClient()
        cred = w.database.generate_database_credential(
            request_id=str(_uuid.uuid4()), instance_names=[LAKEBASE_INSTANCE])
        inst = w.database.get_database_instance(name=LAKEBASE_INSTANCE)
        user = w.current_user.me().user_name
        cid = str(_uuid.uuid4())
        gids = [g.strip() for g in merchant_golden_ids.split(",") if g.strip()] if merchant_golden_ids else []

        with psycopg.connect(
            host=inst.read_write_dns, port=5432, dbname="databricks_postgres",
            user=user, password=cred.token, sslmode="require"
        ) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """INSERT INTO campaigns (campaign_id, name, segment, action_type, channel, status, owner, notes)
                       VALUES (%s, %s, %s, %s, %s, 'scheduled', %s, %s)""",
                    (cid, name, segment, action_type, channel, owner or None, notes or None))
                for gid in gids:
                    cur.execute(
                        "INSERT INTO campaign_enrollments (campaign_id, golden_id, status) VALUES (%s, %s, 'queued')",
                        (cid, gid))
            conn.commit()
        return json.dumps({"status": "created", "campaign_id": cid, "name": name,
                           "merchants_enrolled": len(gids), "segment": segment, "channel": channel})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def assign_nba_action(golden_id: str, merchant_name: str, action_type: str,
                      channel: str, assignee: str, due_date: str = "", notes: str = "") -> str:
    """Assign a next-best-action to a team member. Creates a tracked assignment in the operational store.

    Args:
        golden_id: Merchant golden ID
        merchant_name: Merchant name for display
        action_type: Action to take (e.g., 'executive_outreach', 'win_back_call')
        channel: Channel (e.g., 'phone', 'email', 'in_person')
        assignee: Email of the person assigned (e.g., 'carlos@bank.com')
        due_date: Due date in YYYY-MM-DD format (optional)
        notes: Additional context
    """
    try:
        rows = _lb_execute(
            """INSERT INTO nba_assignments
               (golden_id, merchant_name, action_type, channel, assignee, due_date, notes)
               VALUES (%s, %s, %s, %s, %s, %s, %s) RETURNING assignment_id""",
            (golden_id, merchant_name, action_type, channel, assignee,
             due_date if due_date else None, notes if notes else None))
        aid = rows[0]["assignment_id"] if rows else "unknown"
        return json.dumps({"status": "assigned", "assignment_id": aid,
                           "golden_id": golden_id, "assignee": assignee, "action": action_type})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def acknowledge_alert(golden_id: str, merchant_name: str, anomaly_type: str,
                      resolution: str, notes: str = "") -> str:
    """Acknowledge and triage an anomaly alert. Resolution options: investigating, resolved, false_positive, escalated.

    Args:
        golden_id: Merchant golden ID
        merchant_name: Merchant name
        anomaly_type: Type of anomaly (volume_drop, unexpected_inactivity, ticket_spike, health_collapse)
        resolution: Resolution status (investigating, resolved, false_positive, escalated)
        notes: Triage notes explaining the resolution
    """
    try:
        resolved_at = "NOW()" if resolution not in ("investigating", "open") else "NULL"
        _lb_execute(
            f"""INSERT INTO alert_triage (golden_id, merchant_name, anomaly_type, resolution, triaged_by, notes, resolved_at)
               VALUES (%s, %s, %s, %s, 'cdp_supervisor_agent', %s, {resolved_at})""",
            (golden_id, merchant_name, anomaly_type, resolution, notes if notes else None))
        return json.dumps({"status": "triaged", "golden_id": golden_id,
                           "anomaly_type": anomaly_type, "resolution": resolution})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def check_suppression_list(merchant_golden_ids: str) -> str:
    """Check if merchants are on the suppression list (recently contacted, opt-out, etc.).
    Use this before creating campaigns to avoid over-contacting merchants.

    Args:
        merchant_golden_ids: Comma-separated golden IDs to check
    """
    try:
        gids = [g.strip() for g in merchant_golden_ids.split(",") if g.strip()]
        if not gids:
            return json.dumps({"suppressed": [], "total_checked": 0})
        placeholders = ",".join(["%s"] * len(gids))
        rows = _lb_execute(
            f"SELECT DISTINCT golden_id FROM suppression_list WHERE golden_id IN ({placeholders}) AND (expires_at IS NULL OR expires_at > NOW())",
            tuple(gids))
        suppressed = [r["golden_id"] for r in rows]
        return json.dumps({"suppressed": suppressed, "total_checked": len(gids),
                           "suppressed_count": len(suppressed), "clear_count": len(gids) - len(suppressed)})
    except Exception as e:
        return json.dumps({"error": str(e)})


@tool
def get_ops_status() -> str:
    """Get operational status: active campaigns, pending assignments, and triage summary.
    Use this to give the user a quick overview of what's happening operationally.
    """
    try:
        campaigns = _lb_execute(
            "SELECT status, COUNT(*) AS cnt FROM campaigns GROUP BY status ORDER BY cnt DESC")
        assignments = _lb_execute(
            "SELECT status, COUNT(*) AS cnt FROM nba_assignments GROUP BY status ORDER BY cnt DESC")
        triage = _lb_execute(
            "SELECT resolution, COUNT(*) AS cnt FROM alert_triage GROUP BY resolution ORDER BY cnt DESC")
        return json.dumps({"campaigns": campaigns, "assignments": assignments, "triage": triage})
    except Exception as e:
        return json.dumps({"error": str(e)})


ALL_UC_FUNCTIONS = [
    "ahs_demos_catalog.cdp_360.lookup_merchant",
    "ahs_demos_catalog.cdp_360.get_at_risk_merchants",
    "ahs_demos_catalog.cdp_360.get_segment_summary",
    "ahs_demos_catalog.cdp_360.get_segment_merchants",
    "ahs_demos_catalog.cdp_360.get_next_best_actions",
    "ahs_demos_catalog.cdp_360.get_nba_summary",
    "ahs_demos_catalog.cdp_360.get_health_scores",
    "ahs_demos_catalog.cdp_360.get_action_history",
    "ahs_demos_catalog.cdp_360.get_action_log_summary",
    "ahs_demos_catalog.cdp_360.get_churn_kpis",
    "ahs_demos_catalog.cdp_360.get_clv_rankings",
    "ahs_demos_catalog.cdp_360.get_clv_summary",
    "ahs_demos_catalog.cdp_360.get_channel_attribution",
    "ahs_demos_catalog.cdp_360.get_behavioral_segments",
    "ahs_demos_catalog.cdp_360.get_support_analytics",
    "ahs_demos_catalog.cdp_360.get_support_kpis",
    "ahs_demos_catalog.cdp_360.get_call_center_agents",
    "ahs_demos_catalog.cdp_360.get_call_center_sentiment",
    "ahs_demos_catalog.cdp_360.get_personalization_signals",
    "ahs_demos_catalog.cdp_360.get_ad_creative",
    "ahs_demos_catalog.cdp_360.get_campaign_roi",
    "ahs_demos_catalog.cdp_360.get_audience",
    "ahs_demos_catalog.cdp_360.get_anomaly_alerts",
    "ahs_demos_catalog.cdp_360.get_merchant_timeline",
]


class CDPSupervisorAgent(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)

        uc_toolkit = UCFunctionToolkit(function_names=ALL_UC_FUNCTIONS)
        self.tools = list(uc_toolkit.tools) + [
            query_genie_space, log_nba_action,
            create_campaign, assign_nba_action, acknowledge_alert,
            check_suppression_list, get_ops_status,
        ]
        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def _build_graph(self):
        tool_node = ToolNode(self.tools)

        def should_continue(state):
            last = state["messages"][-1]
            if isinstance(last, AIMessage) and last.tool_calls:
                return "tools"
            return "end"

        def call_model(state):
            messages = [{"role": "system", "content": SYSTEM_PROMPT}] + state["messages"]
            response = self.llm_with_tools.invoke(messages)
            return {"messages": [response]}

        graph = StateGraph(AgentState)
        graph.add_node("agent", RunnableLambda(call_model))
        graph.add_node("tools", tool_node)
        graph.add_conditional_edges("agent", should_continue, {"tools": "tools", "end": END})
        graph.add_edge("tools", "agent")
        graph.set_entry_point("agent")
        return graph.compile()

    def predict(self, request: ResponsesAgentRequest) -> ResponsesAgentResponse:
        outputs = [
            event.item
            for event in self.predict_stream(request)
            if event.type == "response.output_item.done"
        ]
        return ResponsesAgentResponse(output=outputs)

    def predict_stream(
        self, request: ResponsesAgentRequest
    ) -> Generator[ResponsesAgentStreamEvent, None, None]:
        messages = to_chat_completions_input([m.model_dump() for m in request.input])
        graph = self._build_graph()

        for event in graph.stream({"messages": messages}, stream_mode=["updates"]):
            if event[0] == "updates":
                for node_data in event[1].values():
                    if node_data.get("messages"):
                        yield from output_to_responses_items_stream(node_data["messages"])


mlflow.langchain.autolog()
AGENT = CDPSupervisorAgent()
mlflow.models.set_model(AGENT)
