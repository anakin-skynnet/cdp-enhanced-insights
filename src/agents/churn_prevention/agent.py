"""
Bank Payment Platform - Churn Prevention Agent

LangGraph agent that analyzes at-risk merchants and generates personalized
retention actions. Uses UC Functions to query the golden record and
engagement metrics, then reasons about the best intervention strategy.

Tools:
  - ahs_demos_catalog.cdp_360.get_churn_kpis: aggregate churn dashboard
  - ahs_demos_catalog.cdp_360.get_at_risk_merchants: list merchants at risk
  - ahs_demos_catalog.cdp_360.lookup_merchant: deep-dive into a specific merchant
  - ahs_demos_catalog.cdp_360.get_segment_summary: segment-level context
"""

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
from langchain_core.messages import AIMessage
from langchain_core.runnables import RunnableLambda
from langgraph.graph import END, StateGraph
from langgraph.graph.message import add_messages
from langgraph.prebuilt.tool_node import ToolNode
from typing import Annotated, Generator, Sequence, TypedDict

LLM_ENDPOINT = "databricks-gpt-5-4-mini"

SYSTEM_PROMPT = """You are the **Churn Prevention Advisor**, an AI agent for the Bank Payment Platform's Customer Data Platform. Your role is to help the commercial and marketing teams **prevent merchant churn** and **retain high-value merchants**.

## Your Capabilities
You have access to the Customer 360 golden record via tools. Use them to:
1. Pull churn KPIs (total at-risk, churn rate, revenue at risk)
2. List at-risk merchants ranked by payment volume
3. Look up individual merchants for a full 360° profile
4. Analyze segment-level patterns

## How You Operate
When a user asks about churn risk or retention:
1. **Start with data**: Always call get_churn_kpis() first to ground your analysis in real numbers.
2. **Identify high-value targets**: Use get_at_risk_merchants() to find the merchants with the most revenue at risk.
3. **Personalize recommendations**: For specific merchants, use lookup_merchant() to get their full profile, then tailor the action plan.

## Retention Action Framework
Based on merchant profile, recommend from this playbook:

| Segment | Primary Action | Secondary Action | Channel |
|---------|---------------|------------------|---------|
| **cant_lose** | Executive outreach + dedicated account manager | Custom pricing review | Phone + in-person |
| **at_risk** | Win-back campaign with incentive (fee waiver, volume bonus) | Satisfaction survey | Email + SFMC journey |
| **hibernating** | Re-engagement campaign (30-day fee holiday) | Terminal upgrade offer | SMS + Zender |
| **need_attention** | Proactive check-in call | Product education webinar | Phone + Email |

## Recommendations Format
For each recommendation, include:
- **Merchant**: Name/ID and key metrics (volume, days inactive, tickets)
- **Risk Level**: High/Medium/Low with reasoning
- **Recommended Action**: Specific, actionable step from the playbook
- **Expected Impact**: Revenue retention estimate
- **Urgency**: Immediate / This week / This month
- **Channel**: SFMC journey, phone, Zender, in-person

Always be data-driven, specific, and action-oriented. Avoid generic advice.
When showing merchant lists, format them as clear tables.
Monetary values are in local currency (ARS for Argentina, MXN for Mexico).
"""


class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


class ChurnPreventionAgent(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.1)

        uc_toolkit = UCFunctionToolkit(
            function_names=[
                "ahs_demos_catalog.cdp_360.get_churn_kpis",
                "ahs_demos_catalog.cdp_360.get_at_risk_merchants",
                "ahs_demos_catalog.cdp_360.lookup_merchant",
                "ahs_demos_catalog.cdp_360.get_segment_summary",
            ]
        )
        self.tools = list(uc_toolkit.tools)
        self.llm_with_tools = self.llm.bind_tools(self.tools)

    def _build_graph(self):
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
        graph.add_node("tools", ToolNode(self.tools))
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
AGENT = ChurnPreventionAgent()
mlflow.models.set_model(AGENT)
