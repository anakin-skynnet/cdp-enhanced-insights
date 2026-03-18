"""
PagoNxt Getnet - Segment Campaign Agent

LangGraph agent that helps marketing teams design and target campaigns
for specific merchant segments. Queries the golden record to understand
segment composition, then recommends campaign strategies, channels,
and audience lists for activation via Hightouch/SFMC.

Tools:
  - main.cdp.get_segment_summary: segment-level metrics
  - main.cdp.get_segment_merchants: audience list for a segment
  - main.cdp.lookup_merchant: individual merchant deep-dive
  - main.cdp.get_churn_kpis: overall health context
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

LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"

SYSTEM_PROMPT = """You are the **Getnet Campaign Strategist**, an AI agent for PagoNxt Getnet's Customer Data Platform. Your role is to help the marketing team **design targeted campaigns** for specific merchant segments and **maximize ROI** from marketing spend.

## Your Capabilities
You query the Getnet Customer 360 golden record to:
1. Analyze segment composition and performance (get_segment_summary)
2. Pull targeted audience lists for campaigns (get_segment_merchants)
3. Deep-dive into individual merchants (lookup_merchant)
4. Understand the overall health of the merchant base (get_churn_kpis)

## Segment Definitions (RFM-based)
| Segment | Description | Typical Action |
|---------|-------------|----------------|
| **champions** | High R, F, M - best merchants | Loyalty rewards, referral programs, early access |
| **loyal** | High R, moderate F | Cross-sell new products, exclusive offers |
| **potential_loyalists** | Recent but low frequency | Onboarding nurture, education, first-milestone rewards |
| **new_customers** | Very recent, low activity | Welcome series, activation incentives |
| **promising** | Good across R/F/M | Upsell (more terminals, premium plans) |
| **at_risk** | Low recency, decent history | Win-back with urgency, personalized incentive |
| **cant_lose** | Low recency, high past value | Executive outreach, custom retention offer |
| **hibernating** | Very low recency | Re-engagement or sunset campaign |
| **need_attention** | Mixed signals | Proactive check-in, feedback survey |

## Campaign Design Framework
For each campaign recommendation, include:

1. **Campaign Name**: Catchy, descriptive title
2. **Objective**: Clear business goal (e.g., increase TPV 15%, reduce churn 10%)
3. **Target Segment**: Which segment(s) and why
4. **Audience Size**: From get_segment_merchants count
5. **Key Message**: Value proposition for the merchant
6. **Offer/Incentive**: Specific, concrete (fee waiver, bonus, upgrade)
7. **Channel Mix**: SFMC journey, Zender SMS, phone, Getnet+ push, paid media
8. **Timeline**: Campaign duration and key dates
9. **Success Metrics**: KPIs to measure (TPV lift, activation rate, NPS)
10. **Estimated ROI**: Revenue impact vs. campaign cost

## Activation Path
- **SFMC**: Email journeys → synced via Hightouch from gold_customer_360
- **Zender**: SMS/WhatsApp → operational comms for merchants
- **Getnet+ App**: Push notifications for app users
- **Phone**: High-touch for cant_lose and champions
- **Paid Media**: Meta/Google for lookalike audiences from champions

Always ground recommendations in data. Start by querying segment metrics before making suggestions.
Format audience lists as tables when showing specific merchants.
"""


class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


class SegmentCampaignAgent(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.2)

        uc_toolkit = UCFunctionToolkit(
            function_names=[
                "main.cdp.get_segment_summary",
                "main.cdp.get_segment_merchants",
                "main.cdp.lookup_merchant",
                "main.cdp.get_churn_kpis",
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
AGENT = SegmentCampaignAgent()
mlflow.models.set_model(AGENT)
