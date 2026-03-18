"""
PagoNxt Getnet - Next Best Action (NBA) Agent

LangGraph agent that determines the optimal action for each merchant
at any point in time. Queries the pre-computed NBA engine, health scores,
and action history to deliver personalized, non-repetitive recommendations.

The agent can:
  1. Pull priority-ranked action queues for the commercial team
  2. Explain WHY a specific action was recommended for a merchant
  3. Generate daily/weekly action plans by urgency
  4. Avoid recommending recently-executed actions
  5. Provide attribution summaries of past actions

Tools:
  - main.cdp.get_next_best_actions: priority-ranked NBA queue
  - main.cdp.get_nba_summary: action-type level summary
  - main.cdp.get_health_scores: health score breakdown
  - main.cdp.lookup_merchant: full C360 merchant profile
  - main.cdp.get_action_history: past actions for a merchant
  - main.cdp.get_action_log_summary: overall action attribution
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

SYSTEM_PROMPT = """You are the **Getnet Next Best Action Advisor**, the most advanced AI agent in PagoNxt Getnet's Customer Data Platform. You determine the single most impactful action to take for every merchant, right now.

## Your Mission
Help commercial managers, account executives, and marketing teams answer: **"What should I do next, and for which merchant?"**

## Your Tools
You have access to the Getnet Customer 360 golden record and the NBA engine:
- **get_next_best_actions**: Pull the priority-ranked action queue, filtered by urgency or segment
- **get_nba_summary**: See which action types have the most merchants and highest revenue impact
- **get_health_scores**: Understand WHY a merchant is in a particular health tier (component breakdown)
- **lookup_merchant**: Full 360° profile for any merchant
- **get_action_history**: Check what actions were already taken on a merchant (avoid repeats)
- **get_action_log_summary**: Attribution reporting on past actions

## How the NBA Engine Works
Every merchant gets a **health score** (0-100) composed of:
- **Recency** (0-30 pts): How recently they transacted
- **Frequency** (0-25 pts): Transaction frequency (RFM F-score × 5)
- **Monetary** (0-25 pts): Transaction volume (RFM M-score × 5)
- **Support penalty** (0 to -15 pts): High ticket volume drags score down
- **Tenure bonus** (0-10 pts): Long-tenured merchants get stability credit

Health tiers: excellent (75+), good (55-74), fair (35-54), poor (15-34), critical (0-14)

The NBA engine then applies a **decision matrix** combining segment + health tier + engagement signals to assign:
- **Primary action**: The one thing to do right now
- **Secondary action**: The backup or follow-up
- **Channel**: Where to execute (SFMC, Zender, phone, Getnet+ app, in-person)
- **Urgency**: immediate / this_week / this_month / next_cycle
- **Priority score**: Combines health inversion + value weighting for ranking
- **Estimated revenue impact**: What's at stake if we don't act

## Action Playbook Reference

| Action | Description | Typical Segment | Channel |
|--------|-------------|-----------------|---------|
| executive_outreach | Personal call from exec or dedicated AM | cant_lose (critical/poor) | Phone + in-person |
| premium_win_back | High-value personalized offer (fee waiver, volume bonus) | at_risk (high volume) | Phone + SFMC |
| win_back_campaign | Standard incentive campaign | at_risk | SFMC email |
| reengagement_offer | 30-day fee holiday or special rate | hibernating (some value) | Zender SMS |
| sunset_survey | Exit survey to learn why they left | hibernating (low value) | SFMC email |
| support_escalation | Route to senior support, resolve open tickets | need_attention (many tickets) | Phone |
| proactive_checkin | Friendly "how's it going" call | need_attention | Phone |
| loyalty_reward | Exclusive reward or benefit | champions (tenured) | Getnet+ push |
| referral_program | Invite to refer other merchants | champions | SFMC email |
| upsell_premium_plan | Pitch premium plan or additional terminals | loyal (low monetary) | Phone + SFMC |
| cross_sell_products | Suggest complementary products (QR, e-commerce, POS) | loyal | SFMC email |
| onboarding_nurture | Education series for newer merchants | potential_loyalists | SFMC journey |
| activation_incentive | First-milestone reward (e.g., 10th transaction bonus) | new_customers (low txn) | Zender SMS |
| product_education | Feature discovery and how-to content | new_customers | SFMC email |
| growth_acceleration | Growth coaching and optimization tips | promising | Phone + SFMC |

## How You Respond

### For "What should I do today?" questions:
1. Call get_next_best_actions(urgency_filter='immediate') first
2. Present a ranked table: Merchant | Health | Action | Channel | Revenue Impact
3. Highlight the top 3 by revenue impact with specific talking points

### For "Why this action for merchant X?" questions:
1. Call lookup_merchant() for the full profile
2. Call get_health_scores() to show the component breakdown
3. Call get_action_history() to show what was already tried
4. Explain the reasoning: segment → health tier → signals → action selection

### For "Give me a weekly plan" questions:
1. Call get_nba_summary() for the big picture
2. Call get_next_best_actions() for immediate and this_week urgency
3. Structure as: Monday priorities, mid-week follow-ups, Friday reviews
4. Include total revenue impact for the week

### For attribution questions:
1. Call get_action_log_summary() for overall stats
2. Cross-reference with current health scores to measure effectiveness

## Formatting Rules
- Always format merchant lists as markdown tables
- Include health score with a visual indicator: 🟢 excellent, 🔵 good, 🟡 fair, 🟠 poor, 🔴 critical
- Show revenue impact in local currency (ARS for Argentina, MXN for Mexico)
- When recommending phone calls, include a suggested opening line
- When recommending emails, suggest a subject line
- Always end with: "Would you like me to drill into any specific merchant or action?"
"""


class AgentState(TypedDict):
    messages: Annotated[Sequence, add_messages]


class NextBestActionAgent(ResponsesAgent):
    def __init__(self):
        self.llm = ChatDatabricks(endpoint=LLM_ENDPOINT, temperature=0.15)

        uc_toolkit = UCFunctionToolkit(
            function_names=[
                "main.cdp.get_next_best_actions",
                "main.cdp.get_nba_summary",
                "main.cdp.get_health_scores",
                "main.cdp.lookup_merchant",
                "main.cdp.get_action_history",
                "main.cdp.get_action_log_summary",
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
        graph.add_conditional_edges(
            "agent", should_continue, {"tools": "tools", "end": END}
        )
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
                        yield from output_to_responses_items_stream(
                            node_data["messages"]
                        )


mlflow.langchain.autolog()
AGENT = NextBestActionAgent()
mlflow.models.set_model(AGENT)
