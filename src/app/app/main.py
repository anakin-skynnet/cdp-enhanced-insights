"""
PagoNxt Getnet CDP - Customer 360 Application
FastAPI backend with Pydantic response models and mock/Databricks data toggle.

Set CDP_DATA_SOURCE=mock for offline development (no Databricks connection needed).
Set CDP_DATA_SOURCE=databricks (default) for live warehouse data.
"""

from __future__ import annotations

import csv
import io
import os
import json
import httpx
from typing import Union

from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel

from . import models as M

DATA_SOURCE = os.environ.get("CDP_DATA_SOURCE", "mock").lower()

if DATA_SOURCE == "mock":
    from . import mock_data as ds
else:
    from . import db as ds  # type: ignore[no-redef]

app = FastAPI(
    title="Getnet CDP - Customer 360",
    version="2.0.0",
    description="PagoNxt Getnet Customer Data Platform API. "
                f"Data source: **{DATA_SOURCE}**",
)

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
async def root():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/metrics")
async def metrics():
    return {"status": "healthy", "data_source": DATA_SOURCE}


# ── Data Source Config ────────────────────────────────────────────

@app.get("/api/config", response_model=M.DataSourceConfig)
async def get_config():
    connected = DATA_SOURCE == "databricks"
    if connected:
        try:
            ds.query("SELECT 1")
        except Exception:
            connected = False
    return M.DataSourceConfig(
        source=DATA_SOURCE,
        catalog=os.environ.get("CDP_CATALOG", "main"),
        schema=os.environ.get("CDP_SCHEMA", "cdp"),
        warehouse_connected=connected,
    )


@app.post("/api/config/toggle")
async def toggle_data_source():
    """Returns instructions — actual toggle requires env var restart."""
    current = DATA_SOURCE
    other = "databricks" if current == "mock" else "mock"
    return {
        "current": current,
        "instruction": f"Set CDP_DATA_SOURCE={other} and restart to switch.",
    }


# ── Dashboard ────────────────────────────────────────────────────

@app.get("/api/dashboard/kpis", response_model=M.DashboardKPIs)
async def dashboard_kpis():
    return ds.get_dashboard_kpis()


@app.get("/api/dashboard/segments", response_model=list[M.SegmentDistribution])
async def dashboard_segments():
    return ds.get_segment_distribution()


@app.get("/api/dashboard/health", response_model=list[M.HealthDistribution])
async def dashboard_health():
    return ds.get_health_distribution()


# ── Merchants ────────────────────────────────────────────────────

@app.get("/api/merchants", response_model=list[M.MerchantSummary])
async def list_merchants(
    search: str = "",
    segment: str = "",
    health_tier: str = "",
    sort: str = "txn_volume",
    limit: int = Query(default=50, le=200),
    offset: int = 0,
):
    return ds.get_merchants(search, segment, health_tier, sort, limit, offset)


@app.get("/api/merchants/{golden_id}", response_model=M.MerchantDetail)
async def get_merchant(golden_id: str):
    merchant = ds.get_merchant_detail(golden_id)
    if not merchant:
        raise HTTPException(status_code=404, detail="Merchant not found")
    return merchant


# ── Next Best Actions ────────────────────────────────────────────

@app.get("/api/nba/queue", response_model=list[M.NBAItem])
async def nba_queue(
    urgency: str = "",
    segment: str = "",
    limit: int = Query(default=50, le=200),
):
    return ds.get_nba_queue(urgency, segment, limit)


@app.get("/api/nba/summary", response_model=list[M.NBASummaryItem])
async def nba_summary():
    return ds.get_nba_summary()


# ── Campaigns ────────────────────────────────────────────────────

@app.get("/api/campaigns/audience", response_model=list[M.CampaignAudienceMember])
async def campaign_audience(
    segment: str = Query(..., description="Target segment"),
    limit: int = Query(default=100, le=500),
):
    return ds.get_segment_merchants_for_campaign(segment, limit)


@app.post("/api/campaigns/execute", response_model=M.CampaignResponse)
async def execute_campaign(campaign: M.CampaignRequest):
    merchants = [{"golden_id": mid} for mid in campaign.merchant_ids]
    ds.log_campaign({
        "name": campaign.name,
        "action_type": campaign.action_type,
        "channel": campaign.channel,
        "merchants": merchants,
    })
    return M.CampaignResponse(
        status="executed",
        campaign_name=campaign.name,
        merchants_targeted=len(campaign.merchant_ids),
        action=campaign.action_type,
        channel=campaign.channel,
    )


# ── Solution Accelerators ────────────────────────────────────────

@app.get("/api/clv/summary", response_model=list[M.CLVTierSummary])
async def clv_summary():
    return ds.get_clv_summary()


@app.get("/api/clv/top", response_model=list[M.CLVMerchant])
async def clv_top(limit: int = Query(default=20, le=100)):
    return ds.get_clv_top_merchants(limit)


@app.get("/api/attribution", response_model=list[M.ChannelAttribution])
async def channel_attribution():
    return ds.get_channel_attribution()


@app.get("/api/behavioral-segments", response_model=list[M.BehavioralSegment])
async def behavioral_segments():
    return ds.get_behavioral_segments()


# ── Customer Support Analytics ────────────────────────────────────

@app.get("/api/support/kpis", response_model=M.SupportKPIs)
async def support_kpis():
    return ds.get_support_kpis()


@app.get("/api/support/quality", response_model=list[M.SupportQualityDistribution])
async def support_quality():
    return ds.get_support_quality_distribution()


@app.get("/api/support/merchants", response_model=list[M.SupportMerchant])
async def support_merchants(quality: str = "", limit: int = Query(default=50, le=200)):
    return ds.get_support_merchants(quality, limit)


# ── Call Center Analytics ─────────────────────────────────────────

@app.get("/api/call-center/kpis", response_model=M.CallCenterKPIs)
async def call_center_kpis():
    return ds.get_call_center_kpis()


@app.get("/api/call-center/agents", response_model=list[M.CallCenterAgent])
async def call_center_agents(limit: int = Query(default=20, le=100)):
    return ds.get_call_center_agents(limit)


@app.get("/api/call-center/queues", response_model=list[M.CallCenterQueue])
async def call_center_queues():
    return ds.get_call_center_queues()


@app.get("/api/call-center/sentiment", response_model=list[M.SentimentByTopic])
async def call_center_sentiment():
    return ds.get_call_center_sentiment()


# ── Hyper-Personalization ─────────────────────────────────────────

@app.get("/api/personalization/summary", response_model=list[M.PersonalizationSummary])
async def personalization_summary():
    return ds.get_personalization_summary()


@app.get("/api/personalization/{golden_id}")
async def personalization_signals(golden_id: str):
    return ds.get_personalization_for_merchant(golden_id)


@app.get("/api/propensity", response_model=list[M.PropensityDistribution])
async def propensity_distribution():
    return ds.get_propensity_distribution()


# ── Ad Creative ───────────────────────────────────────────────────

@app.get("/api/ad-creative", response_model=list[M.AdCreativeItem])
async def ad_creative():
    return ds.get_ad_creative_library()


# ── Campaign ROI ──────────────────────────────────────────────────

@app.get("/api/campaign-roi", response_model=list[M.CampaignROISummary])
async def campaign_roi():
    return ds.get_campaign_roi_summary()


@app.get("/api/campaign-roi/outcomes", response_model=list[M.CampaignOutcome])
async def campaign_roi_outcomes():
    return ds.get_campaign_outcome_distribution()


# ── Audience Activation ───────────────────────────────────────────

@app.get("/api/audiences/summary", response_model=list[M.AudienceSummary])
async def audience_summary():
    return ds.get_audience_summary()


@app.get("/api/audiences/{audience_type}", response_model=list[M.AudienceMember])
async def audience_list(audience_type: str, limit: int = Query(default=100, le=500)):
    return ds.get_audience_list(audience_type, limit)


# ── Anomaly Alerts ────────────────────────────────────────────────

@app.get("/api/anomaly-alerts/kpis", response_model=M.AnomalyKPIs)
async def anomaly_kpis():
    return ds.get_anomaly_kpis()


@app.get("/api/anomaly-alerts", response_model=list[M.AnomalyAlert])
async def anomaly_alerts(
    anomaly_type: str = "",
    limit: int = Query(default=50, le=200),
):
    return ds.get_anomaly_alerts(anomaly_type, limit)


# ── Merchant Timeline ─────────────────────────────────────────────

@app.get("/api/merchants/{golden_id}/timeline", response_model=list[M.TimelineEvent])
async def merchant_timeline(golden_id: str, limit: int = Query(default=30, le=100)):
    return ds.get_merchant_timeline(golden_id, limit)


# ── Data Freshness ────────────────────────────────────────────────

@app.get("/api/data-freshness", response_model=list[M.TableFreshness])
async def data_freshness():
    return ds.get_data_freshness()


# ── CSV Export ────────────────────────────────────────────────────

@app.get("/api/export/{resource_type}")
async def export_csv(
    resource_type: str,
    audience_type: str = "churn_risk",
    limit: int = Query(default=500, le=2000),
):
    """Export audience lists, merchants, or NBA queue as downloadable CSV."""
    if resource_type == "audience":
        rows = ds.get_audience_list(audience_type, limit)
    elif resource_type == "merchants":
        rows = ds.get_merchants(limit=limit)
    elif resource_type == "nba":
        rows = ds.get_nba_queue(limit=limit)
    elif resource_type == "anomaly-alerts":
        rows = ds.get_anomaly_alerts(limit=limit)
    else:
        raise HTTPException(status_code=400, detail="Supported: audience, merchants, nba, anomaly-alerts")
    if not rows:
        raise HTTPException(status_code=404, detail="No data found")
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=rows[0].keys())
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=cdp_{resource_type}{'_' + audience_type if resource_type == 'audience' else ''}.csv"},
    )


# ── Agent Feedback ────────────────────────────────────────────────

@app.post("/api/agent/feedback")
async def agent_feedback(feedback: M.AgentFeedback):
    """Record user rating / comment on an AI agent response."""
    result = ds.log_agent_feedback(feedback.model_dump())
    return result


# ═══════════════════════════════════════════════════════════════
# AI Agent Chat + Genie
# ═══════════════════════════════════════════════════════════════

AGENT_ENDPOINT = os.environ.get("CDP_AGENT_ENDPOINT", "main-cdp-cdp_supervisor_agent")
GENIE_SPACE_ID = os.environ.get("CDP_GENIE_SPACE_ID", "")


@app.post("/api/agent/chat")
async def agent_chat(req: M.ChatRequest):
    """Proxy chat requests to the CDP Supervisor Agent on Model Serving.
    In mock mode, returns a simulated AI response.
    """
    if DATA_SOURCE == "mock":
        last_msg = req.messages[-1].content if req.messages else ""
        return _mock_agent_response(last_msg)

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        host = w.config.host
        token = w.config.authenticate()

        payload = {
            "input": [{"role": m.role, "content": m.content} for m in req.messages]
        }
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations",
                headers={"Authorization": f"Bearer {token}"},
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
    except Exception as e:
        return {"output": [{"type": "message", "content": [{"type": "output_text", "text": f"Agent error: {str(e)}. Ensure the CDP Supervisor Agent is deployed."}]}]}


@app.post("/api/genie/ask")
async def genie_ask(req: M.GenieRequest):
    """Proxy natural language questions to the Genie Space Conversation API.
    In mock mode, returns a simulated Genie response.
    """
    if DATA_SOURCE == "mock":
        return _mock_genie_response(req.question)

    if not GENIE_SPACE_ID:
        raise HTTPException(status_code=400, detail="Genie Space not configured. Set CDP_GENIE_SPACE_ID.")

    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        from databricks.sdk.service.dashboards import GenieAPI
        genie = GenieAPI(w.api_client)

        conversation = genie.start_conversation(GENIE_SPACE_ID, content=req.question)
        result = genie.get_message_query_result(
            GENIE_SPACE_ID, conversation.conversation_id, conversation.message_id
        )

        columns = [col.name for col in (result.statement_response.manifest.schema.columns or [])]
        rows = []
        chunk = result.statement_response.result
        if chunk and chunk.data_array:
            for row in chunk.data_array[:100]:
                rows.append(dict(zip(columns, row)))

        return {"question": req.question, "columns": columns, "rows": rows, "row_count": len(rows)}
    except Exception as e:
        return {"question": req.question, "error": str(e)}


def _mock_agent_response(question: str) -> dict:
    q = question.lower()
    if "churn" in q or "risk" in q:
        text = ("Based on the Customer 360 data, I've identified **44 at-risk merchants** "
                "representing **$2.3M in revenue at risk**.\n\n"
                "**Top 3 Priority Actions:**\n"
                "| Merchant | Health | Action | Revenue Impact |\n"
                "|----------|--------|--------|----------------|\n"
                "| Restaurante El Gaucho #12 | 18 (critical) | Executive outreach | $45,200 |\n"
                "| Supermercado El Progreso #5 | 22 (poor) | Premium win-back | $38,100 |\n"
                "| Hotel Vista Mar #7 | 25 (poor) | Win-back campaign | $31,500 |\n\n"
                "I recommend starting with executive outreach to the critical-tier merchants today. "
                "Would you like me to draft a retention plan for any specific merchant?")
    elif "campaign" in q or "segment" in q:
        text = ("I've analyzed the segment composition. Here's a campaign recommendation:\n\n"
                "**Campaign: Holiday Reactivation Blitz**\n"
                "- **Target:** 19 hibernating merchants (avg 78 days inactive)\n"
                "- **Offer:** 30-day processing fee waiver\n"
                "- **Channel:** Zender SMS + SFMC email journey\n"
                "- **Expected ROI:** 36% conversion rate based on past win-back campaigns\n"
                "- **Revenue potential:** $95K reactivation value\n\n"
                "Should I pull the audience list for activation via Hightouch?")
    elif "action" in q or "priority" in q or "week" in q:
        text = ("Here's your **Weekly Action Plan**:\n\n"
                "**Monday (Immediate):** 12 merchants need action today\n"
                "- 4x Executive outreach (cant_lose, critical health)\n"
                "- 5x Win-back calls (at_risk, high volume)\n"
                "- 3x Support escalation (open tickets > 5)\n\n"
                "**Wednesday (This Week):** 18 merchants\n"
                "- 8x SFMC email campaigns (loyalty/cross-sell)\n"
                "- 6x Zender SMS (reengagement)\n"
                "- 4x Product education (new customers)\n\n"
                "**Total revenue at stake:** $485K across 30 merchants.\n"
                "Would you like to drill into any specific action or merchant?")
    else:
        text = ("I'm the **Getnet CDP Intelligence Hub**. I can help you with:\n\n"
                "- **Churn prevention** - identify at-risk merchants and retention actions\n"
                "- **Campaign strategy** - design targeted campaigns for any segment\n"
                "- **Next best actions** - prioritized action queues for your team\n"
                "- **Analytics** - CLV, attribution, support quality, call center metrics\n"
                "- **Natural language queries** - ask any question about your merchant data\n\n"
                "Try asking: *'What are the top priority actions for this week?'*")
    return {"output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}]}


def _mock_genie_response(question: str) -> dict:
    q = question.lower()
    if "segment" in q:
        return {"question": question, "columns": ["segment", "merchant_count", "total_volume"],
                "rows": [
                    {"segment": "champions", "merchant_count": 12, "total_volume": 1850000},
                    {"segment": "loyal", "merchant_count": 18, "total_volume": 1420000},
                    {"segment": "at_risk", "merchant_count": 15, "total_volume": 980000},
                    {"segment": "hibernating", "merchant_count": 14, "total_volume": 320000},
                ], "row_count": 4}
    elif "health" in q:
        return {"question": question, "columns": ["health_tier", "count", "avg_score"],
                "rows": [
                    {"health_tier": "excellent", "count": 15, "avg_score": 82.3},
                    {"health_tier": "good", "count": 28, "avg_score": 63.1},
                    {"health_tier": "fair", "count": 35, "avg_score": 42.8},
                    {"health_tier": "poor", "count": 25, "avg_score": 24.2},
                    {"health_tier": "critical", "count": 17, "avg_score": 8.5},
                ], "row_count": 5}
    return {"question": question, "columns": ["result"],
            "rows": [{"result": "Query processed. Connect to Databricks for live results."}],
            "row_count": 1}
