"""
Bank Payment Platform CDP - Customer 360 Application
FastAPI backend with Pydantic response models and mock/Databricks data toggle.

Set CDP_DATA_SOURCE=mock for offline development (no Databricks connection needed).
Set CDP_DATA_SOURCE=databricks for live warehouse data.
"""

from __future__ import annotations

import asyncio
import csv
import datetime
import io
import logging
import os

import httpx

from fastapi import FastAPI, Query, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, JSONResponse
from starlette.requests import Request

from . import models as M

logger = logging.getLogger(__name__)

DATA_SOURCE = os.environ.get("CDP_DATA_SOURCE", "mock").lower()

if DATA_SOURCE == "mock":
    from . import mock_data as ds
else:
    try:
        from . import db as ds  # type: ignore[no-redef]
    except Exception as _import_err:
        logger.error("Failed to import db module, falling back to mock: %s", _import_err)
        from . import mock_data as ds  # type: ignore[no-redef]
        DATA_SOURCE = "mock (fallback)"

_USE_THREADS = DATA_SOURCE not in ("mock", "mock (fallback)")


async def _run(fn, *args, **kwargs):
    """Run sync datasource calls in a thread pool to avoid blocking the event loop."""
    if _USE_THREADS:
        return await asyncio.to_thread(fn, *args, **kwargs)
    return fn(*args, **kwargs)


app = FastAPI(
    title="Bank Payment Platform CDP - Customer 360",
    version="2.2.0",
    description="Bank Payment Platform Customer Data Platform API. "
                f"Data source: **{DATA_SOURCE}**",
)

_GENERIC_ERROR = "An internal error occurred. Check server logs for details."


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    logger.exception("Unhandled error on %s", request.url.path)
    return JSONResponse(status_code=500, content={"error": _GENERIC_ERROR, "path": request.url.path})


_LAKEBASE_ENABLED = os.environ.get("LAKEBASE_INSTANCE_NAME", "") != ""
_lb = None

if _LAKEBASE_ENABLED:
    try:
        from . import lakebase as _lb  # type: ignore[no-redef]
    except Exception as _lb_err:
        logger.error("Failed to import lakebase module: %s", _lb_err)
        _lb = None


@app.on_event("startup")
async def warmup():
    """Pre-initialize the SDK client so the first user request doesn't pay cold-start cost."""
    if _USE_THREADS:
        try:
            await asyncio.to_thread(ds.query, "SELECT 1")
            logger.info("Startup warmup: warehouse connected OK")
        except Exception as e:
            logger.warning("Startup warmup: warehouse not ready yet (%s)", e)
    if _lb:
        try:
            ok = await asyncio.to_thread(_lb.bootstrap_schema)
            logger.info("Lakebase schema bootstrap: %s", "OK" if ok else "FAILED")
        except Exception as e:
            logger.warning("Lakebase warmup failed: %s", e)

_static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
async def root():
    return FileResponse(os.path.join(_static_dir, "index.html"))


@app.get("/api/v1/healthcheck")
async def healthcheck():
    """Standard healthcheck for Databricks Apps OAuth2 token auth."""
    from datetime import datetime, timezone
    diag = {"db_module": "loaded"}
    try:
        from . import db as _db_check
        diag["db_module"] = "ok"
        diag["host"] = str(getattr(_db_check, 'CATALOG', 'n/a'))
    except Exception as e:
        diag["db_module"] = f"error: {e}"
    return {"status": "OK", "timestamp": datetime.now(timezone.utc).isoformat(), "data_source": DATA_SOURCE, "diagnostics": diag}


@app.get("/metrics")
async def metrics():
    return {"status": "healthy", "data_source": DATA_SOURCE}


# ── Data Source Config ────────────────────────────────────────────

@app.get("/api/config", response_model=M.DataSourceConfig)
async def get_config():
    connected = DATA_SOURCE == "databricks"
    if connected:
        try:
            await _run(ds.query, "SELECT 1")
        except Exception:
            connected = False
    lb_connected = False
    if _lb:
        try:
            await _run(_lb._execute, "SELECT 1")
            lb_connected = True
        except Exception:
            pass
    return M.DataSourceConfig(
        source=DATA_SOURCE,
        catalog=os.environ.get("CDP_CATALOG", "ahs_demos_catalog"),
        schema=os.environ.get("CDP_SCHEMA", "cdp_360"),
        warehouse_connected=connected,
        lakebase_connected=lb_connected,
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
    return await _run(ds.get_dashboard_kpis)


@app.get("/api/dashboard/segments", response_model=list[M.SegmentDistribution])
async def dashboard_segments():
    return await _run(ds.get_segment_distribution)


@app.get("/api/dashboard/health", response_model=list[M.HealthDistribution])
async def dashboard_health():
    return await _run(ds.get_health_distribution)


# ── Merchants ────────────────────────────────────────────────────

@app.get("/api/merchants", response_model=list[M.MerchantSummary])
async def list_merchants(
    search: str = "",
    segment: str = "",
    health_tier: str = "",
    sort: str = "txn_volume",
    limit: int = Query(default=50, le=200),
    offset: int = Query(default=0, ge=0),
):
    return await _run(ds.get_merchants, search, segment, health_tier, sort, limit, offset)


@app.get("/api/merchants/{golden_id}", response_model=M.MerchantDetail)
async def get_merchant(golden_id: str):
    merchant = await _run(ds.get_merchant_detail, golden_id)
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
    return await _run(ds.get_nba_queue, urgency, segment, limit)


@app.get("/api/nba/summary", response_model=list[M.NBASummaryItem])
async def nba_summary():
    return await _run(ds.get_nba_summary)


# ── Campaigns ────────────────────────────────────────────────────

@app.get("/api/campaigns/audience", response_model=list[M.CampaignAudienceMember])
async def campaign_audience(
    segment: str = Query(..., description="Target segment"),
    limit: int = Query(default=100, le=500),
):
    return await _run(ds.get_segment_merchants_for_campaign, segment, limit)


@app.post("/api/campaigns/execute", response_model=M.CampaignResponse)
async def execute_campaign(campaign: M.CampaignRequest):
    if not campaign.merchant_ids:
        raise HTTPException(status_code=422, detail="merchant_ids must not be empty")
    merchants = [{"golden_id": mid} for mid in campaign.merchant_ids]
    await _run(ds.log_campaign, {
        "name": campaign.name,
        "action_type": campaign.action_type,
        "channel": campaign.channel,
        "segment": campaign.segment,
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
    return await _run(ds.get_clv_summary)


@app.get("/api/clv/top", response_model=list[M.CLVMerchant])
async def clv_top(limit: int = Query(default=20, le=100)):
    return await _run(ds.get_clv_top_merchants, limit)


@app.get("/api/attribution", response_model=list[M.ChannelAttribution])
async def channel_attribution():
    return await _run(ds.get_channel_attribution)


@app.get("/api/behavioral-segments", response_model=list[M.BehavioralSegment])
async def behavioral_segments():
    return await _run(ds.get_behavioral_segments)


# ── Customer Support Analytics ────────────────────────────────────

@app.get("/api/support/kpis", response_model=M.SupportKPIs)
async def support_kpis():
    return await _run(ds.get_support_kpis)


@app.get("/api/support/quality", response_model=list[M.SupportQualityDistribution])
async def support_quality():
    return await _run(ds.get_support_quality_distribution)


@app.get("/api/support/merchants", response_model=list[M.SupportMerchant])
async def support_merchants(quality: str = "", limit: int = Query(default=50, le=200)):
    return await _run(ds.get_support_merchants, quality, limit)


# ── Call Center Analytics ─────────────────────────────────────────

@app.get("/api/call-center/kpis", response_model=M.CallCenterKPIs)
async def call_center_kpis():
    return await _run(ds.get_call_center_kpis)


@app.get("/api/call-center/agents", response_model=list[M.CallCenterAgent])
async def call_center_agents(limit: int = Query(default=20, le=100)):
    return await _run(ds.get_call_center_agents, limit)


@app.get("/api/call-center/queues", response_model=list[M.CallCenterQueue])
async def call_center_queues():
    return await _run(ds.get_call_center_queues)


@app.get("/api/call-center/sentiment", response_model=list[M.SentimentByTopic])
async def call_center_sentiment():
    return await _run(ds.get_call_center_sentiment)


# ── Hyper-Personalization ─────────────────────────────────────────

@app.get("/api/personalization/summary", response_model=list[M.PersonalizationSummary])
async def personalization_summary():
    return await _run(ds.get_personalization_summary)


@app.get("/api/personalization/{golden_id}")
async def personalization_signals(golden_id: str):
    result = await _run(ds.get_personalization_for_merchant, golden_id)
    if not result:
        merchant = await _run(ds.get_merchant_detail, golden_id)
        if not merchant:
            raise HTTPException(status_code=404, detail="Merchant not found")
        return {"golden_id": golden_id, "message": "No personalization signals available for this merchant."}
    return result


@app.get("/api/propensity", response_model=list[M.PropensityDistribution])
async def propensity_distribution():
    return await _run(ds.get_propensity_distribution)


# ── Ad Creative ───────────────────────────────────────────────────

@app.get("/api/ad-creative", response_model=list[M.AdCreativeItem])
async def ad_creative():
    return await _run(ds.get_ad_creative_library)


_LLM_ENDPOINT = "databricks-meta-llama-3-3-70b-instruct"
_IMAGE_ENDPOINT = "databricks-shutterstock-imageai-v2"


def _call_llm(prompt: str) -> str:
    from databricks.sdk import WorkspaceClient
    w = WorkspaceClient()
    host = w.config.host.rstrip("/")
    auth = w.config.authenticate()
    import httpx as _httpx
    resp = _httpx.post(
        f"{host}/serving-endpoints/{_LLM_ENDPOINT}/invocations",
        headers=auth,
        json={"messages": [{"role": "user", "content": prompt}], "max_tokens": 1200, "temperature": 0.7},
        timeout=90,
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("choices", [{}])[0].get("message", {}).get("content", "")


def _call_image_gen(prompt: str) -> str | None:
    """Generate an image via Foundation Model API and return base64 data."""
    try:
        from databricks.sdk import WorkspaceClient
        w = WorkspaceClient()
        host = w.config.host.rstrip("/")
        auth = w.config.authenticate()
        import httpx as _httpx
        resp = _httpx.post(
            f"{host}/serving-endpoints/{_IMAGE_ENDPOINT}/invocations",
            headers=auth,
            json={"prompt": prompt, "n": 1, "size": "1024x1024"},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
        imgs = data.get("data", [])
        if imgs and imgs[0].get("b64_json"):
            return imgs[0]["b64_json"]
        if imgs and imgs[0].get("url"):
            return imgs[0]["url"]
        return None
    except Exception as e:
        logger.warning("Image generation failed: %s", e)
        return None


@app.post("/api/ad-creative/generate")
async def generate_creative(req: M.GenerateCreativeRequest):
    """Generate hyper-personalized marketing messages using Foundation Model API."""
    prompt = f"""You are an expert marketing copywriter for a payment processing platform.
Generate hyper-personalized marketing content for a campaign targeting the "{req.segment}" merchant segment.

Campaign context:
- Campaign name: {req.campaign_name or 'General outreach'}
- Channel: {req.channel}
- Objective: {req.objective or 'Engagement and retention'}
- Tone: {req.tone or 'Professional yet warm'}
{f'- Merchant profile: {req.merchant_context}' if req.merchant_context else ''}

Generate content in this exact JSON format (no markdown, no extra text):
{{
  "email_subject": "Compelling subject line (50 chars max)",
  "email_body": "Full email body (3-4 paragraphs, with personalization tokens like {{merchant_name}}, {{segment}})",
  "sms_message": "Concise SMS (160 chars max)",
  "push_notification": "Short push (80 chars max)",
  "ad_headline": "Punchy headline (30 chars max)",
  "ad_description": "Ad body copy (90 chars max)",
  "social_post": "Social media post with hashtags (280 chars max)",
  "banner_tagline": "Hero banner tagline (15 words max)",
  "tone": "{req.tone or 'Professional yet warm'}",
  "cta": "Clear call-to-action text"
}}"""

    try:
        raw = await _run(_call_llm, prompt)
        import json as _json
        start = raw.find("{")
        end = raw.rfind("}") + 1
        if start >= 0 and end > start:
            result = _json.loads(raw[start:end])
        else:
            result = {"error": "Could not parse LLM response", "raw": raw[:500]}
        result["segment"] = req.segment
        result["channel"] = req.channel
        return result
    except Exception as e:
        logger.exception("Creative generation failed")
        raise HTTPException(status_code=502, detail=f"Creative generation failed: {type(e).__name__}")


@app.post("/api/ad-creative/generate-image")
async def generate_image(req: M.GenerateImageRequest):
    """Generate a campaign banner image using Foundation Model API."""
    img_prompt = f"""Professional marketing banner for a payment platform campaign.
Theme: {req.theme or req.segment + ' merchant engagement'}.
Style: Modern, clean, corporate fintech design with subtle gradients.
Text overlay: "{req.tagline or 'Grow your business with us'}"
Color scheme: Blue and white professional palette.
No text in the image, just visual design."""

    try:
        b64_or_url = await _run(_call_image_gen, img_prompt)
        if not b64_or_url:
            raise HTTPException(status_code=502, detail="Image generation endpoint unavailable")
        is_url = b64_or_url.startswith("http")
        return {"image": b64_or_url, "type": "url" if is_url else "base64", "prompt": img_prompt[:200]}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Image generation failed")
        raise HTTPException(status_code=502, detail=f"Image generation failed: {type(e).__name__}")


# ── Campaign ROI ──────────────────────────────────────────────────

@app.get("/api/campaign-roi", response_model=list[M.CampaignROISummary])
async def campaign_roi():
    return await _run(ds.get_campaign_roi_summary)


@app.get("/api/campaign-roi/outcomes", response_model=list[M.CampaignOutcome])
async def campaign_roi_outcomes():
    return await _run(ds.get_campaign_outcome_distribution)


# ── Audience Activation ───────────────────────────────────────────

@app.get("/api/audiences/summary", response_model=list[M.AudienceSummary])
async def audience_summary():
    return await _run(ds.get_audience_summary)


_VALID_AUDIENCES = {"churn_risk", "high_value", "new_onboarding", "winback", "growth", "vip", "immediate_action"}


@app.get("/api/audiences/{audience_type}", response_model=list[M.AudienceMember])
async def audience_list(audience_type: str, limit: int = Query(default=100, le=500)):
    if audience_type not in _VALID_AUDIENCES:
        raise HTTPException(status_code=422, detail=f"Invalid audience_type. Valid: {', '.join(sorted(_VALID_AUDIENCES))}")
    return await _run(ds.get_audience_list, audience_type, limit)


# ── Anomaly Alerts ────────────────────────────────────────────────

@app.get("/api/anomaly-alerts/kpis", response_model=M.AnomalyKPIs)
async def anomaly_kpis():
    return await _run(ds.get_anomaly_kpis)


@app.get("/api/anomaly-alerts", response_model=list[M.AnomalyAlert])
async def anomaly_alerts(
    anomaly_type: str = "",
    limit: int = Query(default=50, le=200),
):
    return await _run(ds.get_anomaly_alerts, anomaly_type, limit)


# ── Merchant Timeline ─────────────────────────────────────────────

@app.get("/api/merchants/{golden_id}/timeline", response_model=list[M.TimelineEvent])
async def merchant_timeline(golden_id: str, limit: int = Query(default=30, le=100)):
    merchant = await _run(ds.get_merchant_detail, golden_id)
    if not merchant:
        raise HTTPException(status_code=404, detail="Merchant not found")
    return await _run(ds.get_merchant_timeline, golden_id, limit)


# ── Data Freshness ────────────────────────────────────────────────

@app.get("/api/data-freshness", response_model=list[M.TableFreshness])
async def data_freshness():
    return await _run(ds.get_data_freshness)


# ── CSV Export ────────────────────────────────────────────────────

@app.get("/api/export/{resource_type}")
async def export_csv(
    resource_type: str,
    audience_type: str = "churn_risk",
    limit: int = Query(default=500, le=2000),
):
    """Export audience lists, merchants, or NBA queue as downloadable CSV."""
    if resource_type == "audience":
        rows = await _run(ds.get_audience_list, audience_type, limit)
    elif resource_type == "merchants":
        rows = await _run(ds.get_merchants, limit=limit)
    elif resource_type == "nba":
        rows = await _run(ds.get_nba_queue, limit=limit)
    elif resource_type == "anomaly-alerts":
        rows = await _run(ds.get_anomaly_alerts, limit=limit)
    elif resource_type == "support":
        rows = await _run(ds.get_support_merchants, limit=limit)
    elif resource_type == "campaign-roi":
        rows = await _run(ds.get_campaign_roi_summary)
    else:
        raise HTTPException(status_code=400, detail="Supported: audience, merchants, nba, anomaly-alerts, support, campaign-roi")
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


# ═══════════════════════════════════════════════════════════════
# Lakebase Operational Endpoints
# ═══════════════════════════════════════════════════════════════


def _require_lakebase():
    if not _lb:
        raise HTTPException(status_code=503, detail="Lakebase not configured. Set LAKEBASE_INSTANCE_NAME.")


@app.get("/api/ops/health")
async def ops_health():
    """Diagnostic endpoint for Lakebase connection health."""
    result = {"lakebase_enabled": _LAKEBASE_ENABLED, "module_loaded": _lb is not None}
    if not _lb:
        result["error"] = "Module not loaded"
        return result
    try:
        rows = await _run(_lb._execute, "SELECT 1 AS ok")
        result["connected"] = True
        result["test_query"] = rows
    except Exception as e:
        result["connected"] = False
        result["error"] = f"{type(e).__name__}: {str(e)}"
    try:
        tables = await _run(
            _lb._execute,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename"
        )
        result["tables"] = [t["tablename"] for t in tables]
    except Exception:
        pass
    return result


@app.post("/api/ops/bootstrap")
async def ops_bootstrap():
    """Manually bootstrap Lakebase schema with detailed error reporting."""
    _require_lakebase()
    import psycopg
    try:
        conn_params = await _run(_lb._get_conn_params)
        with psycopg.connect(**conn_params, autocommit=True) as conn:
            with conn.cursor() as cur:
                results = []
                for stmt in _lb._SCHEMA_SQL.split(";"):
                    stmt = stmt.strip()
                    if stmt:
                        try:
                            cur.execute(stmt)
                            results.append({"sql": stmt[:80], "status": "ok"})
                        except Exception as e:
                            results.append({"sql": stmt[:80], "status": "error", "detail": str(e)})
        return {"status": "ok", "statements": results}
    except Exception as e:
        return {"status": "error", "detail": f"{type(e).__name__}: {str(e)}"}


# ── Ops KPIs ─────────────────────────────────────────────────────

@app.get("/api/ops/kpis")
async def ops_kpis():
    _require_lakebase()
    return await _run(_lb.get_ops_kpis)


# ── Campaigns ────────────────────────────────────────────────────

@app.get("/api/ops/campaigns")
async def ops_campaigns(status: str = "", limit: int = Query(default=50, le=200)):
    _require_lakebase()
    return await _run(_lb.get_campaigns, status, limit)


@app.get("/api/ops/campaigns/{campaign_id}")
async def ops_campaign_detail(campaign_id: str):
    _require_lakebase()
    result = await _run(_lb.get_campaign_detail, campaign_id)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result


@app.post("/api/ops/campaigns")
async def ops_create_campaign(req: M.CreateCampaignRequest):
    _require_lakebase()
    return await _run(
        _lb.create_campaign,
        req.name, req.segment, req.action_type, req.channel,
        req.owner, req.scheduled_at, req.merchant_ids, req.notes,
    )


@app.patch("/api/ops/campaigns/{campaign_id}/status")
async def ops_update_campaign_status(campaign_id: str, req: M.UpdateStatusRequest):
    _require_lakebase()
    result = await _run(_lb.update_campaign_status, campaign_id, req.status)
    if not result:
        raise HTTPException(status_code=404, detail="Campaign not found")
    return result


# ── NBA Assignments ──────────────────────────────────────────────

@app.get("/api/ops/assignments")
async def ops_assignments(assignee: str = "", status: str = "", limit: int = Query(default=50, le=200)):
    _require_lakebase()
    return await _run(_lb.get_assignments, assignee, status, limit)


@app.post("/api/ops/assignments")
async def ops_create_assignment(req: M.CreateAssignmentRequest):
    _require_lakebase()
    return await _run(
        _lb.create_assignment,
        req.golden_id, req.merchant_name, req.action_type, req.channel,
        req.assignee, req.due_date, req.priority_score, req.revenue_impact, req.notes,
    )


@app.patch("/api/ops/assignments/{assignment_id}/status")
async def ops_update_assignment(assignment_id: str, req: M.UpdateStatusRequest):
    _require_lakebase()
    result = await _run(_lb.update_assignment_status, assignment_id, req.status, req.notes)
    if not result:
        raise HTTPException(status_code=404, detail="Assignment not found")
    return result


# ── Alert Triage ─────────────────────────────────────────────────

@app.get("/api/ops/triage")
async def ops_triage_log(resolution: str = "", limit: int = Query(default=50, le=200)):
    _require_lakebase()
    return await _run(_lb.get_triage_log, resolution, limit)


@app.get("/api/ops/triage/kpis")
async def ops_triage_kpis():
    _require_lakebase()
    return await _run(_lb.get_triage_kpis)


@app.post("/api/ops/triage")
async def ops_triage_alert(req: M.TriageAlertRequest):
    _require_lakebase()
    return await _run(
        _lb.triage_alert,
        req.golden_id, req.merchant_name, req.anomaly_type,
        req.resolution, req.triaged_by, req.notes,
    )


# ── Suppression ──────────────────────────────────────────────────

@app.post("/api/ops/suppression/check")
async def ops_check_suppression(merchant_ids: list[str]):
    _require_lakebase()
    return await _run(_lb.check_suppression, merchant_ids)


# ── Agent Feedback ────────────────────────────────────────────────

@app.post("/api/agent/feedback")
async def agent_feedback(feedback: M.AgentFeedback):
    """Record user rating / comment on an AI agent response."""
    result = await _run(ds.log_agent_feedback, feedback.model_dump())
    return result


# ═══════════════════════════════════════════════════════════════
# AI Agent Chat + Genie
# ═══════════════════════════════════════════════════════════════

AGENT_ENDPOINT = os.environ.get("CDP_AGENT_ENDPOINT", "agents_ahs_demos_catalog-cdp_360-cdp_supervisor_agent").strip()
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
        host = w.config.host.rstrip("/")
        headers_fn = w.config.authenticate
        auth_headers = headers_fn()

        payload = {
            "input": [{"role": m.role, "content": m.content} for m in req.messages]
        }
        async with httpx.AsyncClient(timeout=120) as client:
            resp = await client.post(
                f"{host}/serving-endpoints/{AGENT_ENDPOINT}/invocations",
                headers=auth_headers,
                json=payload,
            )
            resp.raise_for_status()
            return resp.json()
    except httpx.HTTPStatusError as e:
        logger.exception("Agent chat HTTP error: %s", e.response.status_code)
        raise HTTPException(status_code=502, detail="Agent endpoint returned an error. Check that the CDP Supervisor Agent is deployed and healthy.")
    except Exception as e:
        logger.exception("Agent chat error")
        raise HTTPException(status_code=502, detail="Failed to reach the AI Agent. Ensure the CDP Supervisor Agent is deployed.")


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

        wait = w.genie.start_conversation(space_id=GENIE_SPACE_ID, content=req.question)
        msg = wait.result(timeout=datetime.timedelta(seconds=120))

        conversation_id = wait.conversation_id
        message_id = wait.message_id

        genie_sql = None
        text_answer = None
        for att in (msg.attachments or []):
            if att.query and getattr(att.query, "query", None):
                genie_sql = att.query.query
            if att.text and getattr(att.text, "content", None):
                text_answer = att.text.content

        if genie_sql:
            from databricks.sdk.service.sql import StatementState
            warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")
            resp = w.statement_execution.execute_statement(
                statement=genie_sql,
                warehouse_id=warehouse_id,
                wait_timeout="50s",
            )
            if resp.status and resp.status.state == StatementState.SUCCEEDED and resp.manifest:
                columns = [c.name for c in (resp.manifest.schema.columns or [])]
                rows = []
                if resp.result and resp.result.data_array:
                    for row in resp.result.data_array[:100]:
                        rows.append(dict(zip(columns, row)))
                return {"question": req.question, "columns": columns, "rows": rows, "row_count": len(rows), "text_answer": text_answer, "sql": genie_sql}

        return {"question": req.question, "columns": ["answer"], "rows": [{"answer": text_answer or "Genie processed your question but returned no tabular data."}], "row_count": 1}
    except Exception as e:
        logger.exception("Genie ask error")
        raise HTTPException(status_code=502, detail="Genie query failed. Check Genie Space configuration and warehouse connectivity.")


def _mock_agent_response(question: str) -> dict:
    q = question.lower()
    if "churn" in q or "risk" in q:
        text = ("Based on the Customer 360 data, the **overall churn risk rate is 36.7%** "
                "(44 of 120 merchants in at-risk, can't-lose, or hibernating segments), "
                "representing **$2.3M in revenue at risk**.\n\n"
                "**Key Churn Metrics:**\n"
                "- At-risk merchants: **44** (36.7%)\n"
                "- Avg health score (at-risk): **22.4** vs portfolio avg **47.8**\n"
                "- Revenue concentration at risk: **18.5%** of total volume\n\n"
                "**Top 3 Priority Actions:**\n"
                "| Merchant | Health | Action | Revenue Impact |\n"
                "|----------|--------|--------|----------------|\n"
                "| Restaurante El Gaucho #12 | 18 (critical) | Executive outreach | $45,200 |\n"
                "| Supermercado El Progreso #5 | 22 (poor) | Premium win-back | $38,100 |\n"
                "| Hotel Vista Mar #7 | 25 (poor) | Win-back campaign | $31,500 |\n\n"
                "I recommend starting with executive outreach to the critical-tier merchants today. "
                "Would you like me to drill into a specific segment or draft a retention plan?")
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
        text = ("I'm the **CDP Intelligence Hub**. I can help you with:\n\n"
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
