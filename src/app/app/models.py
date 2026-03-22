"""
Pydantic response models for the CDP API.
Provides type safety, OpenAPI documentation, and serialization validation.
"""

from __future__ import annotations

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


# ── Dashboard ──────────────────────────────────────────────────

class DashboardKPIs(BaseModel):
    total_merchants: int = 0
    total_volume: float = 0
    avg_volume: float = 0
    total_tickets: int = 0
    avg_recency: int = 0
    at_risk_merchants: int = 0
    avg_health_score: float = 0


class SegmentDistribution(BaseModel):
    segment: str = "unassigned"
    merchant_count: int = 0
    total_volume: float = 0


class HealthDistribution(BaseModel):
    health_tier: str = "unknown"
    merchant_count: int = 0
    avg_score: float = 0


# ── Merchants ──────────────────────────────────────────────────

class MerchantSummary(BaseModel):
    golden_id: str
    merchant_name: str = ""
    email: Optional[str] = None
    industry: str = ""
    country: str = ""
    segment: str = "unassigned"
    health_score: int = 0
    health_tier: str = "unknown"
    txn_volume: float = 0
    txn_count: int = 0
    days_since_last_txn: int = -1
    ticket_count: int = 0
    r_score: int = 0
    f_score: int = 0
    m_score: int = 0


class MerchantDetail(MerchantSummary):
    phone: Optional[str] = None
    city: str = ""
    recency_score: int = 0
    frequency_score: int = 0
    monetary_score: int = 0
    support_penalty: int = 0
    tenure_bonus: int = 0
    tenure_days: int = 0
    primary_action: Optional[str] = None
    secondary_action: Optional[str] = None
    primary_channel: Optional[str] = None
    urgency: Optional[str] = None
    priority_score: float = 0
    estimated_revenue_impact: float = 0


# ── Next Best Actions ──────────────────────────────────────────

class NBAItem(BaseModel):
    golden_id: str
    merchant_name: str = ""
    email: Optional[str] = None
    segment: str = "unassigned"
    health_score: int = 0
    health_tier: str = "unknown"
    primary_action: str = "review"
    secondary_action: Optional[str] = None
    primary_channel: str = "email"
    urgency: str = "this_week"
    priority_score: float = 0
    estimated_revenue_impact: float = 0
    txn_volume: float = 0
    days_since_last_txn: int = -1
    ticket_count: int = 0


class NBASummaryItem(BaseModel):
    primary_action: str = "review"
    merchant_count: int = 0
    revenue_impact: float = 0
    avg_health: float = 0
    immediate: int = 0
    this_week: int = 0


# ── Campaigns ──────────────────────────────────────────────────

class CampaignRequest(BaseModel):
    name: str
    action_type: str
    channel: str
    segment: str
    merchant_ids: list[str]
    message: Optional[str] = None


class CampaignResponse(BaseModel):
    status: str
    campaign_name: str
    merchants_targeted: int
    action: str
    channel: str


class CampaignAudienceMember(BaseModel):
    golden_id: str
    merchant_name: str = ""
    email: Optional[str] = None
    txn_volume: float = 0
    health_score: int = 0
    primary_action: Optional[str] = None


# ── Solution Accelerators ──────────────────────────────────────

class CLVTierSummary(BaseModel):
    clv_tier: str = "unknown"
    merchant_count: int = 0
    avg_clv: float = 0
    total_clv: float = 0
    avg_p_alive: float = 0


class CLVMerchant(BaseModel):
    golden_id: str
    merchant_name: str = ""
    clv_12m: float = 0
    clv_tier: str = "unknown"
    p_alive: float = 0
    predicted_purchases_12m: float = 0
    total_amount: float = 0


class ChannelAttribution(BaseModel):
    channel: str = "unknown"
    markov_attributed_value: float = 0
    markov_attribution_share: float = 0
    first_touch_value: Optional[float] = 0
    last_touch_value: Optional[float] = 0
    linear_value: Optional[float] = 0
    time_decay_value: Optional[float] = 0


class BehavioralSegment(BaseModel):
    behavioral_segment: str = "unknown"
    merchant_count: int = 0
    avg_health: float = 0
    avg_volume: float = 0
    avg_tickets: float = 0
    avg_recency: float = 0


# ── Support Analytics ──────────────────────────────────────────

class SupportKPIs(BaseModel):
    merchants_with_tickets: int = 0
    total_tickets: int = 0
    open_tickets: int = 0
    avg_first_response_min: float = 0
    avg_resolution_min: float = 0
    overall_csat: float = 0
    sla_first_response_pct: float = 0
    sla_resolution_pct: float = 0


class SupportQualityDistribution(BaseModel):
    support_quality_tier: str = "unknown"
    merchant_count: int = 0
    avg_resolution: float = 0
    avg_csat: float = 0


class SupportMerchant(BaseModel):
    golden_id: str
    merchant_name: str = ""
    total_tickets: int = 0
    open_tickets: int = 0
    resolved_tickets: int = 0
    avg_first_response_min: float = 0
    avg_resolution_min: float = 0
    csat_score: Optional[float] = None
    sla_resolution_pct: Optional[float] = None
    support_quality_tier: str = "unknown"


# ── Call Center ────────────────────────────────────────────────

class CallCenterKPIs(BaseModel):
    total_interactions: int = 0
    total_voice: int = 0
    total_chat: int = 0
    avg_handle_time: int = 0
    avg_queue_wait: int = 0
    avg_resolution_rate: float = 0
    avg_abandonment_rate: float = 0


class CallCenterAgent(BaseModel):
    agent_id: str = ""
    total_interactions: int = 0
    voice_calls: int = 0
    chat_sessions: int = 0
    email_interactions: int = 0
    avg_handle_time_sec: int = 0
    avg_talk_time_sec: int = 0
    avg_hold_time_sec: int = 0
    avg_queue_wait_sec: int = 0
    resolution_rate: Optional[float] = None
    abandonment_rate: Optional[float] = None


class CallCenterQueue(BaseModel):
    queue_name: str = "unknown"
    total_interactions: int = 0
    avg_handle_time_sec: int = 0
    avg_queue_wait_sec: int = 0
    abandonment_rate: Optional[float] = None
    service_level_pct: Optional[float] = None


class SentimentByTopic(BaseModel):
    topic_category: str = "unknown"
    interaction_count: int = 0
    positive_pct: float = 0
    negative_pct: float = 0
    neutral_pct: float = 0
    avg_duration: int = 0


# ── Personalization ────────────────────────────────────────────

class PersonalizationSummary(BaseModel):
    content_theme: str = "unknown"
    merchant_tier: str = "unknown"
    merchant_count: int = 0
    avg_upsell: float = 0
    avg_churn: float = 0
    avg_activation: float = 0


class PropensityDistribution(BaseModel):
    propensity_tier: str = "unknown"
    merchant_count: int = 0
    avg_churn: float = 0
    avg_upsell: float = 0
    avg_activation: float = 0


# ── Ad Creative ────────────────────────────────────────────────

class AdCreativeItem(BaseModel):
    segment: str = "unknown"
    email_subject: Optional[str] = None
    email_body_preview: Optional[str] = None
    sms_message: Optional[str] = None
    push_notification: Optional[str] = None
    ad_headline: Optional[str] = None
    ad_description: Optional[str] = None
    tone: Optional[str] = None
    cta: Optional[str] = None
    merchant_count: int = 0
    avg_volume: float = 0


class GenerateCreativeRequest(BaseModel):
    segment: str = Field(..., description="Target segment")
    channel: str = Field(default="email", description="Primary channel: email, sms, push, social, display")
    campaign_name: Optional[str] = None
    objective: Optional[str] = None
    tone: Optional[str] = Field(default="Professional yet warm")
    merchant_context: Optional[str] = Field(default=None, description="Additional merchant-level context for hyper-personalization")


class GenerateImageRequest(BaseModel):
    segment: str = Field(..., description="Target segment for visual style")
    tagline: Optional[str] = Field(default=None, description="Text to inspire the banner")
    theme: Optional[str] = Field(default=None, description="Visual theme override")
    merchant_context: Optional[str] = Field(default=None, description="Merchant industry/context for thematic visuals")
    mode: str = Field(default="svg", description="Image mode: 'svg' for LLM-generated vector, 'photo' for AI photo-realistic image")


class GenerateMerchantCreativeRequest(BaseModel):
    golden_id: str = Field(..., description="Merchant golden ID for profile lookup")
    segment: Optional[str] = None
    objective: Optional[str] = None
    tone: Optional[str] = Field(default="Personal and consultative")


class GenerateVariantsRequest(BaseModel):
    segment: str = Field(..., description="Target segment")
    channel: str = Field(default="email")
    objective: Optional[str] = None
    num_variants: int = Field(default=3, ge=2, le=4, description="Number of A/B variants")


# ── Campaign ROI ───────────────────────────────────────────────

class CampaignROISummary(BaseModel):
    campaign_type: str = "unknown"
    channel: str = "unknown"
    merchants_targeted: int = 0
    conversions: int = 0
    conversion_rate: float = 0
    total_post_revenue: float = 0
    reactivations: int = 0


class CampaignOutcome(BaseModel):
    campaign_outcome: str = "unknown"
    merchant_count: int = 0
    avg_revenue: float = 0
    avg_health: float = 0


# ── Audience Activation ───────────────────────────────────────

class AudienceSummary(BaseModel):
    churn_risk: int = 0
    high_value: int = 0
    new_onboarding: int = 0
    winback: int = 0
    growth: int = 0
    vip: int = 0
    immediate_action: int = 0
    total_activatable: int = 0


class AudienceMember(BaseModel):
    golden_id: str
    merchant_name: str = ""
    email: Optional[str] = None
    hashed_email: Optional[str] = None
    rfm_segment: str = "unassigned"
    health_score: int = 0
    txn_volume: float = 0
    primary_action: Optional[str] = None
    urgency: Optional[str] = None


# ── Anomaly Alerts ─────────────────────────────────────────────

class AnomalyAlert(BaseModel):
    golden_id: str
    merchant_name: str = ""
    segment: Optional[str] = None
    health_score: int = 0
    health_tier: str = "unknown"
    current_volume: float = 0
    avg_volume_30d: float = 0
    anomaly_type: str = "unknown"
    deviation_pct: float = 0
    recommended_action: Optional[str] = None
    urgency: Optional[str] = None
    estimated_revenue_impact: float = 0
    detected_at: Optional[str] = None


class AnomalyKPIs(BaseModel):
    total_alerts: int = 0
    volume_drops: int = 0
    unexpected_inactivity: int = 0
    ticket_spikes: int = 0
    health_collapses: int = 0
    total_revenue_at_risk: float = 0


# ── Merchant Timeline ──────────────────────────────────────────

class TimelineEvent(BaseModel):
    event_type: str
    event_date: str
    description: str
    detail: Optional[str] = None
    source: str = ""


# ── Data Freshness ─────────────────────────────────────────────

class TableFreshness(BaseModel):
    table_name: str
    last_updated: Optional[str] = None
    row_count: int = 0
    status: str = "unknown"


# ── Agent Chat ─────────────────────────────────────────────────

class ChatMessage(BaseModel):
    role: str = "user"
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]


class GenieRequest(BaseModel):
    question: str


class AgentFeedback(BaseModel):
    message_content: str
    rating: int = Field(ge=1, le=5, description="1=poor 5=excellent")
    comment: Optional[str] = None


# ── Lakebase Operations ────────────────────────────────────────

class CreateCampaignRequest(BaseModel):
    name: str
    segment: str
    action_type: str
    channel: str
    owner: Optional[str] = None
    scheduled_at: Optional[str] = None
    merchant_ids: list[dict] = Field(default_factory=list)
    notes: Optional[str] = None


class CreateAssignmentRequest(BaseModel):
    golden_id: str
    merchant_name: str = ""
    action_type: str
    channel: str = "email"
    assignee: str
    due_date: Optional[str] = None
    priority_score: float = 0
    revenue_impact: float = 0
    notes: Optional[str] = None


class TriageAlertRequest(BaseModel):
    golden_id: str
    merchant_name: str = ""
    anomaly_type: str
    resolution: str = Field(description="open, investigating, resolved, false_positive, escalated")
    triaged_by: Optional[str] = None
    notes: Optional[str] = None


class UpdateStatusRequest(BaseModel):
    status: str
    notes: Optional[str] = None


# ── Data Source Toggle ─────────────────────────────────────────

class DataSourceConfig(BaseModel):
    model_config = {"populate_by_name": True}
    source: str = Field(description="Current data source: 'databricks' or 'mock'")
    catalog: str = Field(description="Unity Catalog name")
    schema_name: str = Field(alias="schema", description="Schema name")
    warehouse_connected: bool = Field(description="Whether the warehouse is reachable")
    lakebase_connected: bool = Field(default=False, description="Whether Lakebase is reachable")
