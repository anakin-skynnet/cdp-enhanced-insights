"""
SQL warehouse data layer for the CDP application.
Uses the Databricks SDK Statement Execution API (no extra dependencies).
Features: TTL caching, retry logic.
"""

import os
import re
import time
import logging
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

logger = logging.getLogger(__name__)

_w: WorkspaceClient | None = None
_WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")


def _client() -> WorkspaceClient:
    global _w
    if _w is None:
        _w = WorkspaceClient()
        logger.info("SDK client initialised, warehouse=%s", _WAREHOUSE_ID)
    return _w


# ── TTL Cache ────────────────────────────────────────────────────

_cache: dict[str, tuple[float, any]] = {}
_CACHE_TTL = int(os.environ.get("CDP_CACHE_TTL", "120"))


def _cached(key: str):
    hit = _cache.get(key)
    if hit and (time.time() - hit[0]) < _CACHE_TTL:
        return hit[1]
    return None


def _set_cache(key: str, value):
    _cache[key] = (time.time(), value)


# ── Named-param substitution ────────────────────────────────────

def _inline_params(sql: str, params: dict | None) -> str:
    """Replace :name placeholders with literal values (safe for our internal queries)."""
    if not params:
        return sql
    def _repl(m):
        key = m.group(1)
        val = params.get(key, m.group(0))
        if val is None:
            return "NULL"
        return "'" + str(val).replace("'", "''") + "'"
    return re.sub(r":(\w+)", _repl, sql)


# ── Query helpers with retry ─────────────────────────────────────

_MAX_RETRIES = 2


def query(sql: str, params: dict | None = None) -> list[dict]:
    statement = _inline_params(sql, params)
    w = _client()
    for attempt in range(_MAX_RETRIES + 1):
        try:
            resp = w.statement_execution.execute_statement(
                statement=statement,
                warehouse_id=_WAREHOUSE_ID,
                wait_timeout="50s",
            )
            if resp.status and resp.status.state == StatementState.FAILED:
                raise RuntimeError(resp.status.error.message if resp.status.error else "SQL failed")
            columns = [c.name for c in (resp.manifest.schema.columns or [])]
            rows: list[dict] = []
            if resp.result and resp.result.data_array:
                for row in resp.result.data_array:
                    rows.append(dict(zip(columns, row)))
            return rows
        except Exception as e:
            if attempt < _MAX_RETRIES and "timeout" in str(e).lower():
                logger.warning("Query retry %d: %s", attempt + 1, e)
                time.sleep(1)
                continue
            logger.error("SQL error: %s\n%s", e, statement[:300])
            raise


def query_one(sql: str, params: dict | None = None) -> dict | None:
    rows = query(sql, params)
    return rows[0] if rows else None


# ── Canned queries ──────────────────────────────────────────────

CATALOG = os.environ.get("CDP_CATALOG", "ahs_demos_catalog")
SCHEMA = os.environ.get("CDP_SCHEMA", "cdp_360")


def _t(table: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{table}"


def get_dashboard_kpis() -> dict:
    cached = _cached("dashboard_kpis")
    if cached:
        return cached
    result = query_one(f"""
        SELECT
          COUNT(DISTINCT c.golden_id) AS total_merchants,
          COALESCE(SUM(e.txn_volume), 0) AS total_volume,
          COALESCE(ROUND(AVG(e.txn_volume), 2), 0) AS avg_volume,
          COALESCE(SUM(e.ticket_count), 0) AS total_tickets,
          COALESCE(ROUND(AVG(e.days_since_last_txn), 0), 0) AS avg_recency,
          COUNT(DISTINCT CASE WHEN s.segment IN ('at_risk','cant_lose','hibernating')
                THEN c.golden_id END) AS at_risk_merchants,
          COALESCE(ROUND(AVG(h.health_score), 1), 0) AS avg_health_score
        FROM {_t('gold_customer_360')} c
        LEFT JOIN {_t('gold_engagement_metrics')} e ON c.golden_id = e.golden_id
        LEFT JOIN {_t('gold_segments')} s ON c.golden_id = s.golden_id
        LEFT JOIN {_t('gold_health_score')} h ON c.golden_id = h.golden_id
    """) or {}
    _set_cache("dashboard_kpis", result)
    return result


def get_segment_distribution() -> list[dict]:
    cached = _cached("segment_dist")
    if cached:
        return cached
    result = query(f"""
        SELECT
          COALESCE(s.segment, 'unassigned') AS segment,
          COUNT(DISTINCT c.golden_id) AS merchant_count,
          COALESCE(SUM(e.txn_volume), 0) AS total_volume
        FROM {_t('gold_customer_360')} c
        LEFT JOIN {_t('gold_engagement_metrics')} e ON c.golden_id = e.golden_id
        LEFT JOIN {_t('gold_segments')} s ON c.golden_id = s.golden_id
        GROUP BY 1 ORDER BY total_volume DESC
    """)
    _set_cache("segment_dist", result)
    return result


def get_health_distribution() -> list[dict]:
    return query(f"""
        SELECT health_tier, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(health_score), 1), 0) AS avg_score
        FROM {_t('gold_health_score')}
        GROUP BY health_tier
        ORDER BY CASE health_tier
          WHEN 'critical' THEN 1 WHEN 'poor' THEN 2
          WHEN 'fair' THEN 3 WHEN 'good' THEN 4 ELSE 5 END
    """)


def get_merchants(
    search: str = "",
    segment: str = "",
    health_tier: str = "",
    sort: str = "txn_volume",
    limit: int = 50,
    offset: int = 0,
) -> list[dict]:
    where = ["1=1"]
    params: dict = {}
    if search:
        where.append(
            "(LOWER(c.first_name) LIKE :search_pat "
            "OR LOWER(c.email) LIKE :search_pat "
            "OR LOWER(COALESCE(c.merchant_name,'')) LIKE :search_pat "
            "OR c.golden_id = :search_exact)"
        )
        params["search_pat"] = f"%{search.lower().strip()}%"
        params["search_exact"] = search.strip()
    if segment:
        where.append("s.segment = :segment")
        params["segment"] = segment.strip()
    if health_tier:
        where.append("h.health_tier = :health_tier")
        params["health_tier"] = health_tier.strip()

    allowed_sorts = {
        "txn_volume": "e.txn_volume DESC NULLS LAST",
        "health_score": "h.health_score ASC NULLS LAST",
        "days_inactive": "e.days_since_last_txn DESC NULLS LAST",
        "tickets": "e.ticket_count DESC NULLS LAST",
    }
    order = allowed_sorts.get(sort, "e.txn_volume DESC NULLS LAST")
    limit = min(int(limit), 200)
    offset = max(int(offset), 0)

    return query(f"""
        SELECT
          c.golden_id,
          COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
          c.email,
          COALESCE(c.industry, '') AS industry,
          COALESCE(c.country, '') AS country,
          COALESCE(s.segment,'unassigned') AS segment,
          COALESCE(h.health_score, 0) AS health_score,
          COALESCE(h.health_tier, 'unknown') AS health_tier,
          COALESCE(e.txn_volume, 0) AS txn_volume,
          COALESCE(e.txn_count, 0) AS txn_count,
          COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
          COALESCE(e.ticket_count, 0) AS ticket_count,
          COALESCE(s.r_score, 0) AS r_score,
          COALESCE(s.f_score, 0) AS f_score,
          COALESCE(s.m_score, 0) AS m_score
        FROM {_t('gold_customer_360')} c
        LEFT JOIN {_t('gold_engagement_metrics')} e ON c.golden_id = e.golden_id
        LEFT JOIN {_t('gold_segments')} s ON c.golden_id = s.golden_id
        LEFT JOIN {_t('gold_health_score')} h ON c.golden_id = h.golden_id
        WHERE {' AND '.join(where)}
        ORDER BY {order}
        LIMIT {limit} OFFSET {offset}
    """, params or None)


def get_merchant_detail(golden_id: str) -> dict | None:
    return query_one(f"""
        SELECT
          c.golden_id,
          COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
          c.email, c.phone,
          COALESCE(c.industry, '') AS industry,
          COALESCE(c.country, '') AS country,
          COALESCE(c.city, '') AS city,
          COALESCE(s.segment,'unassigned') AS segment,
          COALESCE(h.health_score, 0) AS health_score,
          COALESCE(h.health_tier, 'unknown') AS health_tier,
          COALESCE(h.recency_score, 0) AS recency_score,
          COALESCE(h.frequency_score, 0) AS frequency_score,
          COALESCE(h.monetary_score, 0) AS monetary_score,
          COALESCE(h.support_penalty, 0) AS support_penalty,
          COALESCE(h.tenure_bonus, 0) AS tenure_bonus,
          COALESCE(e.txn_volume, 0) AS txn_volume,
          COALESCE(e.txn_count, 0) AS txn_count,
          COALESCE(e.days_since_last_txn, -1) AS days_since_last_txn,
          COALESCE(e.ticket_count, 0) AS ticket_count,
          COALESCE(e.tenure_days, 0) AS tenure_days,
          COALESCE(s.r_score, 0) AS r_score,
          COALESCE(s.f_score, 0) AS f_score,
          COALESCE(s.m_score, 0) AS m_score,
          n.primary_action, n.secondary_action,
          n.primary_channel, n.urgency,
          COALESCE(n.priority_score, 0) AS priority_score,
          COALESCE(n.estimated_revenue_impact, 0) AS estimated_revenue_impact
        FROM {_t('gold_customer_360')} c
        LEFT JOIN {_t('gold_engagement_metrics')} e ON c.golden_id = e.golden_id
        LEFT JOIN {_t('gold_segments')} s ON c.golden_id = s.golden_id
        LEFT JOIN {_t('gold_health_score')} h ON c.golden_id = h.golden_id
        LEFT JOIN {_t('gold_next_best_actions')} n ON c.golden_id = n.golden_id
        WHERE c.golden_id = :golden_id
    """, {"golden_id": golden_id.strip()})


def get_nba_queue(
    urgency: str = "",
    segment: str = "",
    limit: int = 50,
) -> list[dict]:
    where = ["1=1"]
    params: dict = {}
    if urgency:
        where.append("urgency = :urgency")
        params["urgency"] = urgency.strip()
    if segment:
        where.append("segment = :segment")
        params["segment"] = segment.strip()
    return query(f"""
        SELECT golden_id, merchant_name, email, segment,
               health_score, health_tier,
               primary_action, secondary_action,
               primary_channel, urgency, priority_score,
               ROUND(estimated_revenue_impact, 0) AS estimated_revenue_impact,
               txn_volume, days_since_last_txn, ticket_count
        FROM {_t('gold_next_best_actions')}
        WHERE {' AND '.join(where)}
        ORDER BY priority_score DESC
        LIMIT {min(int(limit), 200)}
    """, params or None)


def get_nba_summary() -> list[dict]:
    return query(f"""
        SELECT primary_action,
               COUNT(*) AS merchant_count,
               COALESCE(ROUND(SUM(estimated_revenue_impact), 0), 0) AS revenue_impact,
               COALESCE(ROUND(AVG(health_score), 1), 0) AS avg_health,
               COUNT(CASE WHEN urgency = 'immediate' THEN 1 END) AS immediate,
               COUNT(CASE WHEN urgency = 'this_week' THEN 1 END) AS this_week
        FROM {_t('gold_next_best_actions')}
        GROUP BY primary_action ORDER BY revenue_impact DESC
    """)


def get_segment_merchants_for_campaign(segment: str, limit: int = 100) -> list[dict]:
    return query(f"""
        SELECT c.golden_id,
               COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
               c.email, COALESCE(e.txn_volume, 0) AS txn_volume,
               COALESCE(h.health_score, 0) AS health_score,
               n.primary_action
        FROM {_t('gold_customer_360')} c
        JOIN {_t('gold_segments')} s ON c.golden_id = s.golden_id
        LEFT JOIN {_t('gold_engagement_metrics')} e ON c.golden_id = e.golden_id
        LEFT JOIN {_t('gold_health_score')} h ON c.golden_id = h.golden_id
        LEFT JOIN {_t('gold_next_best_actions')} n ON c.golden_id = n.golden_id
        WHERE s.segment = :segment
        ORDER BY e.txn_volume DESC NULLS LAST
        LIMIT {min(int(limit), 500)}
    """, {"segment": segment.strip()})


def get_clv_summary() -> list[dict]:
    return query(f"""
        SELECT clv_tier, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(clv_12m), 2), 0) AS avg_clv,
               COALESCE(ROUND(SUM(clv_12m), 0), 0) AS total_clv,
               COALESCE(ROUND(AVG(p_alive), 3), 0) AS avg_p_alive
        FROM {_t('gold_customer_ltv')}
        GROUP BY clv_tier ORDER BY avg_clv DESC
    """)


def get_clv_top_merchants(limit: int = 20) -> list[dict]:
    return query(f"""
        SELECT l.golden_id,
               COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
               COALESCE(l.clv_12m, 0) AS clv_12m, COALESCE(l.clv_tier, '') AS clv_tier,
               COALESCE(l.p_alive, 0) AS p_alive,
               COALESCE(l.predicted_purchases_12m, 0) AS predicted_purchases_12m,
               COALESCE(l.total_amount, 0) AS total_amount
        FROM {_t('gold_customer_ltv')} l
        LEFT JOIN {_t('gold_customer_360')} c ON l.golden_id = c.golden_id
        ORDER BY l.clv_12m DESC LIMIT {min(int(limit), 100)}
    """)


def get_channel_attribution() -> list[dict]:
    return query(f"""
        SELECT channel, markov_attributed_value, markov_attribution_share,
               first_touch_value, last_touch_value, linear_value, time_decay_value
        FROM {_t('gold_channel_attribution')}
        ORDER BY markov_attributed_value DESC
    """)


def get_behavioral_segments() -> list[dict]:
    return query(f"""
        SELECT behavioral_segment, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(health_score), 1), 0) AS avg_health,
               COALESCE(ROUND(AVG(txn_volume), 0), 0) AS avg_volume,
               COALESCE(ROUND(AVG(ticket_count), 1), 0) AS avg_tickets,
               COALESCE(ROUND(AVG(days_since_last_txn), 0), 0) AS avg_recency
        FROM {_t('gold_behavioral_segments')}
        GROUP BY behavioral_segment ORDER BY avg_volume DESC
    """)


def log_campaign(campaign: dict) -> None:
    action_type = str(campaign.get("action_type", ""))[:100]
    channel = str(campaign.get("channel", ""))[:100]
    name = str(campaign.get("name", ""))[:200]
    for m in campaign.get("merchants", []):
        query(f"""
            INSERT INTO {_t('nba_action_log')}
              (action_id, golden_id, action_type, channel, executed_by, notes, executed_at)
            VALUES (uuid(), :gid, :action, :channel, 'cdp_app', :notes, CURRENT_TIMESTAMP())
        """, {
            "gid": str(m["golden_id"]).strip(),
            "action": action_type,
            "channel": channel,
            "notes": name,
        })


# ── Customer Support Analytics ─────────────────────────────────────

def get_support_kpis() -> dict:
    return query_one(f"""
        SELECT
          COUNT(*) AS merchants_with_tickets,
          COALESCE(SUM(total_tickets), 0) AS total_tickets,
          COALESCE(SUM(open_tickets), 0) AS open_tickets,
          COALESCE(ROUND(AVG(avg_first_response_min), 1), 0) AS avg_first_response_min,
          COALESCE(ROUND(AVG(avg_resolution_min), 1), 0) AS avg_resolution_min,
          COALESCE(ROUND(AVG(csat_score), 1), 0) AS overall_csat,
          COALESCE(ROUND(AVG(sla_first_response_pct), 1), 0) AS sla_first_response_pct,
          COALESCE(ROUND(AVG(sla_resolution_pct), 1), 0) AS sla_resolution_pct
        FROM {_t('gold_support_analytics')}
    """) or {}


def get_support_quality_distribution() -> list[dict]:
    return query(f"""
        SELECT support_quality_tier, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(avg_resolution_min), 0), 0) AS avg_resolution,
               COALESCE(ROUND(AVG(csat_score), 1), 0) AS avg_csat
        FROM {_t('gold_support_analytics')}
        GROUP BY support_quality_tier
        ORDER BY CASE support_quality_tier
          WHEN 'excellent' THEN 1 WHEN 'good' THEN 2
          WHEN 'fair' THEN 3 ELSE 4 END
    """)


def get_support_merchants(quality: str = "", limit: int = 50) -> list[dict]:
    where = "1=1"
    params: dict = {}
    if quality:
        where = "sa.support_quality_tier = :quality"
        params["quality"] = quality.strip()
    return query(f"""
        SELECT sa.golden_id,
               COALESCE(c.merchant_name, c.first_name, '') AS merchant_name,
               COALESCE(sa.total_tickets, 0) AS total_tickets,
               COALESCE(sa.open_tickets, 0) AS open_tickets,
               COALESCE(sa.resolved_tickets, 0) AS resolved_tickets,
               COALESCE(sa.avg_first_response_min, 0) AS avg_first_response_min,
               COALESCE(sa.avg_resolution_min, 0) AS avg_resolution_min,
               sa.csat_score, sa.sla_resolution_pct, COALESCE(sa.support_quality_tier, 'unknown') AS support_quality_tier
        FROM {_t('gold_support_analytics')} sa
        LEFT JOIN {_t('gold_customer_360')} c ON sa.golden_id = c.golden_id
        WHERE {where}
        ORDER BY sa.total_tickets DESC
        LIMIT {min(int(limit), 200)}
    """, params or None)


# ── Call Center Analytics ──────────────────────────────────────────

def get_call_center_kpis() -> dict:
    return query_one(f"""
        SELECT
          COALESCE(SUM(total_interactions), 0) AS total_interactions,
          COALESCE(SUM(voice_calls), 0) AS total_voice,
          COALESCE(SUM(chat_sessions), 0) AS total_chat,
          COALESCE(ROUND(AVG(avg_handle_time_sec), 0), 0) AS avg_handle_time,
          COALESCE(ROUND(AVG(avg_queue_wait_sec), 0), 0) AS avg_queue_wait,
          COALESCE(ROUND(AVG(resolution_rate), 1), 0) AS avg_resolution_rate,
          COALESCE(ROUND(AVG(abandonment_rate), 1), 0) AS avg_abandonment_rate
        FROM {_t('gold_call_center_analytics')}
        WHERE metric_type = 'agent'
    """) or {}


def get_call_center_agents(limit: int = 20) -> list[dict]:
    return query(f"""
        SELECT entity_id AS agent_id,
               COALESCE(total_interactions, 0) AS total_interactions,
               COALESCE(voice_calls, 0) AS voice_calls,
               COALESCE(chat_sessions, 0) AS chat_sessions,
               COALESCE(email_interactions, 0) AS email_interactions,
               COALESCE(avg_handle_time_sec, 0) AS avg_handle_time_sec,
               COALESCE(avg_talk_time_sec, 0) AS avg_talk_time_sec,
               COALESCE(avg_hold_time_sec, 0) AS avg_hold_time_sec,
               COALESCE(avg_queue_wait_sec, 0) AS avg_queue_wait_sec,
               COALESCE(resolution_rate, 0) AS resolution_rate,
               COALESCE(abandonment_rate, 0) AS abandonment_rate
        FROM {_t('gold_call_center_analytics')}
        WHERE metric_type = 'agent'
        ORDER BY total_interactions DESC
        LIMIT {limit}
    """)


def get_call_center_queues() -> list[dict]:
    return query(f"""
        SELECT entity_id AS queue_name,
               COALESCE(total_interactions, 0) AS total_interactions,
               COALESCE(avg_handle_time_sec, 0) AS avg_handle_time_sec,
               COALESCE(avg_queue_wait_sec, 0) AS avg_queue_wait_sec,
               COALESCE(abandonment_rate, 0) AS abandonment_rate,
               COALESCE(service_level_pct, 0) AS service_level_pct
        FROM {_t('gold_call_center_analytics')}
        WHERE metric_type = 'queue'
        ORDER BY total_interactions DESC
    """)


def get_call_center_sentiment() -> list[dict]:
    return query(f"""
        SELECT topic_category, COUNT(*) AS interaction_count,
               COALESCE(ROUND(COUNT(CASE WHEN sentiment = 'positive' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1), 0) AS positive_pct,
               COALESCE(ROUND(COUNT(CASE WHEN sentiment = 'negative' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1), 0) AS negative_pct,
               COALESCE(ROUND(COUNT(CASE WHEN sentiment = 'neutral' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0), 1), 0) AS neutral_pct,
               COALESCE(ROUND(AVG(duration_seconds), 0), 0) AS avg_duration
        FROM {_t('gold_call_center_sentiment')}
        GROUP BY topic_category
        ORDER BY interaction_count DESC
    """)


# ── Hyper-Personalization ─────────────────────────────────────────

def get_personalization_summary() -> list[dict]:
    return query(f"""
        SELECT content_theme, merchant_tier,
               COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(upsell_propensity), 2), 0) AS avg_upsell,
               COALESCE(ROUND(AVG(churn_propensity), 2), 0) AS avg_churn,
               COALESCE(ROUND(AVG(activation_propensity), 2), 0) AS avg_activation
        FROM {_t('gold_personalization_signals')}
        GROUP BY content_theme, merchant_tier
        ORDER BY merchant_count DESC
    """)


def get_personalization_for_merchant(golden_id: str) -> dict:
    return query_one(f"""
        SELECT * FROM {_t('gold_personalization_signals')}
        WHERE golden_id = :golden_id
    """, {"golden_id": golden_id.strip()}) or {}


def get_propensity_distribution() -> list[dict]:
    return query(f"""
        SELECT propensity_tier, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(churn_propensity_score), 3), 0) AS avg_churn,
               COALESCE(ROUND(AVG(upsell_propensity_score), 3), 0) AS avg_upsell,
               COALESCE(ROUND(AVG(activation_propensity_score), 3), 0) AS avg_activation
        FROM {_t('gold_propensity_scores')}
        GROUP BY propensity_tier
        ORDER BY merchant_count DESC
    """)


# ── Ad Creative ───────────────────────────────────────────────────

def get_ad_creative_library() -> list[dict]:
    return query(f"""
        SELECT segment, email_subject, email_body_preview, sms_message,
               push_notification, ad_headline, ad_description, tone, cta,
               COALESCE(merchant_count, 0) AS merchant_count,
               COALESCE(avg_volume, 0) AS avg_volume
        FROM {_t('gold_ad_creative_library')}
        ORDER BY avg_volume DESC
    """)


# ── Campaign ROI ──────────────────────────────────────────────────

def get_campaign_roi_summary() -> list[dict]:
    return query(f"""
        SELECT campaign_type, channel,
               COUNT(*) AS merchants_targeted,
               COALESCE(SUM(converted), 0) AS conversions,
               COALESCE(ROUND(SUM(converted) * 100.0 / NULLIF(COUNT(*), 0), 1), 0) AS conversion_rate,
               COALESCE(ROUND(SUM(post_30d_volume), 0), 0) AS total_post_revenue,
               COUNT(CASE WHEN campaign_outcome = 'reactivated' THEN 1 END) AS reactivations
        FROM {_t('gold_campaign_roi')}
        GROUP BY campaign_type, channel
        ORDER BY total_post_revenue DESC
    """)


def get_campaign_outcome_distribution() -> list[dict]:
    return query(f"""
        SELECT campaign_outcome, COUNT(*) AS merchant_count,
               COALESCE(ROUND(AVG(post_30d_volume), 0), 0) AS avg_revenue,
               COALESCE(ROUND(AVG(current_health_score), 1), 0) AS avg_health
        FROM {_t('gold_campaign_roi')}
        GROUP BY campaign_outcome
        ORDER BY merchant_count DESC
    """)


# ── Audience Activation ──────────────────────────────────────────

def get_audience_summary() -> list[dict]:
    return query(f"""
        SELECT
          SUM(CASE WHEN audience_churn_risk THEN 1 ELSE 0 END) AS churn_risk,
          SUM(CASE WHEN audience_high_value THEN 1 ELSE 0 END) AS high_value,
          SUM(CASE WHEN audience_new_onboarding THEN 1 ELSE 0 END) AS new_onboarding,
          SUM(CASE WHEN audience_winback THEN 1 ELSE 0 END) AS winback,
          SUM(CASE WHEN audience_growth THEN 1 ELSE 0 END) AS growth,
          SUM(CASE WHEN audience_vip THEN 1 ELSE 0 END) AS vip,
          SUM(CASE WHEN audience_immediate_action THEN 1 ELSE 0 END) AS immediate_action,
          COUNT(*) AS total_activatable
        FROM {_t('gold_audience_exports')}
    """)


def get_audience_list(audience_type: str, limit: int = 100) -> list[dict]:
    flag_map = {
        "churn_risk": "audience_churn_risk",
        "high_value": "audience_high_value",
        "new_onboarding": "audience_new_onboarding",
        "winback": "audience_winback",
        "growth": "audience_growth",
        "vip": "audience_vip",
        "immediate_action": "audience_immediate_action",
    }
    flag = flag_map.get(audience_type, "TRUE")
    return query(f"""
        SELECT golden_id, merchant_name, email, hashed_email,
               rfm_segment, health_score, txn_volume,
               primary_action, urgency
        FROM {_t('gold_audience_exports')}
        WHERE {flag}
        ORDER BY txn_volume DESC
        LIMIT {limit}
    """)


# ── Anomaly Alerts ───────────────────────────────────────────────

def get_anomaly_alerts(anomaly_type: str = "", limit: int = 50) -> list[dict]:
    where = "anomaly_type IS NOT NULL"
    params: dict = {}
    if anomaly_type:
        where += " AND anomaly_type = :atype"
        params["atype"] = anomaly_type.strip()
    return query(f"""
        SELECT golden_id, COALESCE(merchant_name, '') AS merchant_name,
               COALESCE(segment, '') AS segment,
               COALESCE(health_score, 0) AS health_score,
               COALESCE(health_tier, 'unknown') AS health_tier,
               COALESCE(current_volume, 0) AS current_volume,
               COALESCE(avg_volume_30d, 0) AS avg_volume_30d,
               anomaly_type,
               COALESCE(deviation_pct, 0) AS deviation_pct,
               COALESCE(recommended_action, '') AS recommended_action,
               COALESCE(urgency, '') AS urgency,
               COALESCE(estimated_revenue_impact, 0) AS estimated_revenue_impact,
               detected_at
        FROM {_t('gold_anomaly_alerts')}
        WHERE {where}
        ORDER BY ABS(deviation_pct) DESC
        LIMIT {min(int(limit), 200)}
    """, params or None)


def get_anomaly_kpis() -> dict:
    cached = _cached("anomaly_kpis")
    if cached:
        return cached
    result = query_one(f"""
        SELECT
          COUNT(*) AS total_alerts,
          COUNT(CASE WHEN anomaly_type = 'volume_drop' THEN 1 END) AS volume_drops,
          COUNT(CASE WHEN anomaly_type = 'unexpected_inactivity' THEN 1 END) AS unexpected_inactivity,
          COUNT(CASE WHEN anomaly_type = 'ticket_spike' THEN 1 END) AS ticket_spikes,
          COUNT(CASE WHEN anomaly_type = 'health_collapse' THEN 1 END) AS health_collapses,
          COALESCE(ROUND(SUM(estimated_revenue_impact), 0), 0) AS total_revenue_at_risk
        FROM {_t('gold_anomaly_alerts')}
        WHERE anomaly_type IS NOT NULL
    """) or {}
    _set_cache("anomaly_kpis", result)
    return result


# ── Merchant Timeline ────────────────────────────────────────────

def get_merchant_timeline(golden_id: str, limit: int = 30) -> list[dict]:
    params = {"gid": golden_id.strip()}
    return query(f"""
        WITH txn_events AS (
          SELECT golden_id, 'transaction' AS event_type,
                 DATE_FORMAT(COALESCE(last_txn_date, _refreshed_at), 'yyyy-MM-dd') AS event_date,
                 CONCAT('Transaction volume: $', ROUND(txn_volume, 0)) AS description,
                 CONCAT(txn_count, ' transactions') AS detail,
                 'engagement' AS source
          FROM {_t('gold_engagement_metrics')}
          WHERE golden_id = :gid
        ),
        ticket_events AS (
          SELECT sa.golden_id, 'support_ticket' AS event_type,
                 DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS event_date,
                 CONCAT(sa.open_tickets, ' open / ', sa.resolved_tickets, ' resolved tickets') AS description,
                 CONCAT('CSAT: ', ROUND(sa.csat_score, 1)) AS detail,
                 'support' AS source
          FROM {_t('gold_support_analytics')} sa
          WHERE sa.golden_id = :gid
        ),
        nba_events AS (
          SELECT golden_id, 'nba_recommendation' AS event_type,
                 DATE_FORMAT(CURRENT_DATE(), 'yyyy-MM-dd') AS event_date,
                 CONCAT('Action: ', primary_action) AS description,
                 CONCAT('Channel: ', primary_channel, ' | Urgency: ', urgency) AS detail,
                 'nba' AS source
          FROM {_t('gold_next_best_actions')}
          WHERE golden_id = :gid
        ),
        action_events AS (
          SELECT golden_id, 'action_taken' AS event_type,
                 DATE_FORMAT(executed_at, 'yyyy-MM-dd') AS event_date,
                 CONCAT('Executed: ', action_type, ' via ', channel) AS description,
                 COALESCE(notes, '') AS detail,
                 'action_log' AS source
          FROM {_t('nba_action_log')}
          WHERE golden_id = :gid
        ),
        anomaly_events AS (
          SELECT golden_id, 'anomaly' AS event_type,
                 DATE_FORMAT(detected_at, 'yyyy-MM-dd') AS event_date,
                 CONCAT('Anomaly: ', anomaly_type) AS description,
                 CONCAT('Deviation: ', ROUND(deviation_pct, 1), '%') AS detail,
                 'anomaly' AS source
          FROM {_t('gold_anomaly_alerts')}
          WHERE golden_id = :gid AND anomaly_type IS NOT NULL
        )
        SELECT * FROM txn_events
        UNION ALL SELECT * FROM ticket_events
        UNION ALL SELECT * FROM nba_events
        UNION ALL SELECT * FROM action_events
        UNION ALL SELECT * FROM anomaly_events
        ORDER BY event_date DESC
        LIMIT {min(int(limit), 100)}
    """, params)


# ── Data Freshness ───────────────────────────────────────────────

_GOLD_TABLES = [
    "gold_customer_360", "gold_engagement_metrics", "gold_segments",
    "gold_health_score", "gold_next_best_actions", "gold_customer_ltv",
    "gold_channel_attribution", "gold_behavioral_segments",
    "gold_support_analytics", "gold_call_center_analytics",
    "gold_call_center_sentiment", "gold_personalization_signals",
    "gold_propensity_scores", "gold_ad_creative_library",
    "gold_campaign_roi", "gold_audience_exports", "gold_anomaly_alerts",
]


def get_data_freshness() -> list[dict]:
    cached = _cached("data_freshness")
    if cached:
        return cached
    union = " UNION ALL ".join(
        f"(SELECT '{t}' AS table_name FROM {_t(t)} LIMIT 1)" for t in _GOLD_TABLES
    )
    rows = query(f"""
        WITH tables AS ({union})
        SELECT t.table_name,
               COALESCE(i.last_altered, i.created) AS last_updated
        FROM tables t
        LEFT JOIN {CATALOG}.information_schema.tables i
          ON i.table_catalog = '{CATALOG}'
          AND i.table_schema = '{SCHEMA}'
          AND i.table_name = t.table_name
    """)
    result = []
    for r in rows:
        status = "healthy" if r.get("last_updated") else "unknown"
        result.append({**r, "status": status})
    _set_cache("data_freshness", result)
    return result


# ── Agent Feedback ───────────────────────────────────────────────

def log_agent_feedback(feedback: dict) -> dict:
    rating = max(1, min(5, int(feedback.get("rating", 3))))
    query(f"""
        INSERT INTO {_t('agent_feedback_log')}
          (feedback_id, message_content, rating, comment, created_at)
        VALUES (uuid(), :msg, :rating, :comment, CURRENT_TIMESTAMP())
    """, {
        "msg": str(feedback.get("message_content", ""))[:500],
        "rating": rating,
        "comment": str(feedback.get("comment", ""))[:500],
    })
    return {"status": "recorded"}
