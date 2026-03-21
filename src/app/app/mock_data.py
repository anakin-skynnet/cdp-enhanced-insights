"""
Mock data provider for offline development and demos.
Generates realistic Bank Payment Platform merchant data without a Databricks connection.
Toggle via CDP_DATA_SOURCE=mock environment variable.
"""

from __future__ import annotations

import hashlib
import random
import string
from datetime import datetime, timedelta

random.seed(42)

SEGMENTS = ["champions", "loyal", "potential_loyalists", "new_customers",
            "promising", "at_risk", "cant_lose", "hibernating", "need_attention"]
HEALTH_TIERS = ["critical", "poor", "fair", "good", "excellent"]
URGENCIES = ["immediate", "this_week", "this_month", "next_cycle"]
ACTIONS = ["executive_outreach", "premium_win_back", "win_back_campaign",
           "reengagement_offer", "loyalty_reward", "referral_program",
           "upsell_premium_plan", "cross_sell_products", "onboarding_nurture",
           "activation_incentive", "product_education", "growth_acceleration"]
CHANNELS = ["sfmc_email", "sfmc_journey", "zender_sms", "phone",
            "app_push", "phone_inperson"]
TOPICS = ["billing", "technical_issue", "onboarding", "account_inquiry",
          "complaint", "fraud", "feature_request", "cancellation", "positive_feedback"]

FIRST_NAMES = [
    "Café del Sol", "Restaurante El Gaucho", "Tienda La Esperanza",
    "Farmacia San Martín", "Supermercado El Progreso", "Panadería La Rosa",
    "Hotel Vista Mar", "Taller Mecánico López", "Librería Cultura",
    "Pizzería Napoli", "Carnicería Don Juan", "Óptica Visión",
    "Peluquería Estilo", "Ferretería Central", "Zapatería El Paso",
    "Clínica Salud Plus", "Gimnasio Fitness Pro", "Lavandería Express",
    "Joyería Brillante", "Floristería Primavera", "Heladería Polar",
    "Verdulería Fresca", "Estación de Servicio Central", "Kiosco 24hs",
    "Bar La Esquina", "Mercado Municipal", "Veterinaria Animal",
    "Electrónica Digital", "Imprenta Gráfica", "Centro Educativo ABC",
    "Spa Relax", "Agencia de Viajes Sol", "Mueblería Hogar",
    "Automotriz Premium", "Construcciones del Sur", "Textil Norte",
    "Bodega Don Pedro", "Pastelería Dulce", "Rotisería Express",
    "Juguetería Feliz",
]


def _gid(i: int) -> str:
    return f"GID-{str(i).zfill(6)}"


def _email(name: str) -> str:
    slug = name.lower().replace(" ", ".").replace("á", "a").replace("é", "e") \
        .replace("í", "i").replace("ó", "o").replace("ú", "u").replace("ñ", "n")
    slug = "".join(c for c in slug if c.isalnum() or c == ".")
    return f"{slug[:20]}@bankplatform.com"


def _phone() -> str:
    return f"+54 11 {random.randint(4000,9999)}-{random.randint(1000,9999)}"


def _hash_email(email: str) -> str:
    return hashlib.sha256(email.lower().strip().encode()).hexdigest()


_MERCHANT_COUNT = 120

_merchants: list[dict] | None = None


def _build_merchants() -> list[dict]:
    global _merchants
    if _merchants is not None:
        return _merchants

    merchants = []
    for i in range(1, _MERCHANT_COUNT + 1):
        name = random.choice(FIRST_NAMES) + f" #{i}"
        seg = random.choices(
            SEGMENTS,
            weights=[8, 15, 12, 10, 8, 15, 7, 12, 13],
        )[0]
        txn_count = random.randint(1, 500)
        txn_volume = round(random.uniform(500, 250000), 2)
        days_inactive = random.randint(0, 180)
        ticket_count = random.randint(0, 30)
        health = _health_for(seg, days_inactive, txn_volume, ticket_count)
        r = random.randint(1, 5)
        f = random.randint(1, 5)
        m = random.randint(1, 5)
        email = _email(name)
        action_idx = random.randint(0, len(ACTIONS) - 1)

        industries = ["Retail", "Food & Beverage", "E-commerce", "Healthcare", "Travel", "Entertainment", "Technology", "Financial Services"]
        countries = ["Brazil", "Mexico", "Argentina", "Chile", "Colombia"]
        cities = ["São Paulo", "Mexico City", "Buenos Aires", "Santiago", "Bogotá", "Rio de Janeiro", "Monterrey", "Córdoba"]
        merchants.append({
            "golden_id": _gid(i),
            "merchant_name": name,
            "email": email,
            "phone": _phone(),
            "industry": random.choice(industries),
            "country": random.choice(countries),
            "city": random.choice(cities),
            "segment": seg,
            "health_score": health["score"],
            "health_tier": health["tier"],
            "recency_score": health["recency_score"],
            "frequency_score": health["frequency_score"],
            "monetary_score": health["monetary_score"],
            "support_penalty": health["support_penalty"],
            "tenure_bonus": health["tenure_bonus"],
            "txn_volume": txn_volume,
            "txn_count": txn_count,
            "days_since_last_txn": days_inactive,
            "ticket_count": ticket_count,
            "tenure_days": random.randint(30, 1200),
            "r_score": r,
            "f_score": f,
            "m_score": m,
            "primary_action": ACTIONS[action_idx],
            "secondary_action": ACTIONS[(action_idx + 1) % len(ACTIONS)],
            "primary_channel": random.choice(CHANNELS),
            "urgency": random.choice(URGENCIES),
            "priority_score": round(random.uniform(10, 100), 1),
            "estimated_revenue_impact": round(random.uniform(500, 50000), 0),
            "hashed_email": _hash_email(email),
        })
    _merchants = merchants
    return _merchants


def _health_for(seg, days_inactive, vol, tickets):
    rec = max(0, min(30, 30 - days_inactive // 5))
    freq = min(25, int(vol / 5000))
    mon = min(25, int(vol / 10000))
    sup = min(15, tickets)
    ten = random.randint(0, 10)
    score = max(0, min(100, rec + freq + mon - sup + ten))
    tier = ("excellent" if score >= 75 else "good" if score >= 55
            else "fair" if score >= 35 else "poor" if score >= 15 else "critical")
    return {"score": score, "tier": tier, "recency_score": rec,
            "frequency_score": freq, "monetary_score": mon,
            "support_penalty": -sup, "tenure_bonus": ten}


# ═══════════════════════════════════════════════════════════════
# PUBLIC API - mirrors db.py function signatures
# ═══════════════════════════════════════════════════════════════

def get_dashboard_kpis() -> dict:
    ms = _build_merchants()
    at_risk = [m for m in ms if m["segment"] in ("at_risk", "cant_lose", "hibernating")]
    return {
        "total_merchants": len(ms),
        "total_volume": round(sum(m["txn_volume"] for m in ms), 0),
        "avg_volume": round(sum(m["txn_volume"] for m in ms) / len(ms), 2),
        "total_tickets": sum(m["ticket_count"] for m in ms),
        "avg_recency": round(sum(m["days_since_last_txn"] for m in ms) / len(ms)),
        "at_risk_merchants": len(at_risk),
        "avg_health_score": round(sum(m["health_score"] for m in ms) / len(ms), 1),
    }


def get_segment_distribution() -> list[dict]:
    ms = _build_merchants()
    segs: dict[str, dict] = {}
    for m in ms:
        s = m["segment"]
        if s not in segs:
            segs[s] = {"segment": s, "merchant_count": 0, "total_volume": 0}
        segs[s]["merchant_count"] += 1
        segs[s]["total_volume"] += m["txn_volume"]
    return sorted(segs.values(), key=lambda x: -x["total_volume"])


def get_health_distribution() -> list[dict]:
    ms = _build_merchants()
    tiers: dict[str, list] = {}
    for m in ms:
        t = m["health_tier"]
        tiers.setdefault(t, []).append(m["health_score"])
    order = {"critical": 1, "poor": 2, "fair": 3, "good": 4, "excellent": 5}
    return sorted([
        {"health_tier": t, "merchant_count": len(scores),
         "avg_score": round(sum(scores) / len(scores), 1)}
        for t, scores in tiers.items()
    ], key=lambda x: order.get(x["health_tier"], 9))


def get_merchants(search="", segment="", health_tier="", sort="txn_volume",
                  limit=50, offset=0) -> list[dict]:
    ms = _build_merchants()
    if search:
        sl = search.lower()
        ms = [m for m in ms if sl in m["merchant_name"].lower()
              or sl in (m["email"] or "").lower() or m["golden_id"] == search]
    if segment:
        ms = [m for m in ms if m["segment"] == segment]
    if health_tier:
        ms = [m for m in ms if m["health_tier"] == health_tier]
    sort_keys = {
        "txn_volume": lambda m: -m["txn_volume"],
        "health_score": lambda m: m["health_score"],
        "days_inactive": lambda m: -m["days_since_last_txn"],
        "tickets": lambda m: -m["ticket_count"],
    }
    ms = sorted(ms, key=sort_keys.get(sort, sort_keys["txn_volume"]))
    return ms[offset:offset + limit]


def get_merchant_detail(golden_id: str) -> dict | None:
    ms = _build_merchants()
    for m in ms:
        if m["golden_id"] == golden_id:
            return m
    return None


def get_nba_queue(urgency="", segment="", limit=50) -> list[dict]:
    ms = _build_merchants()
    if urgency:
        ms = [m for m in ms if m["urgency"] == urgency]
    if segment:
        ms = [m for m in ms if m["segment"] == segment]
    ms = sorted(ms, key=lambda m: -m["priority_score"])
    return ms[:limit]


def get_nba_summary() -> list[dict]:
    ms = _build_merchants()
    actions: dict[str, dict] = {}
    for m in ms:
        a = m["primary_action"]
        if a not in actions:
            actions[a] = {"primary_action": a, "merchant_count": 0,
                          "revenue_impact": 0, "_healths": [], "immediate": 0, "this_week": 0}
        actions[a]["merchant_count"] += 1
        actions[a]["revenue_impact"] += m["estimated_revenue_impact"]
        actions[a]["_healths"].append(m["health_score"])
        if m["urgency"] == "immediate":
            actions[a]["immediate"] += 1
        if m["urgency"] == "this_week":
            actions[a]["this_week"] += 1
    result = []
    for a in actions.values():
        a["avg_health"] = round(sum(a["_healths"]) / len(a["_healths"]), 1) if a["_healths"] else 0
        a["revenue_impact"] = round(a["revenue_impact"])
        del a["_healths"]
        result.append(a)
    return sorted(result, key=lambda x: -x["revenue_impact"])


def get_segment_merchants_for_campaign(segment: str, limit=100) -> list[dict]:
    ms = _build_merchants()
    ms = [m for m in ms if m["segment"] == segment]
    ms = sorted(ms, key=lambda m: -m["txn_volume"])
    return [{k: m[k] for k in ("golden_id", "merchant_name", "email",
             "txn_volume", "health_score", "primary_action")} for m in ms[:limit]]


def get_clv_summary() -> list[dict]:
    tiers = ["very_high", "high", "medium", "low", "very_low"]
    return [
        {"clv_tier": t, "merchant_count": random.randint(10, 40),
         "avg_clv": round(random.uniform(500, 50000), 2),
         "total_clv": round(random.uniform(10000, 500000), 0),
         "avg_p_alive": round(random.uniform(0.3, 0.95), 3)}
        for t in tiers
    ]


def get_clv_top_merchants(limit=20) -> list[dict]:
    ms = _build_merchants()[:limit]
    return [
        {"golden_id": m["golden_id"], "merchant_name": m["merchant_name"],
         "clv_12m": round(random.uniform(2000, 80000), 2),
         "clv_tier": random.choice(["very_high", "high", "medium"]),
         "p_alive": round(random.uniform(0.5, 0.99), 3),
         "predicted_purchases_12m": round(random.uniform(5, 50), 1),
         "total_amount": m["txn_volume"]}
        for m in ms
    ]


def get_channel_attribution() -> list[dict]:
    channels = ["sfmc_email", "google_ads", "meta_ads", "zender_sms",
                "platform_app", "referral", "organic"]
    total = 1000000
    result = []
    for ch in channels:
        val = round(random.uniform(50000, 250000), 0)
        result.append({
            "channel": ch,
            "markov_attributed_value": val,
            "markov_attribution_share": round(val / total, 3),
            "first_touch_value": round(val * random.uniform(0.7, 1.3), 0),
            "last_touch_value": round(val * random.uniform(0.7, 1.3), 0),
            "linear_value": round(val * random.uniform(0.8, 1.2), 0),
            "time_decay_value": round(val * random.uniform(0.8, 1.2), 0),
        })
    return sorted(result, key=lambda x: -x["markov_attributed_value"])


def get_behavioral_segments() -> list[dict]:
    names = ["high_value_stable", "growth_potential", "price_sensitive",
             "at_risk_heavy_users", "new_low_engagement", "dormant"]
    return [
        {"behavioral_segment": n, "merchant_count": random.randint(10, 35),
         "avg_health": round(random.uniform(20, 85), 1),
         "avg_volume": round(random.uniform(5000, 150000), 0),
         "avg_tickets": round(random.uniform(0.5, 8), 1),
         "avg_recency": random.randint(3, 90)}
        for n in names
    ]


def log_campaign(campaign: dict) -> None:
    pass  # no-op in mock


def get_support_kpis() -> dict:
    return {
        "merchants_with_tickets": 87,
        "total_tickets": 1243,
        "open_tickets": 156,
        "avg_first_response_min": 42.3,
        "avg_resolution_min": 387.5,
        "overall_csat": 78.2,
        "sla_first_response_pct": 89.1,
        "sla_resolution_pct": 72.4,
    }


def get_support_quality_distribution() -> list[dict]:
    return [
        {"support_quality_tier": "excellent", "merchant_count": 22, "avg_resolution": 85, "avg_csat": 94.2},
        {"support_quality_tier": "good", "merchant_count": 31, "avg_resolution": 320, "avg_csat": 82.1},
        {"support_quality_tier": "fair", "merchant_count": 20, "avg_resolution": 890, "avg_csat": 68.5},
        {"support_quality_tier": "poor", "merchant_count": 14, "avg_resolution": 2100, "avg_csat": 45.0},
    ]


def get_support_merchants(quality="", limit=50) -> list[dict]:
    ms = _build_merchants()[:limit]
    result = []
    for m in ms:
        csat = round(random.uniform(30, 100), 1)
        tier = ("excellent" if csat >= 85 else "good" if csat >= 70
                else "fair" if csat >= 50 else "poor")
        if quality and tier != quality:
            continue
        avg_res = round(random.uniform(60, 600) if csat >= 70 else random.uniform(400, 2500), 1)
        result.append({
            "golden_id": m["golden_id"], "merchant_name": m["merchant_name"],
            "total_tickets": random.randint(1, 40), "open_tickets": random.randint(0, 5),
            "resolved_tickets": random.randint(1, 35),
            "avg_first_response_min": round(random.uniform(10, 60) if csat >= 70 else random.uniform(40, 120), 1),
            "avg_resolution_min": avg_res,
            "csat_score": csat,
            "sla_resolution_pct": round(random.uniform(75, 100) if csat >= 70 else random.uniform(40, 75), 1),
            "support_quality_tier": tier,
        })
    return result[:limit]


def get_call_center_kpis() -> dict:
    return {
        "total_interactions": 8742,
        "total_voice": 5231,
        "total_chat": 2890,
        "avg_handle_time": 342,
        "avg_queue_wait": 28,
        "avg_resolution_rate": 87.3,
        "avg_abandonment_rate": 4.2,
    }


def get_call_center_agents(limit=20) -> list[dict]:
    return [
        {"agent_id": f"AGT-{str(i).zfill(3)}", "total_interactions": random.randint(100, 600),
         "voice_calls": random.randint(50, 400), "chat_sessions": random.randint(20, 200),
         "email_interactions": random.randint(5, 50),
         "avg_handle_time_sec": random.randint(180, 480),
         "avg_talk_time_sec": random.randint(120, 360),
         "avg_hold_time_sec": random.randint(10, 60),
         "avg_queue_wait_sec": random.randint(10, 45),
         "resolution_rate": round(random.uniform(75, 98), 1),
         "abandonment_rate": round(random.uniform(1, 8), 1)}
        for i in range(1, min(limit + 1, 21))
    ]


def get_call_center_queues() -> list[dict]:
    queues = ["billing", "technical", "onboarding", "general", "fraud", "cancellation"]
    return [
        {"queue_name": q, "total_interactions": random.randint(500, 2500),
         "avg_handle_time_sec": random.randint(200, 500),
         "avg_queue_wait_sec": random.randint(10, 60),
         "abandonment_rate": round(random.uniform(2, 10), 1),
         "service_level_pct": round(random.uniform(70, 95), 1)}
        for q in queues
    ]


def get_call_center_sentiment() -> list[dict]:
    result = []
    for t in TOPICS:
        pos = round(random.uniform(20, 55), 1)
        neg = round(random.uniform(5, 25), 1)
        neu = round(100 - pos - neg, 1)
        result.append({
            "topic_category": t, "interaction_count": random.randint(100, 800),
            "positive_pct": pos, "negative_pct": neg, "neutral_pct": neu,
            "avg_duration": random.randint(120, 480),
        })
    return result


def get_personalization_summary() -> list[dict]:
    themes = ["loyalty_reward", "win_back", "onboarding", "activation", "growth", "general"]
    tiers = ["premium", "growth", "standard", "starter"]
    result = []
    for theme in themes:
        for tier in random.sample(tiers, random.randint(1, 3)):
            result.append({
                "content_theme": theme, "merchant_tier": tier,
                "merchant_count": random.randint(5, 25),
                "avg_upsell": round(random.uniform(0.1, 0.9), 2),
                "avg_churn": round(random.uniform(0.05, 0.8), 2),
                "avg_activation": round(random.uniform(0.1, 0.9), 2),
            })
    return result


def get_personalization_for_merchant(golden_id: str) -> dict:
    m = get_merchant_detail(golden_id)
    if not m:
        return {}
    return {**m, "purchase_frequency_pattern": "monthly",
            "preferred_time_slot": "morning", "recommended_channel": "sfmc_email",
            "upsell_propensity": 0.65, "churn_propensity": 0.2,
            "activation_propensity": 0.4, "content_theme": "growth",
            "merchant_tier": "standard"}


def get_propensity_distribution() -> list[dict]:
    return [
        {"propensity_tier": "high_churn_risk", "merchant_count": 18,
         "avg_churn": 0.82, "avg_upsell": 0.15, "avg_activation": 0.2},
        {"propensity_tier": "upsell_ready", "merchant_count": 32,
         "avg_churn": 0.1, "avg_upsell": 0.78, "avg_activation": 0.35},
        {"propensity_tier": "activation_candidate", "merchant_count": 25,
         "avg_churn": 0.15, "avg_upsell": 0.3, "avg_activation": 0.72},
        {"propensity_tier": "stable", "merchant_count": 45,
         "avg_churn": 0.08, "avg_upsell": 0.25, "avg_activation": 0.15},
    ]


def get_ad_creative_library() -> list[dict]:
    creatives = {
        "champions": ("Your loyalty deserves a reward", "Congrats on being a top merchant! Unlock exclusive rates.",
                      "VIP: Exclusive rates inside", "VIP rates unlocked!", "Exclusive Rates", "Save up to 15% on processing fees", "celebratory", "Claim Reward"),
        "at_risk": ("We miss you — come back with a special offer", "It's been a while. Here's a 30-day fee waiver to welcome you back.",
                    "Come back! Fee waiver waiting", "Miss you! Free month inside", "Come Back", "30-day processing fee waiver", "warm", "Reactivate Now"),
        "new_customers": ("Welcome — get started in 3 easy steps", "Your first 1000 transactions are fee-free. Start accepting payments today.",
                          "Welcome! Free transactions await", "1000 free txns for you!", "Get Started Free", "1000 fee-free transactions for new merchants", "friendly", "Start Now"),
        "loyal": ("Thank you for growing with us", "Upgrade to Premium and unlock advanced analytics + priority support.",
                  "Upgrade to Premium today", "Premium analytics await!", "Go Premium", "Advanced analytics and priority support", "professional", "Upgrade Now"),
        "hibernating": ("Your account is waiting", "Reactivate today and we'll waive your first month's fees.",
                        "Reactivate today: free month", "We miss you", "Reactivate", "First month fees waived on reactivation", "empathetic", "Reactivate Free"),
    }
    result = []
    for seg, c in creatives.items():
        result.append({
            "segment": seg, "email_subject": c[0], "email_body_preview": c[1],
            "sms_message": c[2], "push_notification": c[3],
            "ad_headline": c[4], "ad_description": c[5],
            "tone": c[6], "cta": c[7],
            "merchant_count": random.randint(10, 40),
            "avg_volume": round(random.uniform(10000, 150000), 0),
        })
    return result


def get_campaign_roi_summary() -> list[dict]:
    return [
        {"campaign_type": "win_back_campaign", "channel": "sfmc_email",
         "merchants_targeted": 45, "conversions": 12,
         "conversion_rate": 26.7, "total_post_revenue": 185000, "reactivations": 8},
        {"campaign_type": "loyalty_reward", "channel": "app_push",
         "merchants_targeted": 30, "conversions": 18,
         "conversion_rate": 60.0, "total_post_revenue": 420000, "reactivations": 0},
        {"campaign_type": "premium_win_back", "channel": "phone",
         "merchants_targeted": 15, "conversions": 7,
         "conversion_rate": 46.7, "total_post_revenue": 310000, "reactivations": 5},
        {"campaign_type": "activation_incentive", "channel": "zender_sms",
         "merchants_targeted": 25, "conversions": 9,
         "conversion_rate": 36.0, "total_post_revenue": 95000, "reactivations": 9},
    ]


def get_campaign_outcome_distribution() -> list[dict]:
    return [
        {"campaign_outcome": "engaged", "merchant_count": 46, "avg_revenue": 18500, "avg_health": 62.3},
        {"campaign_outcome": "reactivated", "merchant_count": 22, "avg_revenue": 12200, "avg_health": 38.1},
        {"campaign_outcome": "no_response", "merchant_count": 47, "avg_revenue": 0, "avg_health": 28.5},
    ]


def get_audience_summary() -> list[dict]:
    ms = _build_merchants()
    counts = {
        "churn_risk": len([m for m in ms if m["segment"] in ("at_risk", "cant_lose")]),
        "high_value": len([m for m in ms if m["txn_volume"] > 50000 and m["segment"] in ("champions", "loyal")]),
        "new_onboarding": len([m for m in ms if m["segment"] == "new_customers"]),
        "winback": len([m for m in ms if m["segment"] in ("hibernating", "need_attention") and m["days_since_last_txn"] > 45]),
        "growth": len([m for m in ms if m["segment"] in ("promising", "potential_loyalists")]),
        "vip": len([m for m in ms if m["txn_volume"] > 100000]),
        "immediate_action": len([m for m in ms if m["urgency"] == "immediate"]),
    }
    unique_ids = set()
    for key, filt in {
        "churn_risk": lambda m: m["segment"] in ("at_risk", "cant_lose"),
        "high_value": lambda m: m["txn_volume"] > 50000 and m["segment"] in ("champions", "loyal"),
        "new_onboarding": lambda m: m["segment"] == "new_customers",
        "winback": lambda m: m["segment"] in ("hibernating", "need_attention") and m["days_since_last_txn"] > 45,
        "growth": lambda m: m["segment"] in ("promising", "potential_loyalists"),
        "vip": lambda m: m["txn_volume"] > 100000,
        "immediate_action": lambda m: m["urgency"] == "immediate",
    }.items():
        for m in ms:
            if filt(m):
                unique_ids.add(m["golden_id"])
    counts["total_activatable"] = len(unique_ids)
    return [counts]


def get_audience_list(audience_type: str, limit=100) -> list[dict]:
    ms = _build_merchants()
    flag_map = {
        "churn_risk": lambda m: m["segment"] in ("at_risk", "cant_lose"),
        "high_value": lambda m: m["txn_volume"] > 50000 and m["segment"] in ("champions", "loyal"),
        "new_onboarding": lambda m: m["segment"] == "new_customers",
        "winback": lambda m: m["segment"] in ("hibernating", "need_attention") and m["days_since_last_txn"] > 45,
        "growth": lambda m: m["segment"] in ("promising", "potential_loyalists"),
        "vip": lambda m: m["txn_volume"] > 100000,
        "immediate_action": lambda m: m["urgency"] == "immediate",
    }
    filt = flag_map.get(audience_type, lambda m: True)
    filtered = [m for m in ms if filt(m)]
    return [{
        "golden_id": m["golden_id"], "merchant_name": m["merchant_name"],
        "email": m["email"], "hashed_email": m["hashed_email"],
        "rfm_segment": m["segment"], "health_score": m["health_score"],
        "txn_volume": m["txn_volume"], "primary_action": m["primary_action"],
        "urgency": m["urgency"],
    } for m in filtered[:limit]]


# ── Anomaly Alerts ───────────────────────────────────────────────

ANOMALY_TYPES = ["volume_drop", "unexpected_inactivity", "ticket_spike", "health_collapse"]
ANOMALY_ACTIONS = {
    "volume_drop": "executive_outreach",
    "unexpected_inactivity": "reengagement_offer",
    "ticket_spike": "support_escalation",
    "health_collapse": "premium_win_back",
}


_anomaly_cache: list[dict] | None = None


def _build_anomaly_alerts() -> list[dict]:
    global _anomaly_cache
    if _anomaly_cache is not None:
        return _anomaly_cache
    rng = random.Random(42)
    ms = _build_merchants()
    alerts = []
    for m in ms:
        if m["health_score"] > 50 or rng.random() > 0.35:
            continue
        at = rng.choice(ANOMALY_TYPES)
        dev = round(rng.uniform(-85, -20), 1)
        alerts.append({
            "golden_id": m["golden_id"],
            "merchant_name": m["merchant_name"],
            "segment": m["segment"],
            "health_score": m["health_score"],
            "health_tier": m["health_tier"],
            "current_volume": m["txn_volume"],
            "avg_volume_30d": round(m["txn_volume"] / (1 + dev / 100), 0),
            "anomaly_type": at,
            "deviation_pct": dev,
            "recommended_action": ANOMALY_ACTIONS[at],
            "urgency": "immediate" if dev < -60 else "this_week",
            "estimated_revenue_impact": round(abs(dev / 100) * m["txn_volume"] * 0.3, 0),
            "detected_at": (datetime.now() - timedelta(hours=rng.randint(1, 72))).strftime("%Y-%m-%d %H:%M"),
        })
    alerts.sort(key=lambda a: abs(a["deviation_pct"]), reverse=True)
    _anomaly_cache = alerts
    return _anomaly_cache


def get_anomaly_alerts(anomaly_type: str = "", limit: int = 50) -> list[dict]:
    alerts = _build_anomaly_alerts()
    if anomaly_type:
        alerts = [a for a in alerts if a["anomaly_type"] == anomaly_type]
    return alerts[:limit]


def get_anomaly_kpis() -> dict:
    alerts = _build_anomaly_alerts()
    return {
        "total_alerts": len(alerts),
        "volume_drops": sum(1 for a in alerts if a["anomaly_type"] == "volume_drop"),
        "unexpected_inactivity": sum(1 for a in alerts if a["anomaly_type"] == "unexpected_inactivity"),
        "ticket_spikes": sum(1 for a in alerts if a["anomaly_type"] == "ticket_spike"),
        "health_collapses": sum(1 for a in alerts if a["anomaly_type"] == "health_collapse"),
        "total_revenue_at_risk": sum(a["estimated_revenue_impact"] for a in alerts),
    }


# ── Merchant Timeline ────────────────────────────────────────────

def get_merchant_timeline(golden_id: str, limit: int = 30) -> list[dict]:
    m = get_merchant_detail(golden_id)
    if not m:
        return []
    now = datetime.now()
    events = [
        {"event_type": "transaction", "event_date": (now - timedelta(days=m.get("days_since_last_txn", 1))).strftime("%Y-%m-%d"),
         "description": f"Transaction volume: ${m['txn_volume']:,.0f}", "detail": f"{m.get('txn_count', 0)} transactions", "source": "engagement"},
        {"event_type": "health_update", "event_date": now.strftime("%Y-%m-%d"),
         "description": f"Health score: {m['health_score']} ({m['health_tier']})", "detail": f"Segment: {m['segment']}", "source": "health"},
    ]
    if m.get("primary_action"):
        events.append({"event_type": "nba_recommendation", "event_date": (now - timedelta(days=random.randint(1, 14))).strftime("%Y-%m-%d"),
                        "description": f"Action: {m['primary_action']}", "detail": f"Channel: {m.get('primary_channel', 'N/A')} | Urgency: {m.get('urgency', 'N/A')}", "source": "nba"})
    if m.get("ticket_count", 0) > 0:
        events.append({"event_type": "support_ticket", "event_date": (now - timedelta(days=random.randint(1, 30))).strftime("%Y-%m-%d"),
                        "description": f"{m['ticket_count']} support tickets", "detail": "Includes open and resolved", "source": "support"})
    for i in range(random.randint(2, 6)):
        events.append({"event_type": "action_taken", "event_date": (now - timedelta(days=random.randint(5, 90))).strftime("%Y-%m-%d"),
                        "description": f"Executed: {random.choice(ACTIONS)} via {random.choice(CHANNELS)}", "detail": "", "source": "action_log"})
    events.sort(key=lambda e: e["event_date"], reverse=True)
    return events[:limit]


# ── Data Freshness ───────────────────────────────────────────────

_freshness_cache: list[dict] | None = None


def get_data_freshness() -> list[dict]:
    global _freshness_cache
    if _freshness_cache is not None:
        return _freshness_cache
    tables = [
        "gold_customer_360", "gold_engagement_metrics", "gold_segments",
        "gold_health_score", "gold_next_best_actions", "gold_customer_ltv",
        "gold_channel_attribution", "gold_behavioral_segments",
        "gold_support_analytics", "gold_call_center_analytics",
        "gold_call_center_sentiment", "gold_personalization_signals",
        "gold_propensity_scores", "gold_ad_creative_library",
        "gold_campaign_roi", "gold_audience_exports", "gold_anomaly_alerts",
    ]
    rng = random.Random(42)
    result = []
    now = datetime.now()
    for t in tables:
        hrs = rng.randint(0, 2) if rng.random() > 0.12 else rng.randint(4, 8)
        result.append({
            "table_name": t,
            "last_updated": (now - timedelta(hours=hrs)).strftime("%Y-%m-%d %H:%M"),
            "row_count": rng.randint(50, 5000),
            "status": "healthy" if hrs < 4 else "stale",
        })
    _freshness_cache = result
    return _freshness_cache


# ── Agent Feedback ───────────────────────────────────────────────

_feedback_store: list[dict] = []


def log_agent_feedback(feedback: dict) -> dict:
    _feedback_store.append(feedback)
    return {"status": "recorded", "total_feedback": len(_feedback_store)}
