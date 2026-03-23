# Bank Payment Platform CDP / Customer 360

Customer Data Platform and Customer 360 solution on **Azure Databricks** for the Bank Payment Platform.

## Use Cases

| # | Use Case | Status | Key Components |
|---|----------|--------|----------------|
| 1 | **Customer 360 Platform** | Complete | Identity resolution, golden record, full medallion architecture |
| 2 | **Customer Churn Analysis & Prediction** | Complete | XGBoost model, churn risk dashboard, churn prevention agent |
| 3 | **Customer Segmentation** | Complete | RFM segmentation + K-Means behavioral clustering |
| 4 | **Customer Support Analytics** | Complete | TTR, CSAT, SLA compliance, support quality tiers |
| 5 | **Call Center Analytics** | Complete | Sentiment analysis, topic classification, agent/queue metrics |
| 6 | **Hyper-Personalization Insights** | Complete | Propensity scoring (churn/upsell/activation), contextual signals |
| 7 | **Ad Creative Generation** | Complete | Data-enriched AI creative (propensity, CLV, campaign history, industry), Standard SVG + Premium GPT-5-4 Designer banners |
| 8 | **Marketing Campaign Analysis & ROI** | Complete | Conversion tracking, revenue lift, reactivation, MTA |
| 9 | **Audience Activation** | Complete | Pre-built audiences, hashed IDs, CSV export, ad platform sync |

## Overview

- **Golden record:** 2–2.5M unique merchants
- **Sources:** Salesforce, Zendesk, Genesys, Apian, transactional data
- **Activation:** Hightouch → SFMC, Meta, Google Ads; Audience export
- **AI:** Churn prediction, propensity scoring, sentiment analysis, hyper-personalized ad creative (data-enriched), GPT-5-4 Premium Designer banners, health scoring, next best actions, CLV, multi-touch attribution, Genie NL analytics, AI agents

## Architecture

```
Bronze (raw) → Silver (validated + enriched) → Gold (golden record + analytics)
                    ↓
         Identity Resolution (gold_customer_360)
                    ↓
    ┌──────────────────────────────────────────────────┐
    │  Gold Layer Views & ML Models                    │
    │  ├── gold_engagement_metrics (RFM features)      │
    │  ├── gold_segments (RFM segments)                │
    │  ├── gold_health_score (composite 0-100)         │
    │  ├── gold_next_best_actions (NBA engine)         │
    │  ├── gold_support_analytics (TTR, CSAT, SLA)     │
    │  ├── gold_call_center_analytics (agent/queue)    │
    │  ├── gold_call_center_sentiment (NLP enriched)   │
    │  ├── gold_personalization_signals (propensity)    │
    │  ├── gold_propensity_scores (ML models)          │
    │  ├── gold_campaign_roi (lift analysis)           │
    │  ├── gold_audience_exports (activation-ready)    │
    │  ├── gold_ad_creative_library (Gen AI content)   │
    │  ├── gold_customer_ltv (CLV model)               │
    │  ├── gold_channel_attribution (MTA Markov)       │
    │  └── gold_behavioral_segments (K-Means)          │
    └──────────────────────────────────────────────────┘
                    ↓
    AI Agents | Dashboards | CDP App | Hightouch | Genie
```

## Project Structure

```
cdp-enhanced-insights/
├── databricks.yml           # DAB config (dev/prod)
├── resources/
│   ├── pipelines.yml        # Bronze, Silver, Gold SDP pipelines
│   ├── jobs.yml             # 11 jobs: identity, churn, segmentation, CLV, MTA,
│   │                        #   behavioral, call center NLP, propensity, ad creative,
│   │                        #   campaign ROI, agent deployment
│   ├── dashboards.yml       # 6 AI/BI Lakeview dashboards
│   └── alerts.yml           # Pipeline failure alerts (see docs/ALERTS_SETUP.md)
├── src/
│   ├── dashboards/          # Lakeview dashboard JSON definitions
│   │   ├── cdp_customer_360.lvdash.json
│   │   ├── cdp_churn_risk.lvdash.json
│   │   ├── cdp_segmentation.lvdash.json
│   │   ├── cdp_next_best_actions.lvdash.json
│   │   ├── cdp_support_analytics.lvdash.json
│   │   └── cdp_call_center.lvdash.json
│   ├── app/                 # CDP web application (FastAPI + Tailwind)
│   │   ├── main.py          # FastAPI routes (50+ API endpoints)
│   │   ├── db.py            # SQL warehouse data layer (40+ query functions)
│   │   ├── static/index.html # 14-page SPA frontend
│   │   ├── app.yaml         # Databricks Apps config
│   │   └── requirements.txt
│   ├── agents/              # AI agents (LangGraph + ResponsesAgent)
│   │   ├── churn_prevention/agent.py
│   │   ├── segment_campaign/agent.py
│   │   ├── next_best_action/agent.py
│   │   ├── uc_functions/setup_uc_functions.sql  # 23 UC functions
│   │   └── deploy_agents.py
│   └── cdp_etl/
│       ├── transformations/
│       │   ├── bronze/      # 5 raw ingestion tables
│       │   ├── silver/      # 5 cleaned tables (enriched with lifecycle metrics)
│       │   └── gold/        # 9 materialized views
│       └── notebooks/
│           ├── identity_resolution/     # Entity resolution
│           ├── churn_model/             # XGBoost churn prediction
│           ├── segmentation/            # Materialized view refresh
│           ├── clv/                     # BG/NBD + Gamma-Gamma CLV
│           ├── attribution/             # Markov Chain MTA
│           ├── advanced_segmentation/   # K-Means behavioral
│           ├── call_center_analytics/   # Speech-to-text + sentiment (ai_query)
│           ├── hyper_personalization/   # Propensity scoring models
│           ├── ad_creative/             # Gen AI ad copy (Foundation Model API)
│           └── campaign_analysis/       # Campaign ROI analysis
└── docs/
    ├── LAKEFLOW_CONNECT_SETUP.md
    ├── AIRBYTE_FIVETRAN_SETUP.md
    ├── HIGHTOUCH_SETUP.md
    ├── GENIE_SPACE_SETUP.md
    └── ALERTS_SETUP.md
```

## Quick Start

### 1. Configure Connectors

- **Lakeflow Connect:** Salesforce (AR org), Zendesk — see [docs/LAKEFLOW_CONNECT_SETUP.md](docs/LAKEFLOW_CONNECT_SETUP.md)
- **Airbyte/Fivetran:** Genesys — see [docs/AIRBYTE_FIVETRAN_SETUP.md](docs/AIRBYTE_FIVETRAN_SETUP.md)

### 2. Create Raw Volumes

Ensure data lands in:

- `/Volumes/ahs_demos_catalog/cdp_360/raw/salesforce/contacts/`
- `/Volumes/ahs_demos_catalog/cdp_360/raw/salesforce/accounts/`
- `/Volumes/ahs_demos_catalog/cdp_360/raw/zendesk/tickets/`
- `/Volumes/ahs_demos_catalog/cdp_360/raw/genesys/`
- `/Volumes/ahs_demos_catalog/cdp_360/raw/transactions/`

### 3. Deploy and Run

```bash
# Validate
databricks bundle validate

# Deploy to dev
databricks bundle deploy

# Run pipelines (order: ingestion → silver → gold)
databricks bundle run cdp_ingestion
databricks bundle run cdp_silver
databricks bundle run cdp_gold_identity

# Run identity resolution job (creates gold_customer_360)
databricks bundle run identity_resolution
```

### 4. Dashboards, Activation, and AI

Dashboards deploy automatically with `databricks bundle deploy`:

- **Customer 360 Overview** - golden record KPIs, segment distribution, identity resolution stats
- **Churn Risk Analysis** - at-risk merchants, recency distribution, revenue at risk
- **Merchant Segmentation** - RFM segments, volume share, segment performance comparison

Other tools:
- **Hightouch:** [docs/HIGHTOUCH_SETUP.md](docs/HIGHTOUCH_SETUP.md)
- **Genie Space:** [docs/GENIE_SPACE_SETUP.md](docs/GENIE_SPACE_SETUP.md)

## Run Order

1. Bronze pipeline (ingestion)
2. Silver pipeline
3. Identity Resolution job → `gold_customer_360`, `gold_identity_graph`
4. Gold pipeline → `gold_engagement_metrics`, `gold_segments`, `gold_health_score`, `gold_next_best_actions`, `gold_support_analytics`, `gold_call_center_analytics`, `gold_personalization_signals`, `gold_campaign_roi`, `gold_audience_exports`
5. Churn model training job
6. Segmentation refresh job
7. CLV training job → `gold_customer_ltv`
8. Multi-touch attribution job → `gold_channel_attribution`
9. Behavioral segmentation job → `gold_behavioral_segments`
10. Call center NLP enrichment job → `gold_call_center_sentiment`
11. Propensity scoring job → `gold_propensity_scores`
12. Ad creative generation job → `gold_ad_creative_library`
13. Campaign ROI analysis job → `gold_campaign_performance_summary`
14. Deploy AI agents → `databricks bundle run deploy_agents`

## AI Agents

Three LangGraph-based agents deployed to Model Serving endpoints:

**Churn Prevention Agent** - Analyzes at-risk merchants and generates personalized retention actions.

```
"Show me the top at-risk merchants by revenue"
"What retention actions should we take for merchant X?"
"Give me a churn prevention plan for this month"
```

**Segment Campaign Agent** - Helps marketing design targeted campaigns for specific merchant segments.

```
"Design a campaign to re-engage hibernating merchants"
"What's the best campaign for our champions segment?"
"Build me an audience for a win-back email journey"
```

**Next Best Action Agent** - Determines the single most impactful action for every merchant, right now.

```
"What should my team do today? Show immediate priorities"
"Why is executive_outreach recommended for merchant X?"
"Give me a weekly action plan ranked by revenue impact"
"Which channels need the most actions this week?"
```

All agents use UC Functions to query the golden record in real-time and apply domain-specific playbooks for the Bank Payment Platform. The NBA agent adds a composite health score (0-100) and a rule engine that maps each merchant to a specific action, channel, urgency, and estimated revenue impact.

## CDP Application

A full-featured 14-page web application for the commercial and marketing teams:

```bash
# Deploy to Databricks Apps
databricks apps create cdp-360 --source-code-path src/app

# Local development
cd src/app && uvicorn app.main:app --reload --port 8080
```

**Tabs:**

- **Dashboard** -- KPI cards, segment donut chart, health distribution
- **Merchants** -- Search/filter, C360 profile with health breakdown, RFM, engagement, NBA
- **Support Analytics** -- TTR, CSAT, SLA, support quality tiers, top ticket merchants
- **Call Center** -- Sentiment by topic (stacked bar), agent performance, queue metrics
- **Next Best Actions** -- Priority-ranked action queue with urgency/segment filters
- **Campaigns** -- Segment targeting, audience preview, campaign launcher
- **Campaign ROI** -- Conversion tracking, outcome distribution, revenue lift by channel
- **Audiences** -- Pre-built audience cards (churn risk, VIP, winback, etc.), CSV export
- **Personalization** -- Propensity distribution, content theme/tier matrix
- **Ad Creative** -- Hyper-personalized AI creative with data-enriched prompts (propensity, CLV, campaign history, industry), Standard SVG banners, Premium GPT-5-4 Designer banners, industry filtering, A/B variants
- **AI Insights** -- CLV tiers, multi-touch attribution, behavioral clusters
- **Operations Center** -- Campaign management, NBA assignments, alert triage (Lakebase-backed)
- **Data Freshness** -- Pipeline health monitoring
- **About this Solution** -- Architecture, gold tables, technology stack, enrichment pipeline

## Databricks Solution Accelerators & Demos

Adapted from official [Databricks Marketing Solution Accelerators](https://www.databricks.com/solutions/accelerators?solutions=Marketing) and [Demo Catalog](https://dbdemos-demo-catalog-2556758628403379.aws.databricksapps.com/):

**[Customer Lifetime Value](https://www.databricks.com/solutions/accelerators/customer-lifetime-value)** (`src/cdp_etl/notebooks/clv/clv_training.py`)
- BG/NBD model for purchase probability + Gamma-Gamma model for monetary value
- Predicts 12-month CLV per merchant with probability-of-alive scores
- Output: `gold_customer_ltv` with CLV tiers (very_high through very_low)

**[Multi-Touch Attribution](https://www.databricks.com/solutions/accelerators/multi-touch-attribution)** (`src/cdp_etl/notebooks/attribution/multi_touch_attribution.py`)
- Markov Chain attribution (data-driven) + heuristic models (first-touch, last-touch, linear, time-decay)
- Measures channel effectiveness across merchant touchpoint journeys
- Output: `gold_channel_attribution` with per-channel credit allocation

**[Customer Segmentation for Personalization](https://www.databricks.com/solutions/accelerators/customer-segmentation-sv)** (`src/cdp_etl/notebooks/advanced_segmentation/behavioral_clustering.py`)
- K-Means clustering on multi-dimensional behavioral features (beyond basic RFM)
- Automatically discovers optimal K via silhouette analysis
- Output: `gold_behavioral_segments` with cluster labels and profiles

**[Lakehouse for C360: Reducing Customer Churn](https://www.databricks.com/resources/demos/tutorials/lakehouse-platform/c360-platform-reduce-churn)** (adapted across the full solution)
- End-to-end customer 360 with identity resolution, medallion architecture, ML churn prediction
- Integrated throughout: bronze/silver/gold ETL, XGBoost churn model, Lakeview dashboards

**[AI-Powered Call Centre Analytics](https://community.databricks.com/t5/technical-blog/ai-powered-call-centre-analytics-from-call-transcripts-to/ba-p/121951)** (`src/cdp_etl/notebooks/call_center_analytics/call_center_nlp.py`)
- Sentiment analysis via `ai_analyze_sentiment()` and topic classification via `ai_query()`
- Agent performance and queue metrics from Genesys interactions
- Output: `gold_call_center_sentiment`, `gold_call_center_analytics`

**[Generative AI for Personalized Marketing Content](https://www.databricks.com/blog/building-generative-ai-workflow-creation-more-personalized-marketing-content)** (`src/cdp_etl/notebooks/ad_creative/ad_creative_generation.py`)
- Uses Foundation Model API with data-enriched prompts: propensity scores, CLV, campaign history, health tiers, industry, and tenure from 6 gold tables
- Batch: enriched segment profiles fed to `ai_query()` with propensity-driven rules (high churn → retention urgency, high upsell → growth)
- App: real-time auto-enrichment pipeline queries 23+ data points per segment before LLM prompt, with industry filter and GPT-5-4 Premium Designer banner generation (multi-layered SVG with gradients, animations, and industry illustrations)
- Output: `gold_ad_creative_library` with data-specific creative per segment

**[AI/BI Marketing Campaign Effectiveness](https://databricks.com/resources/demos/tutorials/aibi-genie-marketing-campaign-effectiveness)** (adapted into campaign ROI module)
- Campaign conversion tracking, revenue lift, reactivation measurement
- Output: `gold_campaign_roi`, `gold_campaign_performance_summary`

## MVP Scope (Argentina, End of July)

- Lakeflow Connect: Salesforce AR, Zendesk
- Airbyte: Genesys
- Bronze/Silver/Gold pipelines
- Identity resolution + golden record
- Hightouch → SFMC
- Churn model + Genie Space

## License

Internal use — Bank Payment Platform.
