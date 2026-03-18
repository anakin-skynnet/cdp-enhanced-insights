# QA Test Report — Getnet CDP Customer 360 App (Extended)

**Test Date:** March 18, 2026  
**App URL Tested:** `http://localhost:8081` (local with mock data)  
**Data Source:** `CDP_DATA_SOURCE=mock`  
**Test Framework:** Playwright (headless Chromium)

---

## Executive Summary

| Page | Status | KPIs | Charts | Tables/Cards | Notes |
|------|--------|------|--------|--------------|-------|
| Call Center | ✅ Pass | 6 cards | 1 bar | Agent + Queue tables | All data meaningful |
| Campaign ROI | ✅ Pass | 4 cards | 2 (bar + doughnut) | 4 rows | ROI + Outcomes |
| Audiences | ✅ Pass | N/A | N/A | 11 segment cards | Export CSV per audience ✓ |
| Personalization | ✅ Pass | N/A | 1 doughnut | 8 cards | Propensity Tiers |
| Ad Creative | ✅ Pass | N/A | N/A | 8 creative cards | Per segment |
| Customer Lifetime Value | ✅ Pass | N/A | 1 bar | 15 rows | CLV by Tier + Top merchants |
| Channel Attribution | ✅ Pass | N/A | 2 (doughnut + bar) | 7 rows | Markov + Multi-Touch |
| Behavioral Segments | ✅ Pass | N/A | 1 bar | 4 cluster cards | Clusters + detail cards |
| Data Freshness | ✅ Pass | N/A | N/A | 17 rows | Table status list |
| About this Solution | ✅ Pass | N/A | N/A | N/A | Architecture, data sources, gold catalog |
| AI Chat Panel | ✅ Pass | N/A | N/A | N/A | FAB, Agent/Genie toggle, thumbs |

**Overall:** 11/11 pages passed. **0 console errors.** 1 non-blocking warning (Tailwind CDN production notice).

---

## 1. Call Center

### What Renders Correctly
- **KPI Cards (6):** Total Interactions (8,742), Avg Handle Time (342s), Resolution Rate (87.3%), Abandonment (4.2%), voice/chat breakdown
- **Sentiment by Topic:** Stacked bar chart with Positive/Neutral/Negative % by topic (billing, technical_issue, onboarding, account_inquiry, etc.)
- **Agent Performance Table:** 16 rows with Agent ID, Total, Voice, Chat, Avg Handle, Resolution %
- **Queue Performance Table:** Queue status (billing, technical, onboarding, general) with Volume, Handle Time, Wait, Abandon %, SLA %

### Data Quality
- All KPIs from `/api/call-center/kpis` return meaningful values (no NaN, undefined, zero-only)
- Mock data: 8,742 interactions, 5,231 voice, 2,890 chat

### Console Errors
- None

### UI/UX Suggestions
- Consider adding a "First-Call Resolution" KPI if distinct from Resolution Rate (currently labeled "First contact")
- Sentiment chart could be more prominent; consider adding a "Sentiment Score" KPI card

---

## 2. Campaign ROI

### What Renders Correctly
- **KPI Cards (4):** Total Post-Campaign Revenue, Total Conversions, Avg Conversion Rate, Campaigns Analyzed
- **Campaign ROI by Type:** Bar chart (win_back, loyalty_reward, premium_win_back, etc.)
- **Campaign Outcomes:** Doughnut chart (engaged, reactivated, no_response)
- **Campaign Details Table:** 4 rows with campaign type, channel, merchants targeted, conversions, conversion rate, revenue

### Data Quality
- All values meaningful; conversion rates 26–60%, revenue in $185K–$420K range

### Console Errors
- None

### UI/UX Suggestions
- Consider adding a "Best Performing Campaign" highlight card

---

## 3. Audiences

### What Renders Correctly
- **Audience Segment Cards (11):** churn_risk, high_value, new_onboarding, winback, growth, vip, immediate_action, etc.

Each card shows:
- Segment name and member count
- **Export CSV** link per audience (e.g. `/api/export/audience?audience_type=churn_risk&limit=500`)

- **Drill-down:** Clicking a card loads `audience-detail` with member list table

### Data Quality
- Member counts: 8–28 per segment (mock data)

### Export CSV Test
- `curl /api/export/audience?audience_type=churn_risk&limit=10` returns HTTP 200 with valid CSV

### Console Errors
- None

### UI/UX Suggestions
- Consider adding a "Total Activatable" summary card at the top
- Drill-down table could show pagination for large audiences

---

## 4. Personalization

### What Renders Correctly
- **Personalization Summary:** 8 cards with content theme, merchant tier, count, avg_upsell, avg_churn, avg_activation
- **Propensity Tiers Doughnut:** high_churn_risk, upsell_ready, activation_candidate, stable
- **Merchant Detail:** Clicking a merchant row opens signals panel (preferred_channel, best_contact_time, content_affinity)

### Data Quality
- All propensity scores meaningful (0.08–0.82 range)

### Console Errors
- None

### UI/UX Suggestions
- Add a "Top Personalization Signal" or "Recommended Action" per merchant in the drill-down

---

## 5. Ad Creative

### What Renders Correctly
- **AI-Generated Ad Creative Cards (8):** One per segment (champions, at_risk, hibernating, etc.)
- Each card shows: email_subject, email_body_preview, sms_message, push_notification, ad_headline, ad_description, tone, CTA, merchant_count, avg_volume

### Data Quality
- All values populated; tone and CTA vary by segment

### Console Errors
- None

### UI/UX Suggestions
- Consider adding "Copy to clipboard" for each creative block (email, SMS, push)
- Add "Preview" or "A/B test" placeholder for future functionality

---

## 6. Customer Lifetime Value (CLV)

### What Renders Correctly
- **CLV by Tier Bar Chart:** Tiers (bronze, silver, gold, platinum) with CLV values
- **Top CLV Merchants Table:** 15 rows with merchant, segment, CLV, tier, volume

### Data Quality
- CLV values in $15K–$85K range (mock)

### Console Errors
- None

### UI/UX Suggestions
- Add a "Total CLV at Risk" or "Upsell Opportunity" KPI

---

## 7. Channel Attribution

### What Renders Correctly
- **Markov Chain Attribution Doughnut:** Channel contribution (sfmc_email, zender_sms, phone, etc.)
- **Multi-Touch Comparison Bar Chart:** Markov vs Linear vs Last-Touch attribution
- **Channel Table:** 7 rows with channel, markov_attribution, linear_attribution, last_touch

### Data Quality
- Attribution percentages sum to 100%; all channels represented

### Console Errors
- None

### UI/UX Suggestions
- Add a "Top Converting Channel" highlight

---

## 8. Behavioral Segments

### What Renders Correctly
- **Behavioral Clusters Bar Chart:** Cluster counts (e.g. high_volume_frequent, low_engagement, etc.)
- **Cluster Detail Cards (4):** Each cluster with member count, avg_volume, avg_recency, description

### Data Quality
- All cluster metrics populated

### Console Errors
- None

### UI/UX Suggestions
- Consider adding drill-down to member list per cluster

---

## 9. Data Freshness

### What Renders Correctly
- **Table Freshness Status List:** 17 rows with table name, last_updated, status (fresh/stale)
- **Header Freshness Indicator:** Dot in header (`#freshness-dot`) with label (e.g. "2 stale tables" or "All fresh")
- Navigate via header button (not sidebar)

### Data Quality
- All tables have last_updated timestamps; status derived from freshness

### Console Errors
- None

### UI/UX Suggestions
- Consider adding "Last refresh" timestamp for the entire app
- In mock mode, clarify that freshness is simulated

---

## 10. About this Solution

### What Renders Correctly
- **Architecture Diagram:** ASCII diagram of data flow (Bronze → Silver → Gold → AI/BI)
- **Data Sources Table:** Salesforce, Zendesk, Genesys, payment processors, etc.
- **Gold Table Catalog:** 20+ gold tables (gold_merchant_360, gold_call_center_analytics, gold_propensity_scores, etc.)
- **AI Models Grid:** CDP Supervisor Agent, Churn Specialist, Campaign Specialist, Support Specialist, AI/BI Genie Space
- **Agent Cards:** Descriptions, tools (UC functions)
- **Technology Stack:** Lakeflow, Unity Catalog, Model Serving, Genie, etc.

### Console Errors
- None

### UI/UX Suggestions
- Consider adding a "Quick Start" or "Deployment" section for new users
- Add links to Databricks docs for each component

---

## 11. AI Chat Panel

### What Renders Correctly
- **FAB Button:** Floating action button bottom-right (`#chat-fab`) opens panel
- **Chat Panel:** `#ai-chat-panel` with Agent/Genie mode toggle (`#mode-agent`, `#mode-genie`)
- **Input:** `#chat-input` with placeholder "Ask the CDP Agent..."/"Ask Genie a data question..."
- **Send:** Button triggers `sendChat()`
- **Quick Prompts:** "Weekly priorities", "Churn risk", "Campaign ideas" buttons
- **Bot Response:** Mock agent returns contextual responses (churn, campaign, action plan)
- **Thumbs Up/Down:** Feedback buttons on bot messages (`title="Helpful"`, `title="Not helpful"`)

### Agent Mode Test
- Sent "What are top priority actions?" → Mock agent returned Weekly Action Plan with 12 immediate, 18 this week, $485K revenue at stake
- No live agent required; mock handles gracefully

### Console Errors
- None

### UI/UX Suggestions
- Add loading indicator during agent response (e.g. "Thinking..." — already present)
- Consider adding "Copy response" for long agent outputs
- Add character limit indicator if input has max length

---

## Console Warnings

| Warning | Severity | Action |
|---------|----------|--------|
| `cdn.tailwindcss.com should not be used in production` | Low | For production, use Tailwind PostCSS or CLI build |

---

## API Endpoint Verification

All endpoints return HTTP 200 with valid JSON:

| Endpoint | Status |
|----------|--------|
| `/api/config` | ✅ |
| `/api/call-center/kpis` | ✅ |
| `/api/call-center/agents` | ✅ |
| `/api/call-center/queues` | ✅ |
| `/api/call-center/sentiment` | ✅ |
| `/api/campaign-roi` | ✅ |
| `/api/campaign-roi/outcomes` | ✅ |
| `/api/audiences/summary` | ✅ |
| `/api/audiences/{type}` | ✅ |
| `/api/export/audience` | ✅ |
| `/api/personalization/summary` | ✅ |
| `/api/propensity` | ✅ |
| `/api/ad-creative` | ✅ |
| `/api/clv/summary` | ✅ |
| `/api/clv/top` | ✅ |
| `/api/attribution` | ✅ |
| `/api/behavioral-segments` | ✅ |
| `/api/data-freshness` | ✅ |

---

## Recommendations Summary

1. **Production:** Replace Tailwind CDN with built CSS; consider bundling Chart.js for smaller payload.
2. **Pagination:** Add pagination for Merchants, NBA, Audiences, and CLV tables.
3. **Data Freshness:** Clarify mock vs live freshness indicator in UI.
4. **AI Chat:** Add "Copy response" for long agent outputs.
5. **Ad Creative:** Add copy-to-clipboard for email/SMS/push blocks.
6. **Accessibility:** Add `aria-label` to icon-only buttons (e.g. Export CSV, chat toggle).

---

## Test Artifacts

- **Extended QA Report:** `qa-report-extended.json`
- **Extended Test Script:** `qa-test-extended.js`
- **Run Command:** `BASE_URL=http://localhost:8081 node qa-test-extended.js`
