# QA Test Report — Getnet CDP Customer 360 App

**Test Date:** March 18, 2026  
**App URL Tested:** `http://127.0.0.1:8765` (local with mock data)  
**Target Live URL:** `https://getnet-financial-closure-984752964297111.11.azure.databricksapps.com`

> **Note:** The live Databricks Apps URL timed out during automated fetch (likely due to auth or network). Testing was performed on the local instance with `CDP_DATA_SOURCE=mock`, which mirrors the deployed app behavior.

---

## Executive Summary

| Page | Status | KPIs | Charts | Tables | Export CSV |
|------|--------|------|--------|--------|------------|
| Dashboard | ✅ Pass | 8 cards | 2 | N/A | N/A |
| Merchants | ✅ Pass | N/A | N/A | 50 rows | ✅ |
| Next Best Actions | ✅ Pass | 6 cards | N/A | 30 rows | ✅ |
| Anomaly Alerts | ✅ Pass | 6 cards | 2 | 28 rows | ✅ |
| Support Analytics | ✅ Pass | 4 cards | 1 | 20 rows | ⚠️ Not implemented |

**Overall:** 5/5 pages passed core rendering and data checks. No JavaScript console errors. One Tailwind CDN warning (non-blocking).

---

## 1. Dashboard

### What Renders Correctly
- **KPI Cards (8):** Total Merchants (120), Total Volume ($15.4M), At-Risk Merchants (44), Avg Health Score (37.5), Avg Volume ($128.5K), Total Tickets (1.8K), Avg Recency (92 days)
- **Segment Distribution:** Doughnut chart with segments (champions, loyal, hibernating, new customers, etc.)
- **Health Distribution:** Bar chart with Count and Avg Score by health tier (critical, poor, fair, good, excellent)
- **Layout:** Dark theme, responsive grid, sidebar navigation
- **Data Quality:** No NaN, undefined, or zero-only values

### What's Broken or Missing
- None

### Console Errors
- None

### UI/UX Suggestions
- Consider adding "Active %" KPI if that metric is available in the data model
- "2 stale tables" badge in header may confuse users in mock mode; consider hiding or clarifying when using mock data

---

## 2. Merchants

### What Renders Correctly
- **Search:** Filter input with debounce (300ms). Typing "Café" correctly filters to 2 merchants
- **Table:** 50 rows with ID, Name, Segment, Health, Score, Volume, Recency, Tickets
- **Export CSV:** Link present and returns HTTP 200 with valid CSV
- **Merchant Detail Sheet:** Clicking a row opens the 360 profile sheet with:
  - Merchant name, golden_id, segment/health badges
  - Health Score, Volume, Recency, Tickets KPIs
  - RFM scores (Recency, Frequency, Monetary)
  - Next Best Action (primary, channel, revenue impact)
  - Contact (email, phone)
  - Activity Timeline
- **Pagination:** Table shows up to 50 merchants (limit from API)

### What's Broken or Missing
- Pagination controls are not visible in the UI; users see "50 merchants" but cannot page to more. Consider adding prev/next or page size selector.

### Console Errors
- None

### UI/UX Suggestions
- Add pagination or "Load more" for large merchant lists
- Consider segment/health filter dropdowns in addition to search

---

## 3. Next Best Actions (NBA)

### What Renders Correctly
- **Summary Cards:** 6 action-type cards (e.g., cross sell products, product education, premium win back) with merchant count, revenue impact, and urgency badges (immediate, this week)
- **Action Queue Table:** 30 rows with Merchant, Segment, Urgency, Action, Channel, Priority, Revenue Impact
- **Export CSV:** Link present and returns HTTP 200
- **Row Click:** Opens merchant detail sheet (same as Merchants page)
- **Data Quality:** All values meaningful; no NaN/undefined

### What's Broken or Missing
- None

### Console Errors
- None

### UI/UX Suggestions
- Add urgency/segment filters for the queue table
- Consider sorting by priority or urgency by default

---

## 4. Anomaly Alerts

### What Renders Correctly
- **KPI Cards (6):** Total Alerts (30), Volume Drops (6), Inactivity (11), Ticket Spikes (4), Health Collapse (9), Revenue at Risk ($448.6K)
- **Alert Distribution:** Doughnut chart (Volume Drop, Inactivity, Ticket Spike, Health Collapse)
- **Severity Distribution:** Bar chart (Critical >60%, High 40–60%, Medium 20–40%, Low <20%)
- **Active Alerts Table:** 28 rows with Merchant, Anomaly, Segment, Health, Deviation, Volume, Revenue Risk, Urgency, Detected
- **Export CSV:** Link present and returns HTTP 200
- **Row Click:** Opens merchant detail sheet

### What's Broken or Missing
- User spec mentioned "affected merchants" and "resolution rate" KPIs; current implementation uses different metrics (volume_drops, unexpected_inactivity, ticket_spikes, health_collapses, total_revenue_at_risk). Consider aligning with spec if those metrics exist in the data model.

### Console Errors
- None

### UI/UX Suggestions
- Add anomaly type filter (dropdown or chips)
- Add severity filter for the table

---

## 5. Support Analytics

### What Renders Correctly
- **KPI Cards (4):** Total Tickets (1.2K, 156 open), Avg First Response (42min, SLA 89.1%), Avg Resolution (388min, SLA 72.4%), CSAT Score (78.2)
- **Support Quality Distribution:** Bar chart (excellent, good, fair, poor) with merchant counts
- **Top Support Merchants Table:** 20 rows with Merchant, Tickets, Avg Resolution, CSAT, Quality
- **Data Quality:** All values meaningful; no NaN/undefined

### What's Broken or Missing
- **Export CSV:** No Export CSV button on this page. User spec requested testing it. Consider adding `/api/export/support-merchants` and a button.

### Console Errors
- None

### UI/UX Suggestions
- Add Export CSV for support merchants list
- User spec mentioned "escalation rate" KPI; not present. Add if available in data model.
- Consider quality tier filter for the merchant table

---

## Console Summary

| Type | Count | Details |
|------|-------|---------|
| Errors | 0 | None |
| Warnings | 1 | Tailwind CDN: "cdn.tailwindcss.com should not be used in production" — expected for dev; use PostCSS/Tailwind CLI for production builds |

---

## Screenshots

Screenshots saved to project root:
- `qa-screenshot-dashboard.png`
- `qa-screenshot-merchants.png`
- `qa-screenshot-nba.png`
- `qa-screenshot-anomalies.png`
- `qa-screenshot-support.png`

---

## Recommendations

1. **Production:** Replace Tailwind CDN with compiled CSS (PostCSS or Tailwind CLI).
2. **Support Analytics:** Add Export CSV for the support merchants table.
3. **Merchants:** Add pagination or "Load more" for lists > 50.
4. **Anomaly Alerts:** Add anomaly type and severity filters.
5. **Live URL Testing:** Run the same test against the live Databricks Apps URL when authenticated:
   ```bash
   BASE_URL=https://getnet-financial-closure-984752964297111.11.azure.databricksapps.com node qa-test-app.js
   ```

---

## Test Artifacts

- `qa-test-app.js` — Playwright-based QA script
- `qa-report.json` — Machine-readable test results
- `QA_TEST_REPORT.md` — This report
