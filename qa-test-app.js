#!/usr/bin/env node
/**
 * QA Test Script for Getnet CDP Customer 360 App
 * Tests Dashboard, Merchants, NBA, Anomaly Alerts, Support Analytics
 * Run: npx playwright test qa-test-app.spec.js (or node qa-test-app.js with playwright)
 *
 * For local testing: Start app with CDP_DATA_SOURCE=mock on port 8765
 * For live URL: Set BASE_URL env var
 */

const BASE_URL = process.env.BASE_URL || 'http://127.0.0.1:8765';

async function runTests() {
  let playwright;
  try {
    playwright = await import('playwright');
  } catch {
    console.error('Playwright not found. Install with: npm init -y && npx playwright install chromium');
    process.exit(1);
  }

  const browser = await playwright.chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    ignoreHTTPSErrors: true,
  });

  const consoleLogs = [];
  const consoleErrors = [];
  context.on('console', (msg) => {
    const text = msg.text();
    if (msg.type() === 'error') consoleErrors.push(text);
    else consoleLogs.push(`[${msg.type()}] ${text}`);
  });

  const report = { pages: [], summary: { passed: 0, failed: 0, issues: [] } };

  try {
    const page = await context.newPage();

    // Helper: wait for page to stabilize
    const waitForLoad = async () => {
      await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
      await page.waitForTimeout(500);
    };

    // Helper: check for bad values in KPI/text
    const hasBadValue = (text) => {
      if (!text || text === 'undefined' || text === 'NaN') return true;
      return /^0$|^0\.0+$/.test(String(text).trim()) && text.length < 10;
    };

    // Navigate
    console.log(`\n=== Navigating to ${BASE_URL} ===\n`);
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await waitForLoad();

    // ─── 1. DASHBOARD ─────────────────────────────────────────
    console.log('--- Testing Dashboard ---');
    await page.click('[data-page="dashboard"]');
    await waitForLoad();

    const dashKpis = await page.$$eval('.rounded-lg.border', (cards) =>
      cards.slice(0, 8).map((c) => c.textContent?.trim() || '')
    );
    const dashCharts = await page.$$eval('canvas', (els) => els.length);
    const dashIssues = [];
    if (dashKpis.some((t) => hasBadValue(t) || t.includes('undefined')))
      dashIssues.push('KPI cards may have undefined/NaN/zero values');
    if (dashCharts < 2) dashIssues.push(`Expected 2 charts, found ${dashCharts}`);

    await page.screenshot({ path: 'qa-screenshot-dashboard.png', fullPage: true });
    report.pages.push({
      name: 'Dashboard',
      kpis: dashKpis.length,
      charts: dashCharts,
      issues: dashIssues,
      screenshot: 'qa-screenshot-dashboard.png',
    });

    // ─── 2. MERCHANTS ────────────────────────────────────────
    console.log('--- Testing Merchants ---');
    await page.click('[data-page="merchants"]');
    await waitForLoad();

    const merchantRows = await page.$$eval('table tbody tr', (rows) => rows.length);
    const searchInput = await page.$('input[placeholder*="Filter"]');
    if (searchInput) {
      await searchInput.fill('Café');
      await page.waitForTimeout(400); // debounce
    }
    const merchantRowsAfterSearch = await page.$$eval('table tbody tr', (rows) => rows.length);

    const exportMerchants = await page.$('a[href*="export/merchants"]');
    const merchantIssues = [];
    if (merchantRows === 0) merchantIssues.push('No merchant rows in table');
    if (!exportMerchants) merchantIssues.push('Export CSV link not found');

    await page.screenshot({ path: 'qa-screenshot-merchants.png', fullPage: true });
    report.pages.push({
      name: 'Merchants',
      tableRows: merchantRows,
      rowsAfterSearch: merchantRowsAfterSearch,
      exportButton: !!exportMerchants,
      issues: merchantIssues,
      screenshot: 'qa-screenshot-merchants.png',
    });

    // Click first merchant to open detail sheet
    const firstRow = await page.$('table tbody tr');
    if (firstRow) {
      await firstRow.click();
      await page.waitForTimeout(500);
      const sheet = await page.$('#merchant-sheet');
      const sheetVisible = sheet && (await sheet.isVisible());
      if (!sheetVisible) merchantIssues.push('Merchant detail sheet did not open');
      const closeBtn = await page.$('button[onclick*="closeMerchantSheet"]');
      if (closeBtn) await closeBtn.click();
    }

    // ─── 3. NEXT BEST ACTIONS ─────────────────────────────────
    console.log('--- Testing NBA ---');
    await page.click('[data-page="nba"]');
    await waitForLoad();

    const nbaRows = await page.$$eval('table tbody tr', (rows) => rows.length);
    const exportNba = await page.$('a[href*="export/nba"]');
    const nbaIssues = [];
    if (nbaRows === 0) nbaIssues.push('No NBA queue rows');
    if (!exportNba) nbaIssues.push('Export CSV link not found');

    await page.screenshot({ path: 'qa-screenshot-nba.png', fullPage: true });
    report.pages.push({
      name: 'Next Best Actions',
      tableRows: nbaRows,
      exportButton: !!exportNba,
      issues: nbaIssues,
      screenshot: 'qa-screenshot-nba.png',
    });

    // ─── 4. ANOMALY ALERTS ───────────────────────────────────
    console.log('--- Testing Anomaly Alerts ---');
    await page.click('[data-page="anomalies"]');
    await waitForLoad();

    const anomalyKpis = await page.$$eval('.rounded-lg.border', (cards) =>
      cards.slice(0, 6).map((c) => c.textContent?.trim() || '')
    );
    const anomalyCharts = await page.$$eval('canvas', (els) => els.length);
    const anomalyRows = await page.$$eval('table tbody tr', (rows) => rows.length);
    const exportAnomaly = await page.$('a[href*="export/anomaly-alerts"]');
    const anomalyIssues = [];
    if (anomalyKpis.some((t) => hasBadValue(t))) anomalyIssues.push('KPI cards may have bad values');
    if (anomalyCharts < 2) anomalyIssues.push(`Expected 2 charts, found ${anomalyCharts}`);
    if (!exportAnomaly) anomalyIssues.push('Export CSV link not found');

    await page.screenshot({ path: 'qa-screenshot-anomalies.png', fullPage: true });
    report.pages.push({
      name: 'Anomaly Alerts',
      kpis: anomalyKpis.length,
      charts: anomalyCharts,
      tableRows: anomalyRows,
      exportButton: !!exportAnomaly,
      issues: anomalyIssues,
      screenshot: 'qa-screenshot-anomalies.png',
    });

    // ─── 5. SUPPORT ANALYTICS ──────────────────────────────────
    console.log('--- Testing Support Analytics ---');
    await page.click('[data-page="support"]');
    await waitForLoad();

    const supportKpis = await page.$$eval('.rounded-lg.border', (cards) =>
      cards.slice(0, 4).map((c) => c.textContent?.trim() || '')
    );
    const supportCharts = await page.$$eval('canvas', (els) => els.length);
    const supportRows = await page
      .$$eval('#support-merchants-table table tbody tr', (r) => r.length)
      .catch(() => 0);
    const supportIssues = [];
    if (supportKpis.some((t) => hasBadValue(t))) supportIssues.push('KPI cards may have bad values');
    if (supportCharts < 1) supportIssues.push(`Expected Support Quality chart, found ${supportCharts}`);
    if (supportRows === 0) supportIssues.push('No rows in merchant support table');

    await page.screenshot({ path: 'qa-screenshot-support.png', fullPage: true });
    report.pages.push({
      name: 'Support Analytics',
      kpis: supportKpis.length,
      charts: supportCharts,
      tableRows: supportRows,
      issues: supportIssues,
      screenshot: 'qa-screenshot-support.png',
    });

    // Collect console errors
    report.consoleErrors = consoleErrors;
    report.consoleWarnings = consoleLogs.filter((l) => l.includes('warn'));

    // Summary
    report.summary.passed = report.pages.filter((p) => p.issues.length === 0).length;
    report.summary.failed = report.pages.filter((p) => p.issues.length > 0).length;
    report.pages.forEach((p) => {
      if (p.issues.length) report.summary.issues.push(...p.issues.map((i) => `[${p.name}] ${i}`));
    });

    return report;
  } finally {
    await browser.close();
  }
}

runTests()
  .then((report) => {
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('              QA TEST REPORT - Getnet CDP App');
    console.log('═══════════════════════════════════════════════════════════\n');

    report.pages.forEach((p) => {
      console.log(`\n## ${p.name}`);
      console.log(`   KPIs: ${p.kpis ?? 'N/A'}, Charts: ${p.charts ?? 'N/A'}, Table rows: ${p.tableRows ?? 'N/A'}`);
      if (p.issues.length) {
        console.log(`   ISSUES: ${p.issues.join('; ')}`);
      } else {
        console.log('   ✓ All checks passed');
      }
      if (p.screenshot) console.log(`   Screenshot: ${p.screenshot}`);
    });

    console.log('\n--- Console Errors ---');
    if (report.consoleErrors?.length) {
      report.consoleErrors.forEach((e) => console.log('  ERROR:', e));
    } else {
      console.log('  None');
    }

    console.log('\n--- Summary ---');
    console.log(`   Passed: ${report.summary.passed}/5 pages`);
    console.log(`   Failed: ${report.summary.failed}/5 pages`);
    if (report.summary.issues.length) {
      console.log('   All issues:', report.summary.issues);
    }

    // Write JSON report
    const fs = require('fs');
    fs.writeFileSync('qa-report.json', JSON.stringify(report, null, 2));
    console.log('\nFull report saved to qa-report.json');
  })
  .catch((err) => {
    console.error('Test failed:', err.message);
    process.exit(1);
  });
