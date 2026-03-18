#!/usr/bin/env node
/**
 * Extended QA Test - All pages requested for CDP app
 * Tests: Call Center, Campaign ROI, Audiences, Personalization, Ad Creative,
 *        CLV, Channel Attribution, Behavioral, Data Freshness, About, AI Chat
 */

const BASE_URL = process.env.BASE_URL || 'http://127.0.0.1:8081';

async function runTests() {
  let playwright;
  try {
    playwright = await import('playwright');
  } catch {
    console.error('Playwright not found. Install with: npm install playwright && npx playwright install chromium');
    process.exit(1);
  }

  const browser = await playwright.chromium.launch({ headless: true });
  const context = await browser.newContext({
    viewport: { width: 1920, height: 1080 },
    ignoreHTTPSErrors: true,
  });

  const consoleErrors = [];
  const consoleWarnings = [];
  context.on('console', (msg) => {
    const text = msg.text();
    if (msg.type() === 'error') consoleErrors.push(text);
    else if (msg.type() === 'warning') consoleWarnings.push(text);
  });

  const report = { pages: [], summary: { passed: 0, failed: 0, issues: [] } };
  const hasBadValue = (text) => {
    if (!text || text === 'undefined' || text === 'NaN') return true;
    return /^0$|^0\.0+$/.test(String(text).trim()) && text.length < 10;
  };

  const waitForLoad = async (page) => {
    await page.waitForLoadState('networkidle', { timeout: 10000 }).catch(() => {});
    await page.waitForTimeout(600);
  };

  try {
    const page = await context.newPage();
    console.log(`\n=== Navigating to ${BASE_URL} ===\n`);
    await page.goto(BASE_URL, { waitUntil: 'domcontentloaded', timeout: 15000 });
    await waitForLoad(page);

    const testPage = async (name, pageId, checks) => {
      console.log(`--- Testing ${name} ---`);
      await page.click(`[data-page="${pageId}"]`);
      await waitForLoad(page);
      const result = { name, issues: [] };
      for (const check of checks) {
        const r = await check(page, result);
        if (r) result.issues.push(r);
      }
      report.pages.push(result);
    };

    // 1. Call Center
    await testPage('Call Center', 'callcenter', [
      async (p, r) => {
        const kpis = await p.$$eval('.rounded-lg.border', els => els.slice(0, 6).map(e => e.textContent?.trim() || ''));
        r.kpis = kpis.length;
        if (kpis.some(t => hasBadValue(t) || t.includes('undefined'))) return 'KPI cards have undefined/NaN/zero';
      },
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 1) return 'No charts (expected Sentiment by Topic bar chart)';
      },
      async (p, r) => {
        const rows = await p.$$eval('table tbody tr', els => els.length);
        r.tableRows = rows;
        if (rows === 0) return 'Agent table has no rows';
      },
      async (p, r) => {
        const hasQueue = await p.$('text=Queue');
        if (!hasQueue) return 'Queue status section not found';
      },
    ]);

    // 2. Campaign ROI
    await testPage('Campaign ROI', 'roi', [
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 2) return 'Expected ROI bar chart + Outcomes doughnut, found ' + charts;
      },
      async (p, r) => {
        const rows = await p.$$eval('table tbody tr', els => els.length);
        r.tableRows = rows;
        if (rows === 0) return 'Campaign summary table has no rows';
      },
    ]);

    // 3. Audiences
    await testPage('Audiences', 'audiences', [
      async (p, r) => {
        const cards = await p.$$eval('.rounded-lg.border, [class*="card"]', els => els.length);
        r.cards = cards;
        if (cards < 3) return 'Expected audience segment cards with member counts';
      },
      async (p, r) => {
        const firstCard = await p.$('.rounded-lg.border, [class*="cursor-pointer"]');
        if (firstCard) {
          await firstCard.click();
          await page.waitForTimeout(500);
          const memberList = await p.$('table tbody tr, [class*="member"]');
          r.drilldown = !!memberList;
        }
      },
      async (p, r) => {
        const exportBtn = await p.$('a[href*="export/audience"], button:has-text("Export")');
        if (!exportBtn) return 'Export CSV per audience not found';
      },
    ]);

    // 4. Personalization
    await testPage('Personalization', 'personalization', [
      async (p, r) => {
        const summary = await p.$$eval('.rounded-lg.border, [class*="card"]', els => els.length);
        r.cards = summary;
        if (summary < 1) return 'Personalization summary not found';
      },
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 1) return 'Expected Propensity Tiers doughnut chart';
      },
      async (p, r) => {
        const merchantRow = await p.$('table tbody tr, [class*="merchant"]');
        if (merchantRow) {
          await merchantRow.click();
          await page.waitForTimeout(500);
          const signals = await p.$('text=signal, text=Signals');
          r.merchantSignals = !!signals;
        }
      },
    ]);

    // 5. Ad Creative
    await testPage('Ad Creative', 'creative', [
      async (p, r) => {
        const cards = await p.$$eval('.rounded-lg.border, [class*="card"]', els => els.length);
        r.cards = cards;
        if (cards < 1) return 'Expected AI-generated ad creative cards per segment';
      },
    ]);

    // 6. CLV
    await testPage('Customer Lifetime Value', 'clv', [
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 1) return 'Expected CLV by Tier bar chart';
      },
      async (p, r) => {
        const rows = await p.$$eval('table tbody tr', els => els.length);
        r.tableRows = rows;
        if (rows === 0) return 'Top CLV merchants table has no rows';
      },
    ]);

    // 7. Channel Attribution
    await testPage('Channel Attribution', 'attribution', [
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 2) return 'Expected Markov + Multi-Touch charts';
      },
      async (p, r) => {
        const rows = await p.$$eval('table tbody tr', els => els.length);
        r.tableRows = rows;
        if (rows === 0) return 'Channel table has no rows';
      },
    ]);

    // 8. Behavioral Segments
    await testPage('Behavioral Segments', 'behavioral', [
      async (p, r) => {
        const charts = await p.$$eval('canvas', els => els.length);
        r.charts = charts;
        if (charts < 1) return 'Expected Behavioral Clusters bar chart';
      },
      async (p, r) => {
        const cards = await p.$$eval('.rounded-lg.border', els => els.length);
        r.cards = cards;
        if (cards < 1) return 'Expected cluster detail cards';
      },
    ]);

    // 9. Data Freshness (navigate via header button, not sidebar)
    console.log('--- Testing Data Freshness ---');
    const freshnessBtn = await page.$('#freshness-btn');
    if (freshnessBtn) {
      await freshnessBtn.click();
      await waitForLoad(page);
    } else {
      await page.evaluate(() => typeof navigateTo === 'function' && navigateTo('freshness'));
      await waitForLoad(page);
    }
    const freshnessResult = { name: 'Data Freshness', issues: [] };
    {
      const rows = await page.$$eval('table tbody tr', els => els.length);
      freshnessResult.tableRows = rows;
      if (rows === 0) freshnessResult.issues.push('Table freshness status list empty');
      const dot = await page.$('#freshness-dot');
      if (!dot) freshnessResult.issues.push('Freshness indicator dot in header not found');
    }
    report.pages.push(freshnessResult);

    // 10. About
    await testPage('About this Solution', 'about', [
      async (p, r) => {
        const body = await p.textContent('body');
        if (!body?.toLowerCase().includes('architecture')) return 'Architecture diagram/section not found';
      },
      async (p, r) => {
        const body = await p.textContent('body');
        if (!body?.toLowerCase().includes('data source')) return 'Data sources table not found';
      },
      async (p, r) => {
        const body = await p.textContent('body');
        if (!body?.toLowerCase().includes('gold')) return 'Gold table catalog not found';
      },
    ]);

    // 11. AI Chat Panel
    console.log('--- Testing AI Chat Panel ---');
    await page.click('[data-page="dashboard"]');
    await waitForLoad(page);
    const chatFab = await page.$('#chat-fab');
    const chatResult = { name: 'AI Chat Panel', issues: [] };
    if (!chatFab) {
      chatResult.issues.push('Chat FAB button (#chat-fab) not found');
    } else {
      await chatFab.click();
      await page.waitForTimeout(600);
      const panel = await page.$('#ai-chat-panel');
      const panelVisible = panel && !(await panel.evaluate(el => el.classList.contains('hidden')));
      if (!panelVisible) chatResult.issues.push('Chat panel did not open');
      const agentToggle = await page.$('#mode-agent, #mode-genie');
      if (!agentToggle) chatResult.issues.push('Agent/Genie mode toggle not found');
      const input = await page.$('#chat-input');
      if (input) {
        await input.fill('What are top priority actions?');
        await page.waitForTimeout(300);
        const sendBtn = await page.$('button[onclick*="sendChat"]');
        if (sendBtn) {
          await sendBtn.click();
          await page.waitForTimeout(4500);
          const botMsg = await page.$('#chat-messages .prose, #chat-messages .bg-muted');
          if (botMsg) {
            const thumbs = await page.$$('button[title="Helpful"], button[title="Not helpful"]');
            if (thumbs.length === 0) chatResult.issues.push('Thumbs up/down feedback buttons not found');
          }
        } else {
          chatResult.issues.push('Send button not found');
        }
      } else {
        chatResult.issues.push('Chat input not found');
      }
    }
    report.pages.push(chatResult);

    report.consoleErrors = consoleErrors;
    report.consoleWarnings = consoleWarnings;
    report.summary.passed = report.pages.filter(p => p.issues.length === 0).length;
    report.summary.failed = report.pages.filter(p => p.issues.length > 0).length;
    report.pages.forEach(p => {
      if (p.issues.length) report.summary.issues.push(...p.issues.map(i => `[${p.name}] ${i}`));
    });

    return report;
  } finally {
    await browser.close();
  }
}

runTests()
  .then(report => {
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('       EXTENDED QA TEST REPORT - Getnet CDP App');
    console.log('═══════════════════════════════════════════════════════════\n');

    report.pages.forEach(p => {
      console.log(`\n## ${p.name}`);
      console.log(`   KPIs: ${p.kpis ?? 'N/A'}, Charts: ${p.charts ?? 'N/A'}, Table rows: ${p.tableRows ?? 'N/A'}, Cards: ${p.cards ?? 'N/A'}`);
      if (p.issues.length) {
        console.log(`   ISSUES: ${p.issues.join('; ')}`);
      } else {
        console.log('   ✓ All checks passed');
      }
    });

    console.log('\n--- Console Errors ---');
    (report.consoleErrors || []).forEach(e => console.log('  ERROR:', e));
    if (!report.consoleErrors?.length) console.log('  None');

    console.log('\n--- Summary ---');
    console.log(`   Passed: ${report.summary.passed}/${report.pages.length} pages`);
    console.log(`   Failed: ${report.summary.failed}/${report.pages.length} pages`);
    if (report.summary.issues?.length) console.log('   Issues:', report.summary.issues);

    require('fs').writeFileSync('qa-report-extended.json', JSON.stringify(report, null, 2));
    console.log('\nFull report: qa-report-extended.json');
  })
  .catch(err => {
    console.error('Test failed:', err);
    process.exit(1);
  });
