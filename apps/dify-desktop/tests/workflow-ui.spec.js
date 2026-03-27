const path = require('path');
const os = require('os');
const fs = require('fs');
const http = require('http');
const { test, expect, _electron: electron } = require('@playwright/test');

async function openWorkflow(options = {}) {
  const appDir = path.resolve(__dirname, '..');
  const args = [appDir, '--workflow', '--workflow-debug-api'];
  if (options.admin) args.push('--workflow-admin');
  const envOverrides = options.env && typeof options.env === 'object' ? options.env : {};
  const previousEnv = new Map();
  Object.entries(envOverrides).forEach(([key, value]) => {
    previousEnv.set(key, Object.prototype.hasOwnProperty.call(process.env, key) ? process.env[key] : undefined);
    process.env[key] = String(value);
  });
  const electronApp = await electron.launch({
    args,
    env: { ...process.env, ...envOverrides },
  });
  previousEnv.forEach((value, key) => {
    if (typeof value === 'undefined') delete process.env[key];
    else process.env[key] = value;
  });
  const page = await electronApp.firstWindow();
  return { electronApp, page };
}

function governanceJson(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

async function withGovernanceMock(runItems, fn) {
  const items = Array.isArray(runItems) ? runItems : [];
  const versions = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (req.method === 'GET' && url.pathname === '/governance/meta/control-plane') {
      governanceJson(res, 200, {
        ok: true,
        boundary: {
          governance_surfaces: [
            {
              capability: 'workflow_run_audit',
              route_prefix: '/governance/workflow-runs',
              owned_route_prefixes: ['/governance/workflow-runs', '/governance/workflow-audit-events'],
            },
            {
              capability: 'workflow_versions',
              route_prefix: '/governance/workflow-versions',
              owned_route_prefixes: ['/governance/workflow-versions'],
            },
          ],
        },
      });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/governance/workflow-runs') {
      governanceJson(res, 200, { ok: true, items });
      return;
    }
    if (req.method === 'GET' && url.pathname === '/governance/workflow-versions') {
      governanceJson(res, 200, { ok: true, items: versions });
      return;
    }
    if (req.method === 'PUT' && url.pathname.startsWith('/governance/workflow-versions/')) {
      let body = '';
      req.on('data', (chunk) => {
        body += String(chunk || '');
      });
      req.on('end', () => {
        const payload = body ? JSON.parse(body) : {};
        const version = payload?.version && typeof payload.version === 'object' ? payload.version : {};
        const id = decodeURIComponent(url.pathname.split('/').pop() || '');
        const item = { ...version, version_id: String(version.version_id || id) };
        const index = versions.findIndex((entry) => String(entry?.version_id || '') === item.version_id);
        if (index >= 0) versions[index] = item;
        else versions.unshift(item);
        governanceJson(res, 200, { ok: true, item });
      });
      return;
    }
    governanceJson(res, 404, { ok: false, error: `mock route not found: ${req.method} ${url.pathname}` });
  });
  await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
  const { port } = server.address();
  const baseUrl = `http://127.0.0.1:${port}`;
  try {
    return await fn(baseUrl);
  } finally {
    await new Promise((resolve) => server.close(resolve));
  }
}

test('workflow studio renders baseline graph', async () => {
  const { electronApp, page } = await openWorkflow();
  await expect(page.locator('h2')).toHaveText(/Workflow Studio/);
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.graph === 'function');
  const expectedNodes = await page.evaluate(() => {
    const g = window.__aiwfDebug.graph();
    return Array.isArray(g?.nodes) ? g.nodes.length : 0;
  });
  await expect(page.locator('.node')).toHaveCount(expectedNodes);
  const edgeCount = await page.locator('#edges path.edge-line').count();
  expect(edgeCount).toBeGreaterThan(0);
  await electronApp.close();
});

test('horizontal same-row nodes render visible edge path', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const edgeInfo = await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'hline',
      name: 'hline',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 40, y: 120 },
        { id: 'n2', type: 'clean_md', x: 420, y: 120 },
      ],
      edges: [{ from: 'n1', to: 'n2' }],
    });
    const p = document.querySelector('#edges path.edge-line');
    if (!p) return { ok: false, reason: 'missing_path' };
    const d = String(p.getAttribute('d') || '');
    const bb = p.getBBox();
    return { ok: true, d, w: bb.width, h: bb.height };
  });
  expect(edgeInfo.ok).toBeTruthy();
  expect(edgeInfo.d).toContain('M');
  expect(edgeInfo.d).toContain('L');
  expect(edgeInfo.w).toBeGreaterThan(120);
  await electronApp.close();
});

test('unlink button removes edges among selected nodes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => document.querySelectorAll('#edges path.edge-line').length > 0);
  const before = await page.locator('#edges path.edge-line').count();
  expect(before).toBeGreaterThan(0);
  await page.click('.node[data-id="n1"]');
  await page.click('.node[data-id="n2"]', { modifiers: ['Shift'] });
  await page.click('.node[data-id="n3"]', { modifiers: ['Shift'] });
  await expect(page.locator('.node.selected')).toHaveCount(3);
  await page.click('#btnUnlinkSelected');
  const after = await page.locator('#edges path.edge-line').count();
  expect(after).toBe(before - 2);
  await electronApp.close();
});

test('re-linking same direction toggles edge off', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const out = await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'toggle_edge',
      name: 'toggle_edge',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 40, y: 120 },
        { id: 'n2', type: 'clean_md', x: 420, y: 120 },
      ],
      edges: [],
    });
    const r1 = api.tryLink('n1', 'n2');
    const e1 = api.graph().edges.length;
    const r2 = api.tryLink('n1', 'n2');
    const e2 = api.graph().edges.length;
    return { r1, r2, e1, e2 };
  });
  expect(out.r1?.ok).toBeTruthy();
  expect(out.e1).toBe(1);
  expect(out.r2?.ok).toBeTruthy();
  expect(out.r2?.toggled).toBeTruthy();
  expect(out.e2).toBe(0);
  await electronApp.close();
});

test('edge path stays valid after zoom and horizontal layout changes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  await page.click('#btnZoomIn');
  await page.click('#btnZoomIn');
  const edgeInfo = await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'hline_zoomed',
      name: 'hline_zoomed',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 40, y: 120 },
        { id: 'n2', type: 'clean_md', x: 520, y: 120 },
      ],
      edges: [{ from: 'n1', to: 'n2' }],
    });
    const p = document.querySelector('#edges path.edge-line');
    if (!p) return { ok: false, reason: 'missing_path' };
    const d = String(p.getAttribute('d') || '');
    return { ok: true, d };
  });
  expect(edgeInfo.ok).toBeTruthy();
  expect(edgeInfo.d).not.toContain('NaN');
  expect(edgeInfo.d).toMatch(/^M\s+/);
  await electronApp.close();
});

test('zoom controls update zoom label', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.click('#btnZoomIn');
  await expect(page.locator('#zoomText')).not.toHaveText('100%');
  await page.click('#btnZoomReset');
  await expect(page.locator('#zoomText')).toHaveText('100%');
  await electronApp.close();
});

test('fit view button rescales large graphs', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'fit_view',
      name: 'fit_view',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 0, y: 120 },
        { id: 'n2', type: 'clean_md', x: 1680, y: 1200 },
        { id: 'n3', type: 'compute_rust', x: 3120, y: 1680 },
      ],
      edges: [
        { from: 'n1', to: 'n2' },
        { from: 'n2', to: 'n3' },
      ],
    });
  });
  await page.click('#btnFitCanvas');
  await expect(page.locator('#zoomText')).not.toHaveText('100%');
  await electronApp.close();
});

test('touch drag updates node position', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const out = await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'touch_drag',
      name: 'touch_drag',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 120, y: 160 },
      ],
      edges: [],
    });
    const header = document.querySelector('.node[data-id="n1"] .node-hd');
    if (!header) return { ok: false, reason: 'missing_header' };
    const rect = header.getBoundingClientRect();
    const startX = rect.left + rect.width / 2;
    const startY = rect.top + rect.height / 2;
    header.dispatchEvent(new PointerEvent('pointerdown', {
      bubbles: true,
      cancelable: true,
      pointerId: 11,
      pointerType: 'touch',
      isPrimary: true,
      clientX: startX,
      clientY: startY,
    }));
    window.dispatchEvent(new PointerEvent('pointermove', {
      bubbles: true,
      cancelable: true,
      pointerId: 11,
      pointerType: 'touch',
      isPrimary: true,
      clientX: startX + 180,
      clientY: startY + 120,
    }));
    window.dispatchEvent(new PointerEvent('pointerup', {
      bubbles: true,
      cancelable: true,
      pointerId: 11,
      pointerType: 'touch',
      isPrimary: true,
      clientX: startX + 180,
      clientY: startY + 120,
    }));
    const node = api.graph().nodes.find((item) => item.id === 'n1');
    return { ok: true, x: node?.x, y: node?.y };
  });
  expect(out.ok).toBeTruthy();
  expect(out.x).toBeGreaterThan(120);
  expect(out.y).toBeGreaterThan(160);
  await electronApp.close();
});

test('minimap bitmap tracks css size and device pixel ratio', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const out = await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'minimap_dpi',
      name: 'minimap_dpi',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 0, y: 120 },
        { id: 'n2', type: 'clean_md', x: 1420, y: 920 },
      ],
      edges: [{ from: 'n1', to: 'n2' }],
    });
    const canvas = document.getElementById('minimap');
    const rect = canvas.getBoundingClientRect();
    return {
      dpr: window.devicePixelRatio || 1,
      cssW: Math.round(rect.width),
      cssH: Math.round(rect.height),
      pixelW: canvas.width,
      pixelH: canvas.height,
    };
  });
  expect(Math.abs(out.pixelW - Math.round(out.cssW * out.dpr))).toBeLessThanOrEqual(1);
  expect(Math.abs(out.pixelH - Math.round(out.cssH * out.dpr))).toBeLessThanOrEqual(1);
  await electronApp.close();
});

test('batch align requires multi-select', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.click('#btnAlignLeft');
  await expect(page.locator('#status')).toContainText('至少两个节点');
  await electronApp.close();
});

test('align and distribute buttons affect selected nodes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'arrange_case',
      name: 'arrange_case',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 120, y: 240 },
        { id: 'n2', type: 'clean_md', x: 560, y: 80 },
        { id: 'n3', type: 'compute_rust', x: 320, y: 420 },
      ],
      edges: [],
    });
  });
  const n1 = page.locator('.node[data-id="n1"]');
  const n2 = page.locator('.node[data-id="n2"]');
  const n3 = page.locator('.node[data-id="n3"]');

  await n1.click();
  await n2.click({ modifiers: ['Shift'] });
  await n3.click({ modifiers: ['Shift'] });
  await expect(page.locator('.node.selected')).toHaveCount(3);

  await page.click('#btnAlignTop');
  const b1 = await n1.boundingBox();
  const b2 = await n2.boundingBox();
  const b3 = await n3.boundingBox();
  expect(b1).toBeTruthy();
  expect(b2).toBeTruthy();
  expect(b3).toBeTruthy();
  expect(Math.abs(b1.y - b2.y)).toBeLessThanOrEqual(2);
  expect(Math.abs(b2.y - b3.y)).toBeLessThanOrEqual(2);

  await page.click('#btnDistributeH');
  const a1 = await n1.boundingBox();
  const a2 = await n2.boundingBox();
  const a3 = await n3.boundingBox();
  expect(a1).toBeTruthy();
  expect(a2).toBeTruthy();
  expect(a3).toBeTruthy();
  const xs = [a1.x, a2.x, a3.x].sort((x, y) => x - y);
  const d12 = xs[1] - xs[0];
  const d23 = xs[2] - xs[1];
  expect(Math.abs(d12 - d23)).toBeLessThanOrEqual(4);

  await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'arrange_case_v',
      name: 'arrange_case_v',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 120, y: 120 },
        { id: 'n2', type: 'clean_md', x: 120, y: 420 },
        { id: 'n3', type: 'compute_rust', x: 120, y: 260 },
      ],
      edges: [],
    });
  });
  await n1.click();
  await n2.click({ modifiers: ['Shift'] });
  await n3.click({ modifiers: ['Shift'] });
  await page.click('#btnDistributeV');
  const v1 = await n1.boundingBox();
  const v2 = await n2.boundingBox();
  const v3 = await n3.boundingBox();
  expect(v1).toBeTruthy();
  expect(v2).toBeTruthy();
  expect(v3).toBeTruthy();
  const ys = [v1.y, v2.y, v3.y].sort((x, y) => x - y);
  const dv12 = ys[1] - ys[0];
  const dv23 = ys[2] - ys[1];
  expect(Math.abs(dv12 - dv23)).toBeLessThanOrEqual(24);

  await electronApp.close();
});

test('distribution keeps non-overlap for dense selected nodes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  await page.evaluate(() => {
    const api = window.__aiwfDebug;
    api.setGraph({
      workflow_id: 'dense_overlap_case',
      name: 'dense_overlap_case',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 120, y: 120 },
        { id: 'n2', type: 'clean_md', x: 140, y: 130 },
        { id: 'n3', type: 'compute_rust', x: 160, y: 140 },
        { id: 'n4', type: 'ai_refine', x: 180, y: 150 },
      ],
      edges: [],
    });
    api.selectNodes(['n1', 'n2', 'n3', 'n4']);
  });
  await expect(page.locator('.node.selected')).toHaveCount(4);
  await page.click('#btnDistributeH');

  const p1 = await page.locator('.node[data-id="n1"]').boundingBox();
  const p2 = await page.locator('.node[data-id="n2"]').boundingBox();
  const p3 = await page.locator('.node[data-id="n3"]').boundingBox();
  const p4 = await page.locator('.node[data-id="n4"]').boundingBox();
  expect(p1).toBeTruthy();
  expect(p2).toBeTruthy();
  expect(p3).toBeTruthy();
  expect(p4).toBeTruthy();
  const xs = [p1.x, p2.x, p3.x, p4.x].sort((a, b) => a - b);
  expect(xs[1] - xs[0]).toBeGreaterThanOrEqual(200);
  expect(xs[2] - xs[1]).toBeGreaterThanOrEqual(200);
  expect(xs[3] - xs[2]).toBeGreaterThanOrEqual(200);
  await electronApp.close();
});

test('export flow json prints graph payload', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.click('#btnExport');
  const txt = await page.locator('#log').innerText();
  expect(txt).toContain('"nodes"');
  expect(txt).toContain('"edges"');
  await electronApp.close();
});

test('delete button removes node and connected edges', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.graph === 'function');
  const beforeNodeCount = await page.evaluate(() => {
    const g = window.__aiwfDebug.graph();
    return Array.isArray(g?.nodes) ? g.nodes.length : 0;
  });
  await expect(page.locator('.node')).toHaveCount(beforeNodeCount);
  const edgeCountBefore = await page.locator('#edges path.edge-line').count();
  expect(edgeCountBefore).toBeGreaterThan(0);

  await page.click('.node[data-id="n2"] .mini-btn.del');
  await expect(page.locator('.node[data-id="n2"]')).toHaveCount(0);
  await expect(page.locator('.node')).toHaveCount(beforeNodeCount - 1);
  const edgeCountAfter = await page.locator('#edges path.edge-line').count();
  expect(edgeCountAfter).toBeLessThan(edgeCountBefore);
  await electronApp.close();
});

test('minimap element exists and has size', async () => {
  const { electronApp, page } = await openWorkflow();
  await expect(page.locator('#minimap')).toBeVisible();
  const size = await page.evaluate(() => {
    const c = document.getElementById('minimap');
    return { w: c.width, h: c.height };
  });
  expect(size.w).toBeGreaterThan(0);
  expect(size.h).toBeGreaterThan(0);
  await electronApp.close();
});

test('save/load workflow via mock dialog path', async () => {
  await withGovernanceMock([], async (glueUrl) => {
    const { electronApp, page } = await openWorkflow({
      env: { AIWF_GLUE_URL: glueUrl },
    });
    await page.evaluate(async (nextGlueUrl) => {
      const cfg = await window.aiwfDesktop.getConfig();
      await window.aiwfDesktop.saveConfig({ ...cfg, glueUrl: nextGlueUrl });
    }, glueUrl);
    const fp = path.join(os.tmpdir(), `aiwf_workflow_${Date.now()}.json`);
    const saved = await page.evaluate(async (filePath) => {
      const g = {
        workflow_id: 't1',
        version: '1.0.0',
        name: '测试流',
        nodes: [{ id: 'n1', type: 'ingest_files', x: 1, y: 2 }],
        edges: [],
      };
      return await window.aiwfDesktop.saveWorkflow(g, 't1', { mock: true, path: filePath });
    }, fp);
    expect(saved && saved.ok).toBeTruthy();
    expect(fs.existsSync(fp)).toBeTruthy();

    const loaded = await page.evaluate(async (filePath) => {
      return await window.aiwfDesktop.loadWorkflow({ mock: true, path: filePath });
    }, fp);
    expect(loaded && loaded.ok).toBeTruthy();
    expect(loaded.graph && loaded.graph.workflow_id).toBe('t1');
    await electronApp.close();
  });
});

test('marquee selection selects multiple nodes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  await page.evaluate(() => {
    window.__aiwfDebug.setGraph({
      workflow_id: 'marquee',
      name: 'marquee',
      nodes: [
        { id: 'n1', type: 'ingest_files', x: 40, y: 120 },
        { id: 'n2', type: 'clean_md', x: 360, y: 120 },
        { id: 'n3', type: 'compute_rust', x: 980, y: 520 },
      ],
      edges: [{ from: 'n1', to: 'n2' }],
    });
  });
  await page.locator('#canvasWrap').scrollIntoViewIfNeeded();
  const box1 = await page.locator('.node[data-id="n1"]').boundingBox();
  const box2 = await page.locator('.node[data-id="n2"]').boundingBox();
  expect(box1).toBeTruthy();
  expect(box2).toBeTruthy();

  const x0 = Math.min(box1.x, box2.x) - 12;
  const y0 = Math.min(box1.y, box2.y) - 12;
  const x1 = Math.max(box1.x + box1.width, box2.x + box2.width) + 12;
  const y1 = Math.max(box1.y + box1.height, box2.y + box2.height) + 12;

  await page.mouse.move(x0, y0);
  await page.mouse.down();
  await page.mouse.move(x1, y1);
  await page.mouse.up();

  await expect(page.locator('.node.selected')).toHaveCount(2);
  await expect(page.locator('.node[data-id="n1"].selected')).toHaveCount(1);
  await expect(page.locator('.node[data-id="n2"].selected')).toHaveCount(1);
  await electronApp.close();
});

test('multi-drag keeps relative movement across selected nodes', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.locator('#canvasWrap').scrollIntoViewIfNeeded();

  const beforeA = await page.locator('.node[data-id="n1"]').boundingBox();
  const beforeB = await page.locator('.node[data-id="n2"]').boundingBox();
  expect(beforeA).toBeTruthy();
  expect(beforeB).toBeTruthy();

  const box1 = await page.locator('.node[data-id="n1"]').boundingBox();
  const box2 = await page.locator('.node[data-id="n2"]').boundingBox();
  expect(box1).toBeTruthy();
  expect(box2).toBeTruthy();
  const x0 = Math.min(box1.x, box2.x) - 12;
  const y0 = Math.min(box1.y, box2.y) - 12;
  const x1 = Math.max(box1.x + box1.width, box2.x + box2.width) + 12;
  const y1 = Math.max(box1.y + box1.height, box2.y + box2.height) + 12;
  await page.mouse.move(x0, y0);
  await page.mouse.down();
  await page.mouse.move(x1, y1);
  await page.mouse.up();
  await expect(page.locator('.node[data-id="n1"].selected')).toHaveCount(1);
  await expect(page.locator('.node[data-id="n2"].selected')).toHaveCount(1);

  const h1 = await page.locator('.node[data-id="n1"] .node-hd').boundingBox();
  expect(h1).toBeTruthy();
  const sx = h1.x + h1.width / 2;
  const sy = h1.y + h1.height / 2;
  await page.mouse.move(sx, sy);
  await page.mouse.down();
  await page.mouse.move(sx + 120, sy + 72, { steps: 8 });
  await page.mouse.up();

  const afterA = await page.locator('.node[data-id="n1"]').boundingBox();
  const afterB = await page.locator('.node[data-id="n2"]').boundingBox();
  expect(afterA).toBeTruthy();
  expect(afterB).toBeTruthy();

  const d1x = afterA.x - beforeA.x;
  const d1y = afterA.y - beforeA.y;
  const d2x = afterB.x - beforeB.x;
  const d2y = afterB.y - beforeB.y;
  expect(Math.abs(d1x - d2x)).toBeLessThanOrEqual(24);
  expect(Math.abs(d1y - d2y)).toBeLessThanOrEqual(24);
  expect(Math.abs(d1x)).toBeGreaterThan(24);
  expect(Math.abs(d1y)).toBeGreaterThan(24);
  await electronApp.close();
});

test('router reports bounded fallback ratio on clustered graph', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const stat = await page.evaluate(async () => {
    const nodes = [];
    const edges = [];
    for (let i = 0; i < 24; i += 1) {
      const col = i % 6;
      const row = Math.floor(i / 6);
      nodes.push({ id: `n${i}`, type: 'clean_md', x: 80 + col * 220, y: 80 + row * 150 });
    }
    for (let i = 0; i < 20; i += 1) edges.push({ from: `n${i}`, to: `n${i + 4}` });
    for (let i = 0; i < 12; i += 1) edges.push({ from: `n${i}`, to: `n${23 - i}` });
    window.__aiwfDebug.setGraph({ workflow_id: 'clustered_router', name: 'clustered', nodes, edges });
    await new Promise((r) => setTimeout(r, 80));
    return window.__aiwfDebug.routeStats();
  });
  expect(stat.edges).toBeGreaterThan(20);
  expect(stat.solved).toBeGreaterThan(20);
  expect(stat.fallback_ratio).toBeLessThan(0.85);
  await electronApp.close();
});

test('router replay cases stay within per-case fallback bounds', async () => {
  const { electronApp, page } = await openWorkflow();
  await page.waitForFunction(() => !!window.__aiwfDebug && typeof window.__aiwfDebug.setGraph === 'function');
  const casesPath = path.join(__dirname, 'fixtures', 'route_replay_cases.json');
  const raw = fs.readFileSync(casesPath, 'utf8').replace(/^\uFEFF/, '');
  const cases = JSON.parse(raw);
  const legacyNodeTypeAliases = {
    office_output: 'office_slot_fill_v1',
    publish: 'manual_review',
  };
  for (const item of cases) {
    const stat = await page.evaluate(async (payload) => {
      window.__aiwfDebug.setGraph(payload.graph);
      await new Promise((r) => setTimeout(r, 100));
      return window.__aiwfDebug.routeStats();
    }, {
      ...item,
      graph: {
        ...(item.graph || {}),
        nodes: Array.isArray(item?.graph?.nodes)
          ? item.graph.nodes.map((node) => ({
              ...node,
              type: legacyNodeTypeAliases[node?.type] || node?.type,
            }))
          : [],
      },
    });
    expect(stat.solved).toBeGreaterThan(0);
    expect(stat.fallback_ratio).toBeLessThan(item.max_fallback_ratio);
  }
  await electronApp.close();
});

test('quality gate filters and export format persist after reload', async () => {
  await withGovernanceMock([{
    ts: '2026-03-25T09:00:00Z',
    run_id: 'run_gate_blocked',
    workflow_id: 'wf_gate',
    result: {
      ok: false,
      status: 'quality_blocked',
      quality_gate: { blocked: true, passed: false, issues: ['missing_amount'] },
    },
  }], async (glueUrl) => {
    const { electronApp, page } = await openWorkflow({
      admin: true,
      env: { AIWF_GLUE_URL: glueUrl },
    });
    await page.evaluate(async (nextGlueUrl) => {
      const cfg = await window.aiwfDesktop.getConfig();
      await window.aiwfDesktop.saveConfig({ ...cfg, glueUrl: nextGlueUrl });
    }, glueUrl);
    await expect(page.locator('#qualityGateRunIdFilter')).toBeVisible();

    await page.fill('#qualityGateRunIdFilter', 'run_abc_123');
    await page.selectOption('#qualityGateStatusFilter', 'blocked');
    await page.selectOption('#qualityGateExportFormat', 'json');

    await page.reload();
    await expect(page.locator('h2')).toHaveText(/Workflow Studio/);
    await expect(page.locator('#qualityGateRunIdFilter')).toHaveValue('run_abc_123');
    await expect(page.locator('#qualityGateStatusFilter')).toHaveValue('blocked');
    await expect(page.locator('#qualityGateExportFormat')).toHaveValue('json');

    await electronApp.close();
  });
});

test('quality gate export writes json and md reports via mock path', async () => {
  await withGovernanceMock([{
    ts: '2026-03-25T09:00:00Z',
    run_id: 'run_gate_blocked',
    workflow_id: 'wf_gate',
    result: {
      ok: false,
      status: 'quality_blocked',
      quality_gate: { blocked: true, passed: false, issues: ['missing_amount'] },
    },
  }, {
    ts: '2026-03-25T09:05:00Z',
    run_id: 'run_gate_pass',
    workflow_id: 'wf_gate',
    result: {
      ok: true,
      status: 'done',
      quality_gate: { blocked: false, passed: true, issues: [] },
    },
  }], async (glueUrl) => {
    const { electronApp, page } = await openWorkflow({
      admin: true,
      env: { AIWF_GLUE_URL: glueUrl },
    });
    await page.evaluate(async (nextGlueUrl) => {
      const cfg = await window.aiwfDesktop.getConfig();
      await window.aiwfDesktop.saveConfig({ ...cfg, glueUrl: nextGlueUrl });
    }, glueUrl);
    const fpJson = path.join(os.tmpdir(), `aiwf_quality_gate_${Date.now()}.json`);
    const fpMd = path.join(os.tmpdir(), `aiwf_quality_gate_${Date.now()}.md`);

    const out = await page.evaluate(async ({ jsonPath, mdPath }) => {
      const r1 = await window.aiwfDesktop.exportWorkflowQualityGateReports({
        mock: true,
        path: jsonPath,
        format: 'json',
        limit: 100,
        filter: { run_id: '', status: 'all' },
      });
      const r2 = await window.aiwfDesktop.exportWorkflowQualityGateReports({
        mock: true,
        path: mdPath,
        format: 'md',
        limit: 100,
        filter: { run_id: '', status: 'all' },
      });
      return { r1, r2 };
    }, { jsonPath: fpJson, mdPath: fpMd });

    expect(out?.r1?.ok).toBeTruthy();
    expect(out?.r2?.ok).toBeTruthy();
    expect(fs.existsSync(fpJson)).toBeTruthy();
    expect(fs.existsSync(fpMd)).toBeTruthy();

    const jsonText = fs.readFileSync(fpJson, 'utf8');
    const jsonObj = JSON.parse(jsonText);
    expect(typeof jsonObj?.exported_at).toBe('string');
    expect(typeof jsonObj?.total).toBe('number');
    expect(Array.isArray(jsonObj?.items)).toBeTruthy();

    const mdText = fs.readFileSync(fpMd, 'utf8');
    expect(mdText).toContain('# AIWF 质量门禁报告');
    expect(mdText).toContain('| Run | 状态 | 问题 | 时间 |');

    await electronApp.close();
  });
});

test('quality gate export returns readable error on invalid target path', async () => {
  await withGovernanceMock([{
    ts: '2026-03-25T09:00:00Z',
    run_id: 'run_gate_blocked',
    workflow_id: 'wf_gate',
    result: {
      ok: false,
      status: 'quality_blocked',
      quality_gate: { blocked: true, passed: false, issues: ['missing_amount'] },
    },
  }], async (glueUrl) => {
    const { electronApp, page } = await openWorkflow({
      admin: true,
      env: { AIWF_GLUE_URL: glueUrl },
    });
    await page.evaluate(async (nextGlueUrl) => {
      const cfg = await window.aiwfDesktop.getConfig();
      await window.aiwfDesktop.saveConfig({ ...cfg, glueUrl: nextGlueUrl });
    }, glueUrl);
    const badPath = os.tmpdir();
    const out = await page.evaluate(async (p) => {
      return await window.aiwfDesktop.exportWorkflowQualityGateReports({
        mock: true,
        path: p,
        format: 'md',
        limit: 50,
        filter: { run_id: '', status: 'all' },
      });
    }, badPath);
    expect(out?.ok).toBeFalsy();
    expect(String(out?.error || '')).not.toBe('');
    await electronApp.close();
  });
});
