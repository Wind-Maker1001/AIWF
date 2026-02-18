const path = require('path');
const os = require('os');
const fs = require('fs');
const { test, expect, _electron: electron } = require('@playwright/test');

async function openWorkflow() {
  const appDir = path.resolve(__dirname, '..');
  const electronApp = await electron.launch({ args: [appDir, '--workflow', '--workflow-debug-api'] });
  const page = await electronApp.firstWindow();
  return { electronApp, page };
}

test('workflow studio renders baseline graph', async () => {
  const { electronApp, page } = await openWorkflow();
  await expect(page.locator('h2')).toHaveText(/Workflow Studio/);
  await expect(page.locator('.node')).toHaveCount(6);
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
  await expect(page.locator('.node')).toHaveCount(6);
  const edgeCountBefore = await page.locator('#edges path.edge-line').count();
  expect(edgeCountBefore).toBeGreaterThan(0);

  await page.click('.node[data-id="n2"] .mini.del');
  await expect(page.locator('.node[data-id="n2"]')).toHaveCount(0);
  await expect(page.locator('.node')).toHaveCount(5);
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
  const { electronApp, page } = await openWorkflow();
  const fp = path.join(os.tmpdir(), `aiwf_workflow_${Date.now()}.json`);
  const saved = await page.evaluate(async (filePath) => {
    const g = {
      workflow_id: 't1',
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

test('marquee selection selects multiple nodes', async () => {
  const { electronApp, page } = await openWorkflow();
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
  for (const item of cases) {
    const stat = await page.evaluate(async (payload) => {
      window.__aiwfDebug.setGraph(payload.graph);
      await new Promise((r) => setTimeout(r, 100));
      return window.__aiwfDebug.routeStats();
    }, item);
    expect(stat.solved).toBeGreaterThan(0);
    expect(stat.fallback_ratio).toBeLessThan(item.max_fallback_ratio);
  }
  await electronApp.close();
});
