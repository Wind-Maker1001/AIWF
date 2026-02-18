const { performance } = require('perf_hooks');
const path = require('path');
const fs = require('fs');
const { pathToFileURL } = require('url');

const NODE_W = 210;
const NODE_H = 88;
const GRID = 24;
let sharedRoutedPath = null;

function buildRoute(a, b, obstacles = []) {
  if (typeof sharedRoutedPath !== 'function') {
    throw new Error('routing_core.mjs not loaded');
  }
  return sharedRoutedPath(a, b, obstacles, {
    routeGrid: GRID,
    attempts: [
      { grid: GRID, margin: 180, startDir: 0, endDir: 1, pad: 8, guardMax: 12000 },
      { grid: GRID, margin: 220, startDir: -1, endDir: -1, pad: 3, guardMax: 12000 },
      { grid: GRID, margin: 300, startDir: -1, endDir: -1, pad: 0, guardMax: 12000 },
    ],
  });
}

function evalFixedCase(name, nodes, edges, maxFallbackRatio = 0.6) {
  const map = new Map((nodes || []).map((n) => [n.id, n]));
  let astar = 0;
  let segmented = 0;
  let fallback = 0;
  for (const e of edges || []) {
    const from = map.get(e.from);
    const to = map.get(e.to);
    if (!from || !to) continue;
    const a = { x: from.x + NODE_W, y: from.y + NODE_H / 2 };
    const b = { x: to.x, y: to.y + NODE_H / 2 };
    const obstacles = obstaclesFromNodesNear(nodes, new Set([e.from, e.to]), a, b, 220);
    const route = buildRoute(a, b, obstacles);
    if (route?.mode === 'astar') astar += 1;
    else if (route?.mode === 'segmented') segmented += 1;
    else fallback += 1;
  }
  const solved = astar + segmented + fallback;
  const ratio = Number((fallback / Math.max(1, solved)).toFixed(4));
  return {
    name,
    ok: ratio <= maxFallbackRatio,
    max_fallback_ratio: maxFallbackRatio,
    fallback_ratio: ratio,
    solved_total: solved,
    astar_solved: astar,
    segmented_solved: segmented,
    fallback_used: fallback,
  };
}

function buildGridCase() {
  const nodes = [];
  const rows = 5;
  const cols = 8;
  for (let r = 0; r < rows; r += 1) {
    for (let c = 0; c < cols; c += 1) nodes.push({ id: `g${r}_${c}`, x: 80 + c * 260, y: 80 + r * 180 });
  }
  const edges = [];
  for (let r = 0; r < rows; r += 1) for (let c = 0; c < cols - 1; c += 1) edges.push({ from: `g${r}_${c}`, to: `g${r}_${c + 1}` });
  for (let r = 0; r < rows - 1; r += 1) for (let c = 0; c < cols; c += 1) edges.push({ from: `g${r}_${c}`, to: `g${r + 1}_${c}` });
  return { nodes, edges };
}

function buildLongChainCase() {
  const nodes = [];
  for (let i = 0; i < 24; i += 1) nodes.push({ id: `l${i}`, x: 120 + i * 150, y: 120 + (i % 2) * 180 });
  const edges = [];
  for (let i = 0; i < 23; i += 1) edges.push({ from: `l${i}`, to: `l${i + 1}` });
  for (let i = 0; i < 20; i += 2) edges.push({ from: `l${i}`, to: `l${Math.min(23, i + 3)}` });
  return { nodes, edges };
}

function buildClusterCase() {
  const nodes = [];
  for (let i = 0; i < 12; i += 1) nodes.push({ id: `cA${i}`, x: 120 + (i % 4) * 160, y: 120 + Math.floor(i / 4) * 130 });
  for (let i = 0; i < 12; i += 1) nodes.push({ id: `cB${i}`, x: 980 + (i % 4) * 160, y: 160 + Math.floor(i / 4) * 130 });
  const edges = [];
  for (let i = 0; i < 11; i += 1) edges.push({ from: `cA${i}`, to: `cA${i + 1}` });
  for (let i = 0; i < 11; i += 1) edges.push({ from: `cB${i}`, to: `cB${i + 1}` });
  for (let i = 0; i < 12; i += 1) edges.push({ from: `cA${i}`, to: `cB${11 - i}` });
  return { nodes, edges };
}

function buildLargeSparseLongRangeCase() {
  const nodes = [];
  for (let i = 0; i < 60; i += 1) {
    const col = i % 10;
    const row = Math.floor(i / 10);
    nodes.push({ id: `s${i}`, x: 200 + col * 420, y: 120 + row * 360 });
  }
  const edges = [];
  for (let i = 0; i < 50; i += 1) {
    const to = (i + 9) % 60;
    edges.push({ from: `s${i}`, to: `s${to}` });
  }
  for (let i = 0; i < 30; i += 1) {
    const to = (i + 23) % 60;
    edges.push({ from: `s${i}`, to: `s${to}` });
  }
  return { nodes, edges };
}

function runFixedRegressionCases(maxFallbackRatio = 0.6) {
  const c1 = buildGridCase();
  const c2 = buildLongChainCase();
  const c3 = buildClusterCase();
  const c4 = buildLargeSparseLongRangeCase();
  const cases = [
    evalFixedCase('grid', c1.nodes, c1.edges, maxFallbackRatio),
    evalFixedCase('long_chain', c2.nodes, c2.edges, maxFallbackRatio),
    evalFixedCase('cluster_cross', c3.nodes, c3.edges, maxFallbackRatio),
    evalFixedCase('large_sparse_long_range', c4.nodes, c4.edges, maxFallbackRatio),
  ];
  return { ok: cases.every((x) => x.ok), cases };
}

function createRng(seed = 42) {
  let s = (Number(seed) >>> 0) || 42;
  return () => {
    s = (1664525 * s + 1013904223) >>> 0;
    return s / 0xffffffff;
  };
}

function randomInt(rng, min, max) {
  return Math.floor(rng() * (max - min + 1)) + min;
}

function median(values = []) {
  const arr = (values || [])
    .map((x) => Number(x))
    .filter((x) => Number.isFinite(x))
    .sort((a, b) => a - b);
  if (!arr.length) return 0;
  const mid = Math.floor(arr.length / 2);
  if (arr.length % 2 === 1) return arr[mid];
  return (arr[mid - 1] + arr[mid]) / 2;
}

function generateGraph(rng, cfg = {}) {
  const nodeCount = Number(cfg.nodeCount || 120);
  const edgeCount = Number(cfg.edgeCount || 260);
  const xMin = Number(cfg.xMin ?? -500);
  const xMax = Number(cfg.xMax ?? 3000);
  const yMin = Number(cfg.yMin ?? -300);
  const yMax = Number(cfg.yMax ?? 2200);
  const nodes = [];
  for (let i = 0; i < nodeCount; i += 1) {
    nodes.push({ id: `n${i + 1}`, x: randomInt(rng, xMin, xMax), y: randomInt(rng, yMin, yMax) });
  }
  const edges = [];
  for (let i = 0; i < edgeCount; i += 1) {
    const a = randomInt(rng, 0, nodeCount - 1);
    let b = randomInt(rng, 0, nodeCount - 1);
    if (b === a) b = (b + 1) % nodeCount;
    edges.push({ from: nodes[a].id, to: nodes[b].id });
  }
  return { nodes, edges };
}

function obstaclesFromNodesNear(nodes, ignoreSet, a, b, margin = 220) {
  const x1 = Math.min(a.x, b.x) - margin;
  const y1 = Math.min(a.y, b.y) - margin;
  const x2 = Math.max(a.x, b.x) + margin;
  const y2 = Math.max(a.y, b.y) + margin;
  return nodes
    .filter((n) => !ignoreSet.has(n.id))
    .map((n) => ({ x: n.x, y: n.y, w: NODE_W, h: NODE_H }))
    .filter((r) => !(r.x + r.w < x1 || r.x > x2 || r.y + r.h < y1 || r.y > y2));
}

async function loadSharedRoutingCore() {
  const corePath = path.resolve(__dirname, '..', 'renderer', 'workflow', 'routing_core.mjs');
  const core = await import(pathToFileURL(corePath).href);
  sharedRoutedPath = core && typeof core.routedPath === 'function' ? core.routedPath : null;
  if (typeof sharedRoutedPath !== 'function') {
    throw new Error('routing_core.mjs missing routedPath export');
  }
}

async function runBench() {
  const seed = Number(process.env.AIWF_ROUTE_BENCH_SEED || 42);
  const threshold = Number(process.env.AIWF_ROUTE_BENCH_MAX_MS_PER_EDGE || 150);
  const worstScenarioThreshold = Number(process.env.AIWF_ROUTE_BENCH_MAX_WORST_SCENARIO_MS_PER_EDGE || 195);
  const maxFallbackRatio = Number(process.env.AIWF_ROUTE_BENCH_MAX_FALLBACK_RATIO || 0.55);
  const maxRandomFallbackRatio = Number(process.env.AIWF_ROUTE_BENCH_MAX_RANDOM_FALLBACK_RATIO || 0.55);
  const trendWindow = Number(process.env.AIWF_ROUTE_BENCH_TREND_WINDOW || 7);
  const trendMinSamples = Number(process.env.AIWF_ROUTE_BENCH_TREND_MIN_SAMPLES || 5);
  const trendMedianMsMax = Number(process.env.AIWF_ROUTE_BENCH_TREND_MEDIAN_MS_PER_EDGE_MAX || 135);
  const trendMedianWorstMsMax = Number(process.env.AIWF_ROUTE_BENCH_TREND_MEDIAN_WORST_MS_PER_EDGE_MAX || 175);
  const historyPath = path.resolve(process.env.AIWF_ROUTE_BENCH_HISTORY || path.join(__dirname, '..', '..', 'tmp', 'routing_bench_history.jsonl'));
  const latestPath = path.resolve(process.env.AIWF_ROUTE_BENCH_LATEST || path.join(__dirname, '..', '..', 'tmp', 'routing_bench_latest.json'));
  const scenarios = [
    { name: 'sparse', nodeCount: 80, edgeCount: 160, xMin: -300, xMax: 2200, yMin: -200, yMax: 1400 },
    { name: 'balanced', nodeCount: 120, edgeCount: 260, xMin: -500, xMax: 3000, yMin: -300, yMax: 2200 },
    { name: 'dense', nodeCount: 140, edgeCount: 320, xMin: -200, xMax: 1800, yMin: -150, yMax: 1300 },
    { name: 'stress', nodeCount: 220, edgeCount: 520, xMin: -600, xMax: 4200, yMin: -300, yMax: 2800 },
    { name: 'stress_congested_cross', nodeCount: 160, edgeCount: 300, xMin: -700, xMax: 3400, yMin: -400, yMax: 2400 },
  ];
  const rng = createRng(seed);
  let totalMs = 0;
  let totalEdges = 0;
  let totalNodes = 0;
  let astarOk = 0;
  let segmentedOk = 0;
  let fallbackUsed = 0;
  const scenarioResults = [];

  for (const sc of scenarios) {
    const { nodes, edges } = generateGraph(rng, sc);
    const map = new Map(nodes.map((n) => [n.id, n]));
    const t0 = performance.now();
    let scAstar = 0;
    let scSegmented = 0;
    let scFallback = 0;

    for (const e of edges) {
      const from = map.get(e.from);
      const to = map.get(e.to);
      if (!from || !to) continue;
      const a = { x: from.x + NODE_W, y: from.y + NODE_H / 2 };
      const b = { x: to.x, y: to.y + NODE_H / 2 };
      const obstacles = obstaclesFromNodesNear(nodes, new Set([e.from, e.to]), a, b, 200);
      const route = buildRoute(a, b, obstacles);
      if (route?.mode === 'astar') scAstar += 1;
      else if (route?.mode === 'segmented') scSegmented += 1;
      else if (route?.mode === 'fallback') scFallback += 1;
    }

    const t1 = performance.now();
    const scMs = Math.round((t1 - t0) * 100) / 100;
    const scPer = Math.round((scMs / edges.length) * 1000) / 1000;
    scenarioResults.push({
      name: sc.name,
      nodes: nodes.length,
      edges: edges.length,
      astar_solved: scAstar,
      segmented_solved: scSegmented,
      fallback_used: scFallback,
      solved_total: scAstar + scSegmented + scFallback,
      total_ms: scMs,
      ms_per_edge: scPer,
    });
    totalMs += scMs;
    totalEdges += edges.length;
    totalNodes += nodes.length;
    astarOk += scAstar;
    segmentedOk += scSegmented;
    fallbackUsed += scFallback;
  }

  const ms = Math.round(totalMs * 100) / 100;
  const per = Math.round((ms / totalEdges) * 1000) / 1000;
  const worstScenarioMsPerEdge = Math.max(...scenarioResults.map((x) => x.ms_per_edge));
  const fixedCases = runFixedRegressionCases(maxFallbackRatio);
  const randomFallbackRatio = Number((fallbackUsed / Math.max(1, astarOk + segmentedOk + fallbackUsed)).toFixed(4));
  let trend = {
    enabled: true,
    history_path: historyPath,
    latest_path: latestPath,
    window: trendWindow,
    sample_count: 0,
    min_samples: trendMinSamples,
    enforced: false,
    median_ms_per_edge: 0,
    median_worst_ms_per_edge: 0,
    thresholds: {
      median_ms_per_edge_max: trendMedianMsMax,
      median_worst_ms_per_edge_max: trendMedianWorstMsMax,
    },
    ok: true,
  };
  try {
    fs.mkdirSync(path.dirname(historyPath), { recursive: true });
    const rec = {
      ts: new Date().toISOString(),
      ms_per_edge: per,
      worst_scenario_ms_per_edge: worstScenarioMsPerEdge,
      fallback_ratio: randomFallbackRatio,
      solved_total: astarOk + segmentedOk + fallbackUsed,
      edges: totalEdges,
    };
    fs.appendFileSync(historyPath, `${JSON.stringify(rec)}\n`, 'utf8');
    const lines = fs.readFileSync(historyPath, 'utf8').split(/\r?\n/).filter(Boolean);
    const parsed = lines
      .map((ln) => {
        try { return JSON.parse(ln); } catch { return null; }
      })
      .filter(Boolean);
    const recent = parsed.slice(-Math.max(1, trendWindow));
    const medPer = Number(median(recent.map((x) => x.ms_per_edge)).toFixed(3));
    const medWorst = Number(median(recent.map((x) => x.worst_scenario_ms_per_edge)).toFixed(3));
    trend = {
      ...trend,
      sample_count: recent.length,
      median_ms_per_edge: medPer,
      median_worst_ms_per_edge: medWorst,
      enforced: recent.length >= trendMinSamples,
      ok: recent.length < trendMinSamples
        ? true
        : (medPer <= trendMedianMsMax && medWorst <= trendMedianWorstMsMax),
      warning: recent.length < trendMinSamples
        ? `trend gate not enforced yet: sample_count=${recent.length}, min_samples=${trendMinSamples}`
        : undefined,
    };
  } catch (e) {
    trend = {
      ...trend,
      enabled: false,
      ok: true,
      warning: String(e),
    };
  }
  const result = {
    ok: per <= threshold
      && worstScenarioMsPerEdge <= worstScenarioThreshold
      && fixedCases.ok
      && randomFallbackRatio <= maxRandomFallbackRatio
      && trend.ok,
    seed,
    threshold_ms_per_edge: threshold,
    threshold_worst_scenario_ms_per_edge: worstScenarioThreshold,
    max_random_fallback_ratio: maxRandomFallbackRatio,
    nodes: totalNodes,
    edges: totalEdges,
    scenarios: scenarioResults,
    worst_scenario_ms_per_edge: worstScenarioMsPerEdge,
    astar_solved: astarOk,
    segmented_solved: segmentedOk,
    fallback_used: fallbackUsed,
    fallback_ratio: randomFallbackRatio,
    solved_total: astarOk + segmentedOk + fallbackUsed,
    total_ms: ms,
    ms_per_edge: per,
    fixed_cases: fixedCases,
    trend,
  };
  try {
    fs.mkdirSync(path.dirname(latestPath), { recursive: true });
    fs.writeFileSync(latestPath, JSON.stringify(result, null, 2), 'utf8');
  } catch {}
  console.log(JSON.stringify(result, null, 2));
  if (!result.ok) process.exit(2);
}

loadSharedRoutingCore()
  .then(runBench)
  .catch((e) => {
    console.error(String(e));
    process.exit(2);
  });

