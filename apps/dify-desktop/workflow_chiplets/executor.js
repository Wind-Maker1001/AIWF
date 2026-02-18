const { runChipletNode } = require("./runner");

function maxParallel() {
  const n = Number(process.env.AIWF_CHIPLET_MAX_PARALLEL || "2");
  if (Number.isFinite(n) && n > 0) return Math.min(8, Math.floor(n));
  return 2;
}

function buildGraphState(graph = {}) {
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const byId = new Map(nodes.map((n) => [String(n.id), n]));
  const indeg = new Map(nodes.map((n) => [String(n.id), 0]));
  const out = new Map(nodes.map((n) => [String(n.id), []]));
  for (const e of edges) {
    const from = String(e.from || "");
    const to = String(e.to || "");
    if (!byId.has(from) || !byId.has(to)) continue;
    indeg.set(to, (indeg.get(to) || 0) + 1);
    out.get(from).push(to);
  }
  return { byId, indeg, out };
}

function sortedReady(ready, registry, byId) {
  return ready
    .slice()
    .sort((a, b) => {
      const na = byId.get(a);
      const nb = byId.get(b);
      const pa = Number(registry.resolve(na?.type || "")?.priority || 100);
      const pb = Number(registry.resolve(nb?.type || "")?.priority || 100);
      if (pa !== pb) return pb - pa;
      return String(a).localeCompare(String(b));
    });
}

async function executeWorkflowDag({
  graph,
  registry,
  ctx,
  buildEnvelope,
  onNodeStart,
  onNodeSuccess,
  onNodeFailure,
}) {
  const { byId, indeg, out } = buildGraphState(graph);
  const total = byId.size;
  const ready = Array.from(indeg.entries()).filter(([, d]) => d === 0).map(([id]) => id);
  const running = new Map();
  const done = new Set();
  let failed = null;

  async function launch(nodeId) {
    const node = byId.get(nodeId);
    if (!node) return;
    onNodeStart?.(node);
    const p = runChipletNode({
      registry,
      node,
      ctx,
      envelope: buildEnvelope({
        run_id: ctx.runId,
        workflow_id: ctx.workflowId,
        node_id: node.id,
        node_type: node.type,
      }),
    });
    running.set(nodeId, p);
    try {
      const output = await p;
      done.add(nodeId);
      onNodeSuccess?.(node, output);
      for (const to of out.get(nodeId) || []) {
        indeg.set(to, (indeg.get(to) || 0) - 1);
        if (indeg.get(to) === 0) ready.push(to);
      }
    } catch (e) {
      if (!failed) failed = e;
      onNodeFailure?.(node, e);
    } finally {
      running.delete(nodeId);
    }
  }

  while (done.size < total) {
    if (failed) break;
    const limit = maxParallel();
    const next = sortedReady(ready, registry, byId);
    while (running.size < limit && next.length > 0) {
      const nodeId = next.shift();
      const idx = ready.indexOf(nodeId);
      if (idx >= 0) ready.splice(idx, 1);
      // eslint-disable-next-line no-floating-promises
      launch(nodeId);
    }
    if (running.size === 0) break;
    // eslint-disable-next-line no-await-in-loop
    await Promise.race(Array.from(running.values()).map((p) => p.then(() => true).catch(() => false)));
  }

  if (failed) throw failed;
  if (done.size !== total) throw new Error("workflow execute incomplete");
}

module.exports = {
  executeWorkflowDag,
};
