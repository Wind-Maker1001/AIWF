const { runChipletNode } = require("./runner");
const crypto = require("crypto");

function maxParallel() {
  const n = Number(process.env.AIWF_CHIPLET_MAX_PARALLEL || "2");
  if (Number.isFinite(n) && n > 0) return Math.min(8, Math.floor(n));
  return 2;
}

function pickPath(obj, field) {
  const key = String(field || "").trim();
  if (!key) return undefined;
  const parts = key.split(".");
  let cur = obj;
  for (const p of parts) {
    if (!cur || typeof cur !== "object") return undefined;
    cur = cur[p];
  }
  return cur;
}

function evalRule(when, output) {
  const op = String(when.op || "eq").trim().toLowerCase();
  const field = String(when.field || "ok");
  const lhs = pickPath(output, field);
  const rhs = when.value;
  if (op === "exists") return typeof lhs !== "undefined" && lhs !== null;
  if (op === "not_exists") return typeof lhs === "undefined" || lhs === null;
  if (op === "eq") return lhs === rhs;
  if (op === "ne") return lhs !== rhs;
  if (op === "in") return Array.isArray(rhs) && rhs.includes(lhs);
  if (op === "not_in") return Array.isArray(rhs) && !rhs.includes(lhs);
  if (op === "contains") return String(lhs || "").includes(String(rhs || ""));
  const a = Number(lhs);
  const b = Number(rhs);
  if (!Number.isFinite(a) || !Number.isFinite(b)) return false;
  if (op === "gt") return a > b;
  if (op === "gte") return a >= b;
  if (op === "lt") return a < b;
  if (op === "lte") return a <= b;
  return false;
}

function evalWhen(when, output, ctx) {
  if (when === null || typeof when === "undefined") return true;
  if (typeof when === "boolean") return when;
  if (typeof when === "string") {
    const s = when.trim().toLowerCase();
    if (!s || s === "always" || s === "true") return true;
    if (s === "false" || s === "never") return false;
    return !!pickPath({ output, ctx }, when);
  }
  if (!when || typeof when !== "object") return false;
  if (Array.isArray(when.all)) return when.all.every((x) => evalWhen(x, output, ctx));
  if (Array.isArray(when.any)) return when.any.some((x) => evalWhen(x, output, ctx));
  if (when.not) return !evalWhen(when.not, output, ctx);
  return evalRule(when, output);
}

function buildGraphState(graph = {}) {
  const nodes = Array.isArray(graph.nodes) ? graph.nodes : [];
  const edges = Array.isArray(graph.edges) ? graph.edges : [];
  const byId = new Map(nodes.map((n) => [String(n.id), n]));
  const incomingTotal = new Map(nodes.map((n) => [String(n.id), 0]));
  const incomingSeen = new Map(nodes.map((n) => [String(n.id), 0]));
  const incomingActive = new Map(nodes.map((n) => [String(n.id), 0]));
  const incomingFrom = new Map(nodes.map((n) => [String(n.id), []]));
  const out = new Map(nodes.map((n) => [String(n.id), []]));
  for (const e of edges) {
    const from = String(e.from || "");
    const to = String(e.to || "");
    if (!byId.has(from) || !byId.has(to)) continue;
    incomingTotal.set(to, (incomingTotal.get(to) || 0) + 1);
    incomingFrom.get(to).push(from);
    out.get(from).push({ to, when: e.when });
  }
  return { byId, incomingTotal, incomingSeen, incomingActive, incomingFrom, out };
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

function resolveMapValue(expr, nodeOutputs, predecessorIds = []) {
  if (expr && typeof expr === "object" && !Array.isArray(expr)) {
    const from = String(expr.from || predecessorIds[0] || "");
    const p = String(expr.path || "");
    return pickPath(nodeOutputs.get(from) || {}, p);
  }
  const s = String(expr || "").trim();
  if (!s) return undefined;
  if (s.startsWith("$prev.")) {
    const p = s.slice("$prev.".length);
    for (const pid of predecessorIds) {
      const v = pickPath(nodeOutputs.get(pid) || {}, p);
      if (typeof v !== "undefined") return v;
    }
    return undefined;
  }
  const dot = s.indexOf(".");
  if (dot > 0) {
    const nodeId = s.slice(0, dot);
    const p = s.slice(dot + 1);
    if (nodeOutputs.has(nodeId)) return pickPath(nodeOutputs.get(nodeId), p);
  }
  for (const pid of predecessorIds) {
    const v = pickPath(nodeOutputs.get(pid) || {}, s);
    if (typeof v !== "undefined") return v;
  }
  return undefined;
}

function applyOutputMap(output, outputMap) {
  if (!outputMap || typeof outputMap !== "object" || Array.isArray(outputMap)) return output;
  const out = { ...(output || {}) };
  for (const [alias, expr] of Object.entries(outputMap)) {
    out[alias] = resolveMapValue(expr, new Map([["_", output]]), ["_"]);
  }
  return out;
}

function stableStringify(value) {
  if (value === null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map((x) => stableStringify(x)).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

function buildNodeCacheKey(node, resolvedConfig, predecessorOutputs) {
  const payload = {
    type: String(node?.type || ""),
    node_id: String(node?.id || ""),
    config: resolvedConfig || {},
    predecessors: predecessorOutputs || {},
  };
  return crypto.createHash("sha256").update(stableStringify(payload)).digest("hex");
}

function collectAncestors(incomingFrom, target) {
  const t = String(target || "");
  if (!t) return new Set();
  const out = new Set();
  const stack = [t];
  while (stack.length) {
    const cur = stack.pop();
    for (const p of incomingFrom.get(cur) || []) {
      if (out.has(p)) continue;
      out.add(p);
      stack.push(p);
    }
  }
  return out;
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
  const {
    byId,
    incomingTotal,
    incomingSeen,
    incomingActive,
    incomingFrom,
    out,
  } = buildGraphState(graph);
  const total = byId.size;
  const ready = Array.from(incomingTotal.entries()).filter(([, d]) => d === 0).map(([id]) => id);
  const running = new Map();
  const done = new Set();
  const nodeOutputs = new Map();
  const activeSources = new Map(Array.from(byId.keys()).map((id) => [id, []]));
  let failed = null;

  const resume = ctx?.payload?.resume || {};
  const resumeFrom = String(resume.node_id || "");
  const resumeCache = resume.outputs && typeof resume.outputs === "object" ? resume.outputs : {};
  const resumeAncestors = collectAncestors(incomingFrom, resumeFrom);

  function removeReady(nodeId) {
    const idx = ready.indexOf(nodeId);
    if (idx >= 0) ready.splice(idx, 1);
  }

  function scheduleChildren(fromId, output, activeSource) {
    for (const edge of out.get(fromId) || []) {
      const to = edge.to;
      incomingSeen.set(to, (incomingSeen.get(to) || 0) + 1);
      if (activeSource && evalWhen(edge.when, output, ctx)) {
        incomingActive.set(to, (incomingActive.get(to) || 0) + 1);
        activeSources.get(to).push(fromId);
      }
      const totalPred = incomingTotal.get(to) || 0;
      const seenPred = incomingSeen.get(to) || 0;
      if (seenPred === totalPred && !done.has(to) && !running.has(to) && !ready.includes(to)) {
        const activePred = incomingActive.get(to) || 0;
        if (totalPred === 0 || activePred > 0) ready.push(to);
      }
    }
  }

  function skipNode(nodeId, reason) {
    if (done.has(nodeId) || running.has(nodeId)) return;
    const node = byId.get(nodeId);
    if (!node) return;
    done.add(nodeId);
    const output = { ok: true, status: "skipped", skipped: true, reason: String(reason || "unreachable") };
    nodeOutputs.set(nodeId, output);
    onNodeSuccess?.(node, output);
    scheduleChildren(nodeId, output, false);
  }

  function pruneIfUnreachable() {
    let progressed = true;
    while (progressed) {
      progressed = false;
      for (const [id] of byId.entries()) {
        if (done.has(id) || running.has(id) || ready.includes(id)) continue;
        const totalPred = incomingTotal.get(id) || 0;
        const seenPred = incomingSeen.get(id) || 0;
        const activePred = incomingActive.get(id) || 0;
        if (totalPred > 0 && seenPred === totalPred && activePred === 0) {
          skipNode(id, "no_active_incoming_edge");
          progressed = true;
        }
      }
    }
  }

  function resolveNodeConfig(nodeId, node) {
    const base = node?.config && typeof node.config === "object" ? { ...node.config } : {};
    const inputMap = base.input_map && typeof base.input_map === "object" ? base.input_map : null;
    if (!inputMap) return base;
    const preds = activeSources.get(nodeId) || [];
    for (const [targetKey, expr] of Object.entries(inputMap)) {
      const v = resolveMapValue(expr, nodeOutputs, preds);
      if (typeof v !== "undefined") base[targetKey] = v;
    }
    return base;
  }

  function predecessorOutputSnapshot(predIds = []) {
    const outMap = {};
    for (const pid of predIds) outMap[pid] = nodeOutputs.get(pid) || null;
    return outMap;
  }

  async function launch(nodeId) {
    const node = byId.get(nodeId);
    if (!node) return;
    onNodeStart?.(node);

    if (resumeFrom && nodeId !== resumeFrom && resumeAncestors.has(nodeId) && resumeCache[nodeId]) {
      const reused = { ...(resumeCache[nodeId] || {}), resumed_from_cache: true, status: "done" };
      done.add(nodeId);
      nodeOutputs.set(nodeId, reused);
      onNodeSuccess?.(node, reused);
      scheduleChildren(nodeId, reused, true);
      pruneIfUnreachable();
      return;
    }

    const runtimeNode = {
      ...node,
      config: resolveNodeConfig(nodeId, node),
    };
    const preds = activeSources.get(nodeId) || [];
    const predSnapshot = predecessorOutputSnapshot(preds);
    const cacheApi = ctx?.nodeCache && typeof ctx.nodeCache === "object" ? ctx.nodeCache : null;
    const cacheKey = cacheApi ? buildNodeCacheKey(runtimeNode, runtimeNode.config, predSnapshot) : "";
    if (cacheApi && cacheKey && typeof cacheApi.get === "function") {
      const hit = cacheApi.get(cacheKey);
      if (hit && typeof hit === "object") {
        const outHit = { ...hit, cached: true, status: hit.status || "done" };
        done.add(nodeId);
        nodeOutputs.set(nodeId, outHit);
        onNodeSuccess?.(node, outHit);
        scheduleChildren(nodeId, outHit, true);
        pruneIfUnreachable();
        return;
      }
    }
    const p = runChipletNode({
      registry,
      node: runtimeNode,
      ctx,
      envelope: buildEnvelope({
        run_id: ctx.runId,
        workflow_id: ctx.workflowId,
        node_id: node.id,
        node_type: node.type,
        trace_id: ctx.traceId || ctx.payload?.trace_id || "",
      }),
    });
    running.set(nodeId, p);
    try {
      let output = await p;
      output = applyOutputMap(output, runtimeNode?.config?.output_map);
      done.add(nodeId);
      nodeOutputs.set(nodeId, output);
      if (cacheApi && cacheKey && typeof cacheApi.set === "function") {
        try { cacheApi.set(cacheKey, output); } catch {}
      }
      onNodeSuccess?.(node, output);
      scheduleChildren(nodeId, output, true);
      pruneIfUnreachable();
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
      removeReady(nodeId);
      // eslint-disable-next-line no-floating-promises
      launch(nodeId);
    }
    pruneIfUnreachable();
    if (running.size === 0 && ready.length === 0) break;
    // eslint-disable-next-line no-await-in-loop
    await Promise.race(Array.from(running.values()).map((p) => p.then(() => true).catch(() => false)));
  }

  if (failed) throw failed;
  if (done.size !== total) throw new Error("workflow execute incomplete");
  ctx.nodeOutputs = Object.fromEntries(nodeOutputs);
}

module.exports = {
  executeWorkflowDag,
};
