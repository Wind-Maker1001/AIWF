import { validateGraph } from "./graph.js";
import {
  IO_CONTRACT_COMPATIBLE_OPERATORS,
  buildIoContractInput,
  postRustOperator,
} from "./preflight-rust-helpers.js";

function createWorkflowPreflightControllerUi(els, deps = {}) {
  const {
    graphPayload = () => ({}),
    exportGraph = () => ({}),
    applyGraph = () => {},
    computePreflightRisk = () => ({}),
    renderPreflightReport = () => {},
    setLastPreflightReport = () => {},
  } = deps;

  function autoFixGraphStructure() {
    const graph = exportGraph();
    const nodes = Array.isArray(graph?.nodes) ? graph.nodes.slice() : [];
    const edges = Array.isArray(graph?.edges) ? graph.edges.slice() : [];
    const nodeIds = new Set(nodes.map((n) => String(n?.id || "")).filter(Boolean));
    const cleanedEdges = [];
    const edgeSeen = new Set();
    let removedDup = 0;
    let removedSelf = 0;
    let removedBroken = 0;
    const dupEdges = [];
    const selfLoops = [];
    const brokenEdges = [];

    edges.forEach((e) => {
      const from = String(e?.from || "").trim();
      const to = String(e?.to || "").trim();
      if (!from || !to || !nodeIds.has(from) || !nodeIds.has(to)) {
        removedBroken += 1;
        brokenEdges.push({ from, to });
        return;
      }
      if (from === to) {
        removedSelf += 1;
        selfLoops.push({ from, to });
        return;
      }
      const key = `${from}=>${to}`;
      if (edgeSeen.has(key)) {
        removedDup += 1;
        dupEdges.push({ from, to });
        return;
      }
      edgeSeen.add(key);
      cleanedEdges.push(e);
    });

    const inDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
    const outDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
    cleanedEdges.forEach((e) => {
      const from = String(e?.from || "");
      const to = String(e?.to || "");
      outDegree.set(from, (outDegree.get(from) || 0) + 1);
      inDegree.set(to, (inDegree.get(to) || 0) + 1);
    });

    let removedIsolated = 0;
    const isolatedNodes = [];
    const cleanedNodes = nodes.filter((n) => {
      const id = String(n?.id || "");
      if (nodes.length <= 1) return true;
      const isolated = (inDegree.get(id) || 0) === 0 && (outDegree.get(id) || 0) === 0;
      if (isolated) {
        removedIsolated += 1;
        isolatedNodes.push({ id, type: String(n?.type || "") });
      }
      return !isolated;
    });

    applyGraph({ ...graph, nodes: cleanedNodes, edges: cleanedEdges });
    const moved = removedDup + removedSelf + removedBroken + removedIsolated;
    return {
      changed: moved > 0,
      removed_dup_edges: removedDup,
      removed_self_loops: removedSelf,
      removed_broken_edges: removedBroken,
      removed_isolated_nodes: removedIsolated,
      dup_edges: dupEdges,
      self_loops: selfLoops,
      broken_edges: brokenEdges,
      isolated_nodes: isolatedNodes,
    };
  }

  async function runWorkflowPreflight() {
    const graph = graphPayload();
    const valid = validateGraph(graph);
    const issues = [];
    (valid.errors || []).forEach((msg) => issues.push({ level: "error", kind: "graph", message: String(msg) }));
    (valid.warnings || []).forEach((msg) => issues.push({ level: "warning", kind: "graph", message: String(msg) }));

    const endpoint = String(els.rustEndpoint?.value || "").trim().replace(/\/$/, "");
    const rustRequired = !!els.rustRequired?.checked;
    const rustNodes = (Array.isArray(graph?.nodes) ? graph.nodes : []).filter((n) =>
      IO_CONTRACT_COMPATIBLE_OPERATORS.has(String(n?.type || ""))
    );
    const firstInputFile = String((els.inputFiles?.value || "").split(/\r?\n/).map((s) => s.trim()).find(Boolean) || "");

    if (!endpoint) {
      if (rustNodes.length > 0 && rustRequired) {
        issues.push({ level: "error", kind: "rust", message: "Rust Endpoint 为空，无法执行契约预检" });
      }
    } else if (rustNodes.length > 0) {
      try {
        const ops = Array.from(new Set(rustNodes.map((n) => String(n.type || ""))));
        const capRes = await postRustOperator(endpoint, "/operators/capabilities_v1", { include_ops: ops });
        if (!capRes.ok) {
          issues.push({
            level: rustRequired ? "error" : "warning",
            kind: "rust",
            message: `能力发现失败: ${capRes.error || "unknown"}`,
          });
        }
      } catch (e) {
        issues.push({
          level: rustRequired ? "error" : "warning",
          kind: "rust",
          message: `能力发现异常: ${String(e)}`,
        });
      }

      for (const node of rustNodes) {
        const operator = String(node?.type || "");
        const payload = {
          run_id: `preflight_${Date.now()}`,
          operator,
          input: buildIoContractInput(operator, node?.config || {}, firstInputFile),
          strict: false,
        };
        try {
          const out = await postRustOperator(endpoint, "/operators/io_contract_v1/validate", payload);
          if (!out.ok) {
            issues.push({
              level: rustRequired ? "error" : "warning",
              kind: "io_contract",
              node_id: String(node?.id || ""),
              message: `${operator} 契约校验请求失败: ${out.error || "unknown"}`,
            });
            continue;
          }
          const body = out.body || {};
          const validContract = !!body.valid;
          const errs = Array.isArray(body.errors) ? body.errors : [];
          if (!validContract || errs.length) {
            issues.push({
              level: "error",
              kind: "io_contract",
              node_id: String(node?.id || ""),
              message: `${operator} 契约不通过: ${errs.join("; ") || "unknown"}`,
            });
          }
        } catch (e) {
          issues.push({
            level: rustRequired ? "error" : "warning",
            kind: "io_contract",
            node_id: String(node?.id || ""),
            message: `${operator} 契约校验异常: ${String(e)}`,
          });
        }
      }
    }

    const ok = issues.every((x) => String(x.level || "") !== "error");
    const risk = computePreflightRisk(issues);
    const report = { ok, issues, risk, ts: new Date().toISOString() };
    setLastPreflightReport(report);
    renderPreflightReport(report);
    return report;
  }

  return {
    autoFixGraphStructure,
    runWorkflowPreflight,
  };
}

export { createWorkflowPreflightControllerUi };
