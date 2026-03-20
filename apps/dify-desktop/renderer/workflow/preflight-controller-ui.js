import { validateGraph } from "./graph.js";
import {
  buildIoContractInput,
  postRustOperator,
} from "./preflight-rust-helpers.js";
import {
  autoFixWorkflowGraph,
  buildGraphValidationIssues,
  collectRustPreflightContext,
} from "./preflight-controller-support.js";

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
    const out = autoFixWorkflowGraph(exportGraph());
    applyGraph(out.graph);
    return out.summary;
  }

  async function runWorkflowPreflight() {
    const graph = graphPayload();
    const valid = validateGraph(graph);
    const issues = buildGraphValidationIssues(valid);

    const {
      endpoint,
      rustRequired,
      rustNodes,
      firstInputFile,
    } = collectRustPreflightContext(graph, els);

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
