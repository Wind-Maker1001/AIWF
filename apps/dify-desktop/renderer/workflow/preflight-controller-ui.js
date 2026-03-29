import {
  buildWorkflowContractValidationPayload,
  buildIoContractInput,
  postRustOperator as defaultPostRustOperator,
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
    postRustOperator = defaultPostRustOperator,
  } = deps;

  function autoFixGraphStructure() {
    const out = autoFixWorkflowGraph(exportGraph());
    applyGraph(out.graph);
    return out.summary;
  }

  async function runWorkflowPreflight() {
    const graph = graphPayload();
    const issues = [];

    const {
      endpoint,
      rustRequired,
      rustNodes,
      firstInputFile,
    } = collectRustPreflightContext(graph, els);

    if (!endpoint) {
      if ((Array.isArray(graph?.nodes) ? graph.nodes.length : 0) > 0) {
        issues.push({
          level: "error",
          kind: "rust",
          message: "Rust Endpoint 涓虹┖锛屾棤娉曟墽琛屽绾﹂妫€",
        });
      }
    } else {
      try {
        const workflowContractRes = await postRustOperator(
          endpoint,
          "/operators/workflow_contract_v1/validate",
          buildWorkflowContractValidationPayload(graph, {
            allowVersionMigration: true,
            requireNonEmptyNodes: true,
            validationScope: "authoring",
          }),
        );
        if (!workflowContractRes.ok) {
          issues.push({
            level: rustRequired ? "error" : "warning",
            kind: "rust",
            message: `workflow 濂戠害鏍￠獙璇锋眰澶辫触: ${workflowContractRes.error || "unknown"}`,
          });
        } else {
          const body = workflowContractRes.body || {};
          if (body.valid === false || String(body.status || "").trim().toLowerCase() === "invalid") {
            issues.push(...buildGraphValidationIssues({
              errors: Array.isArray(body.error_items)
                ? body.error_items.map((item) => String(item?.message || "")).filter(Boolean)
                : [String(body.error || "workflow contract invalid")],
              error_items: Array.isArray(body.error_items)
                ? body.error_items.map((item) => ({
                  ...item,
                  error_contract: body.error_item_contract || "",
                }))
                : [],
              warnings: Array.isArray(body.notes) ? body.notes : [],
              warning_items: Array.isArray(body.notes)
                ? body.notes.map((note) => ({
                  code: "migration_note",
                  path: "workflow.version",
                  message: String(note || ""),
                  error_contract: body.error_item_contract || "",
                }))
                : [],
            }));
          }
        }
      } catch (error) {
        issues.push({
          level: rustRequired ? "error" : "warning",
          kind: "rust",
          message: `workflow 濂戠害鏍￠獙寮傚父: ${String(error)}`,
        });
      }

      if (rustNodes.length > 0) {
        try {
          const ops = Array.from(new Set(rustNodes.map((node) => String(node.type || ""))));
          const capRes = await postRustOperator(endpoint, "/operators/capabilities_v1", { include_ops: ops });
          if (!capRes.ok) {
            issues.push({
              level: rustRequired ? "error" : "warning",
              kind: "rust",
              message: `鑳藉姏鍙戠幇澶辫触: ${capRes.error || "unknown"}`,
            });
          }
        } catch (error) {
          issues.push({
            level: rustRequired ? "error" : "warning",
            kind: "rust",
            message: `鑳藉姏鍙戠幇寮傚父: ${String(error)}`,
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
                message: `${operator} 濂戠害鏍￠獙璇锋眰澶辫触: ${out.error || "unknown"}`,
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
                message: `${operator} 濂戠害涓嶉€氳繃: ${errs.join("; ") || "unknown"}`,
              });
            }
          } catch (error) {
            issues.push({
              level: rustRequired ? "error" : "warning",
              kind: "io_contract",
              node_id: String(node?.id || ""),
              message: `${operator} 濂戠害鏍￠獙寮傚父: ${String(error)}`,
            });
          }
        }
      }
    }

    const ok = issues.every((issue) => String(issue.level || "") !== "error");
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
