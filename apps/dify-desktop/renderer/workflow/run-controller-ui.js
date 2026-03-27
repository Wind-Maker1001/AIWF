import { formatWorkflowContractError } from "./workflow-contract.js";

function createWorkflowRunControllerUi(els, deps = {}) {
  const {
    setStatus = () => {},
    runWorkflowPreflight = async () => ({ ok: true, issues: [] }),
    runPayload = () => ({}),
    renderNodeRuns = () => {},
    refreshDiagnostics = async () => {},
    refreshRunHistory = async () => {},
    refreshReviewQueue = async () => {},
    refreshQueue = async () => {},
  } = deps;

  function issueStatusText(issue = {}) {
    const message = String(issue?.message || "").trim();
    const resolution = String(issue?.resolution_hint || "").trim();
    return resolution ? `${message} | ${resolution}` : message;
  }

  function preflightMessages(pre) {
    const issues = Array.isArray(pre?.issues) ? pre.issues : [];
    const warns = issues.filter((x) => String(x.level || "") === "warning").map((x) => issueStatusText(x));
    const errs = issues.filter((x) => String(x.level || "") === "error").map((x) => issueStatusText(x));
    return { warns, errs };
  }

  async function runWorkflow() {
    const pre = await runWorkflowPreflight();
    const strictOutputGate = true;
    const { warns, errs } = preflightMessages(pre);
    if (!pre.ok && !strictOutputGate) {
      setStatus(`预检失败: ${errs.join(" | ")}`, false);
      return;
    }
    if (warns.length) {
      setStatus(`预检警告: ${warns.join(" | ")}`, true);
    }
    if (!pre.ok && strictOutputGate) {
      setStatus("预检未通过，已启用严格产物门禁：本次仅输出 Markdown 熟肉。", true);
    }
    setStatus("工作流运行中...");
    try {
      const out = await window.aiwfDesktop.runWorkflow(runPayload({
        params: {
          strict_output_gate: strictOutputGate,
          preflight_passed: !!pre.ok,
          preflight_risk_score: Number(pre?.risk?.score || 0),
          preflight_risk_label: String(pre?.risk?.label || ""),
        },
      }), {});
      if (els.log) els.log.textContent = JSON.stringify(out, null, 2);
      renderNodeRuns(out?.node_runs || []);
      await refreshDiagnostics();
      await refreshRunHistory();
      await refreshReviewQueue();
      if (out?.ok) {
        const slaPassed = out?.sla?.passed !== false;
        const lineageEdges = Number(out?.lineage?.edge_count || 0);
        const aiCalls = Number(out?.governance?.ai_budget?.calls || 0);
        setStatus(`运行完成: ${out.run_id} | SLA:${slaPassed ? "通过" : "未通过"} | 血缘边:${lineageEdges} | AI调用:${aiCalls}`, true);
      } else if (out?.error) {
        setStatus(`运行失败: ${formatWorkflowContractError(out)}`, false);
      } else {
        setStatus(`运行结束: ${out?.status || "failed"}`, false);
      }
    } catch (error) {
      setStatus(`运行失败: ${formatWorkflowContractError(error)}`, false);
    }
  }

  async function enqueueWorkflowRun() {
    const pre = await runWorkflowPreflight();
    const strictOutputGate = true;
    const { warns, errs } = preflightMessages(pre);
    if (!pre.ok && !strictOutputGate) {
      setStatus(`预检失败: ${errs.join(" | ")}`, false);
      return;
    }
    if (warns.length) {
      setStatus(`预检警告: ${warns.join(" | ")}`, true);
    }
    if (!pre.ok && strictOutputGate) {
      setStatus("预检未通过，入队任务将自动降级为 md_only。", true);
    }
    try {
      const out = await window.aiwfDesktop.enqueueWorkflowTask({
        label: String(els.workflowName?.value || "workflow_task"),
        payload: runPayload({
          params: {
            strict_output_gate: strictOutputGate,
            preflight_passed: !!pre.ok,
            preflight_risk_score: Number(pre?.risk?.score || 0),
            preflight_risk_label: String(pre?.risk?.label || ""),
          },
        }),
        cfg: {},
        priority: 100,
      });
      setStatus(out?.ok ? "任务已加入队列" : `入队失败: ${formatWorkflowContractError(out)}`, !!out?.ok);
      await refreshQueue();
    } catch (error) {
      setStatus(`入队失败: ${formatWorkflowContractError(error)}`, false);
    }
  }

  return {
    runWorkflow,
    enqueueWorkflowRun,
  };
}

export { createWorkflowRunControllerUi };
