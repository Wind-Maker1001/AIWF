import { statusColor } from "./panels-ui-run-shared.js";

function createWorkflowPanelsRunHistoryRenderers(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshRunHistory = async () => {},
    refreshReviewQueue = async () => {},
    showReviewQueue = async () => { await refreshReviewQueue(); },
    showQualityGate = async () => {},
  } = deps;

  function renderRunHistoryRows(items = []) {
    if (!els.runHistoryRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.runHistoryRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.runHistoryRows.innerHTML = "";
    items.slice(0, 30).forEach((it) => {
      const tr = document.createElement("tr");
      const tdRun = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdOp = document.createElement("td");
      tdRun.textContent = String(it.run_id || "").slice(0, 10);
      const tplIssues = Array.isArray(it?.result?.template_validation)
        ? it.result.template_validation.reduce((acc, x) => acc + (Array.isArray(x?.warnings) ? x.warnings.length : 0), 0)
        : 0;
      tdStatus.textContent = `${String(it.status || "")}${tplIssues > 0 ? ` | 模板告警:${tplIssues}` : ""}`;
      const runStatusColor = statusColor(it?.status);
      if (runStatusColor) tdStatus.style.color = runStatusColor;
      const runStatus = String(it.status || "").trim().toLowerCase();
      const guidedResumeStatus = runStatus === "pending_review" || runStatus === "quality_blocked";
      const nodeRuns = Array.isArray(it?.result?.node_runs) ? it.result.node_runs : [];
      const failedNodes = nodeRuns.filter((n) => String(n?.status || "") === "failed");
      const preferred = failedNodes.length ? failedNodes : nodeRuns;
      const select = document.createElement("select");
      select.style.maxWidth = "120px";
      select.style.marginRight = "4px";
      if (!preferred.length) {
        const op = document.createElement("option");
        op.value = "";
        op.textContent = "无节点";
        select.appendChild(op);
      } else {
        preferred.forEach((n) => {
          const op = document.createElement("option");
          op.value = String(n.id || "");
          op.textContent = `${String(n.id || "")}(${String(n.type || "")})`;
          select.appendChild(op);
        });
      }
      if (guidedResumeStatus) {
        select.disabled = true;
        select.title = "该运行需走专用处理流";
      }

      const btnAction = document.createElement("button");
      btnAction.className = "mini";
      btnAction.textContent = runStatus === "pending_review" ? "去审核" : (runStatus === "quality_blocked" ? "看门禁" : "续跑");
      btnAction.disabled = !guidedResumeStatus && !preferred.length;
      btnAction.onclick = async () => {
        if (runStatus === "pending_review") {
          await showReviewQueue();
          setStatus("已打开审核队列", true);
          return;
        }
        if (runStatus === "quality_blocked") {
          await showQualityGate(it.run_id);
          setStatus("已打开质量门禁报告", true);
          return;
        }
        const nodeId = String(select.value || "").trim();
        if (!nodeId) return;
        const out = await window.aiwfDesktop.replayWorkflowRun({ run_id: it.run_id, node_id: nodeId }, {});
        els.log.textContent = JSON.stringify(out, null, 2);
        await refreshRunHistory();
        await refreshReviewQueue();
        setStatus(out?.ok ? `续跑完成: ${it.run_id}` : `续跑失败: ${out?.error || "unknown"}`, !!out?.ok);
      };

      const btnCompare = document.createElement("button");
      btnCompare.className = "mini";
      btnCompare.style.marginLeft = "4px";
      btnCompare.textContent = "对比";
      btnCompare.onclick = async () => {
        if (els.compareRunA) els.compareRunA.value = String(it.run_id || "");
      };

      const btnRetryFailed = document.createElement("button");
      btnRetryFailed.className = "mini";
      btnRetryFailed.style.marginLeft = "4px";
      btnRetryFailed.textContent = "重试失败";
      btnRetryFailed.disabled = guidedResumeStatus;
      btnRetryFailed.onclick = async () => {
        if (runStatus === "pending_review") {
          setStatus("审核待处理运行不支持“重试失败节点”，请去审核队列", false);
          return;
        }
        if (runStatus === "quality_blocked") {
          setStatus("质量门禁运行不支持“重试失败节点”，请先查看质量门禁报告", false);
          return;
        }
        const failed = failedNodes[0];
        if (!failed?.id) {
          setStatus("该运行无失败节点", false);
          return;
        }
        const out = await window.aiwfDesktop.replayWorkflowRun({ run_id: it.run_id, node_id: String(failed.id) }, {});
        els.log.textContent = JSON.stringify(out, null, 2);
        await refreshRunHistory();
        await refreshReviewQueue();
        setStatus(out?.ok ? `失败节点重试完成: ${it.run_id}` : `失败节点重试失败: ${out?.error || "unknown"}`, !!out?.ok);
      };

      const btnLog = document.createElement("button");
      btnLog.className = "mini";
      btnLog.style.marginLeft = "4px";
      btnLog.textContent = "日志";
      btnLog.onclick = () => {
        els.log.textContent = JSON.stringify(it, null, 2);
      };

      tdOp.append(select, btnCompare, btnLog, btnRetryFailed, btnAction);
      tr.append(tdRun, tdStatus, tdOp);
      els.runHistoryRows.appendChild(tr);
    });
  }

  return {
    renderRunHistoryRows,
  };
}

export { createWorkflowPanelsRunHistoryRenderers };
