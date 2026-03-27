import { formatAiwfError } from "./workflow-contract.js";

function createWorkflowPanelsGovernanceReviewRenderers(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshRunHistory = async () => {},
    refreshReviewQueue = async () => {},
    refreshReviewHistory = async () => {},
  } = deps;

  function reviewSubmitStatusText(out, approved) {
    if (out?.ok) {
      return approved ? "审核已批准并自动续跑" : "审核已驳回并自动续跑";
    }
    if (out?.review_saved && out?.resumed) {
      return approved
        ? `审核已批准，但自动续跑失败: ${formatAiwfError(out.resumed)}`
        : `审核已驳回，但自动续跑失败: ${formatAiwfError(out.resumed)}`;
    }
    return `审核失败: ${formatAiwfError(out)}`;
  }

  function renderQualityGateRows(items = []) {
    if (!els.qualityGateRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.qualityGateRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.qualityGateRows.innerHTML = "";
    items.slice(0, 100).forEach((it) => {
      const tr = document.createElement("tr");
      const tdRun = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdIssues = document.createElement("td");
      const tdTs = document.createElement("td");
      tdRun.textContent = String(it.run_id || "").slice(0, 12);
      const gate = it.quality_gate && typeof it.quality_gate === "object" ? it.quality_gate : {};
      const status = gate.blocked ? "blocked" : (gate.passed ? "pass" : "unknown");
      tdStatus.textContent = status;
      tdStatus.style.color = status === "pass" ? "#087443" : (status === "blocked" ? "#b42318" : "#5c6b7a");
      tdIssues.textContent = Array.isArray(gate.issues) ? gate.issues.join(",") : "";
      tdTs.textContent = String(it.ts || "");
      tr.append(tdRun, tdStatus, tdIssues, tdTs);
      els.qualityGateRows.appendChild(tr);
    });
  }

  function qualityGateFilterPayload() {
    return {
      run_id: String(els.qualityGateRunIdFilter?.value || "").trim(),
      status: String(els.qualityGateStatusFilter?.value || "all").trim().toLowerCase() || "all",
    };
  }

  function qualityGatePrefsPayload() {
    const filter = qualityGateFilterPayload();
    const format = String(els.qualityGateExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
    return { filter, format };
  }

  function renderReviewRows(items = []) {
    if (!els.reviewRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.reviewRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.reviewRows.innerHTML = "";
    items.slice(0, 30).forEach((it) => {
      const tr = document.createElement("tr");
      const tdKey = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdOp = document.createElement("td");
      tdKey.textContent = `${String(it.review_key || "")} (${String(it.run_id || "").slice(0, 8)})`;
      tdStatus.textContent = String(it.status || "pending");
      const okBtn = document.createElement("button");
      okBtn.className = "mini";
      okBtn.textContent = "批准";
      okBtn.onclick = async () => {
        const reviewer = String(prompt("审核人", String(it.reviewer || "reviewer")) || "").trim();
        const comment = String(prompt("备注", "") || "").trim();
        const out = await window.aiwfDesktop.submitManualReview({
          run_id: it.run_id,
          review_key: it.review_key,
          approved: true,
          reviewer,
          comment,
          auto_resume: true,
        });
        if (out?.resumed) els.log.textContent = JSON.stringify(out.resumed, null, 2);
        setStatus(reviewSubmitStatusText(out, true), !!out?.ok);
        await refreshReviewQueue();
        await refreshRunHistory();
        await refreshReviewHistory();
      };
      const noBtn = document.createElement("button");
      noBtn.className = "mini del";
      noBtn.style.marginLeft = "4px";
      noBtn.textContent = "驳回";
      noBtn.onclick = async () => {
        const reviewer = String(prompt("审核人", String(it.reviewer || "reviewer")) || "").trim();
        const comment = String(prompt("备注", "") || "").trim();
        const out = await window.aiwfDesktop.submitManualReview({
          run_id: it.run_id,
          review_key: it.review_key,
          approved: false,
          reviewer,
          comment,
          auto_resume: true,
        });
        if (out?.resumed) els.log.textContent = JSON.stringify(out.resumed, null, 2);
        setStatus(reviewSubmitStatusText(out, false), !!out?.ok);
        await refreshReviewQueue();
        await refreshRunHistory();
        await refreshReviewHistory();
      };
      tdOp.append(okBtn, noBtn);
      tr.append(tdKey, tdStatus, tdOp);
      els.reviewRows.appendChild(tr);
    });
  }

  return {
    renderQualityGateRows,
    qualityGateFilterPayload,
    qualityGatePrefsPayload,
    renderReviewRows,
  };
}

export { createWorkflowPanelsGovernanceReviewRenderers };
