function createWorkflowSupportReviewHistory(els, deps = {}) {
  const { setStatus = () => {} } = deps;

  function reviewHistoryFilterPayload() {
    return {
      run_id: String(els.reviewHistoryRunId?.value || "").trim(),
      reviewer: String(els.reviewHistoryReviewer?.value || "").trim(),
      status: String(els.reviewHistoryStatus?.value || "").trim(),
      date_from: String(els.reviewHistoryDateFrom?.value || "").trim(),
      date_to: String(els.reviewHistoryDateTo?.value || "").trim(),
    };
  }

  function renderReviewHistoryRows(items = []) {
    if (!els.reviewHistoryRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.reviewHistoryRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.reviewHistoryRows.innerHTML = "";
    items.slice(0, 100).forEach((item) => {
      const tr = document.createElement("tr");
      const tdKey = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdReviewer = document.createElement("td");
      const tdTime = document.createElement("td");
      tdKey.textContent = String(item.review_key || "");
      const status = String(item.status || "");
      tdStatus.textContent = status;
      if (status === "approved") tdStatus.style.color = "#087443";
      if (status === "rejected") tdStatus.style.color = "#b42318";
      tdReviewer.textContent = String(item.reviewer || "");
      tdTime.textContent = String(item.decided_at || "");
      tr.append(tdKey, tdStatus, tdReviewer, tdTime);
      els.reviewHistoryRows.appendChild(tr);
    });
  }

  async function refreshReviewHistory() {
    try {
      const out = await window.aiwfDesktop.listManualReviewHistory({
        limit: 300,
        filter: reviewHistoryFilterPayload(),
      });
      renderReviewHistoryRows(out?.items || []);
    } catch {
      renderReviewHistoryRows([]);
    }
  }

  async function exportReviewHistory() {
    try {
      const out = await window.aiwfDesktop.exportManualReviewHistory({
        filter: reviewHistoryFilterPayload(),
      });
      if (!out?.ok) {
        if (!out?.canceled) setStatus(`导出审核历史失败: ${out?.error || "unknown"}`, false);
        return;
      }
      setStatus(`审核历史已导出: ${out.path} (${out.total} 条)`, true);
    } catch (error) {
      setStatus(`导出审核历史失败: ${error}`, false);
    }
  }

  return {
    exportReviewHistory,
    refreshReviewHistory,
    renderReviewHistoryRows,
    reviewHistoryFilterPayload,
  };
}

export { createWorkflowSupportReviewHistory };
