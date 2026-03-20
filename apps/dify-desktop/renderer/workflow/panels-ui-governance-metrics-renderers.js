function createWorkflowPanelsGovernanceMetricsRenderers(els) {
  function renderTimelineRows(out) {
    if (!els.timelineRows) return;
    if (!out?.ok) {
      els.timelineRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const rows = Array.isArray(out.timeline) ? out.timeline : [];
    if (!rows.length) {
      els.timelineRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.timelineRows.innerHTML = "";
    rows.forEach((r) => {
      const tr = document.createElement("tr");
      const tdNode = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdSec = document.createElement("td");
      tdNode.textContent = `${String(r.node_id || "")}(${String(r.type || "")})`;
      tdStatus.textContent = String(r.status || "");
      tdSec.textContent = Number(r.seconds || 0).toFixed(3);
      tr.append(tdNode, tdStatus, tdSec);
      els.timelineRows.appendChild(tr);
    });
  }

  function renderFailureRows(out) {
    if (!els.failureRows) return;
    const byNode = out?.by_node && typeof out.by_node === "object" ? out.by_node : {};
    const entries = Object.entries(byNode);
    if (!entries.length) {
      els.failureRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.failureRows.innerHTML = "";
    entries
      .sort((a, b) => Number(b[1]?.failed || 0) - Number(a[1]?.failed || 0))
      .slice(0, 50)
      .forEach(([k, v]) => {
        const tr = document.createElement("tr");
        const tdNode = document.createElement("td");
        const tdCnt = document.createElement("td");
        const tdErr = document.createElement("td");
        tdNode.textContent = String(k);
        tdCnt.textContent = String(v?.failed || 0);
        tdErr.textContent = Array.isArray(v?.samples) ? String(v.samples[0] || "") : "";
        tr.append(tdNode, tdCnt, tdErr);
        els.failureRows.appendChild(tr);
      });
  }

  function renderAuditRows(items = []) {
    if (!els.auditRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.auditRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.auditRows.innerHTML = "";
    items.slice(0, 100).forEach((it) => {
      const tr = document.createElement("tr");
      const tdTs = document.createElement("td");
      const tdAction = document.createElement("td");
      const tdDetail = document.createElement("td");
      tdTs.textContent = String(it.ts || "");
      tdAction.textContent = String(it.action || "");
      tdDetail.textContent = JSON.stringify(it.detail || {});
      tr.append(tdTs, tdAction, tdDetail);
      els.auditRows.appendChild(tr);
    });
  }

  return {
    renderTimelineRows,
    renderFailureRows,
    renderAuditRows,
  };
}

export { createWorkflowPanelsGovernanceMetricsRenderers };
