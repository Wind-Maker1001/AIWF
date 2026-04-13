import { formatAiwfError } from "./workflow-contract.js";

function createWorkflowSupportRunCompareRenderer(els) {
  function renderCompareResult(out) {
    if (!els.compareSummary || !els.compareRows) return;
    if (!out?.ok) {
      els.compareSummary.textContent = `运行对比失败: ${formatAiwfError(out)}`;
      els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const summary = out.summary || {};
    const onlyChanged = !!els.compareOnlyChanged?.checked;
    const onlyStatusChanged = !!els.compareOnlyStatusChanged?.checked;
    const minDelta = Number(els.compareMinDelta?.value || 0.001);
    const safeDelta = Number.isFinite(minDelta) ? Math.max(0, minDelta) : 0.001;
    const sourceRows = Array.isArray(out.node_diff) ? out.node_diff : [];
    const rows = sourceRows.filter((row) => {
      const delta = Math.abs(Number(row.seconds_delta || 0));
      if (onlyStatusChanged && !row.status_changed) return false;
      if (onlyChanged && !(row.status_changed || delta >= safeDelta)) return false;
      return true;
    });
    els.compareSummary.textContent = `Run A: ${summary.run_a || "-"} | Run B: ${summary.run_b || "-"} | 变更节点: ${summary.changed_nodes || 0} | 当前显示: ${rows.length}/${sourceRows.length}`;
    if (!rows.length) {
      els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.compareRows.innerHTML = "";
    rows.forEach((row) => {
      const tr = document.createElement("tr");
      const tdNode = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdA = document.createElement("td");
      const tdB = document.createElement("td");
      const tdD = document.createElement("td");
      tdNode.textContent = `${String(row.id || "")}(${String(row.type || "")})`;
      tdStatus.textContent = `${String(row.status_a || "")} / ${String(row.status_b || "")}`;
      tdA.textContent = Number(row.seconds_a || 0).toFixed(3);
      tdB.textContent = Number(row.seconds_b || 0).toFixed(3);
      const delta = Number(row.seconds_delta || 0);
      tdD.textContent = delta.toFixed(3);
      if (row.status_changed) tdStatus.style.color = "#b42318";
      if (delta > 0.001) tdD.style.color = "#b42318";
      if (delta < -0.001) tdD.style.color = "#087443";
      if (row.status_changed || Math.abs(delta) >= safeDelta) tr.style.background = "#fff8f2";
      tr.append(tdNode, tdStatus, tdA, tdB, tdD);
      els.compareRows.appendChild(tr);
    });
  }

  return { renderCompareResult };
}

export { createWorkflowSupportRunCompareRenderer };
