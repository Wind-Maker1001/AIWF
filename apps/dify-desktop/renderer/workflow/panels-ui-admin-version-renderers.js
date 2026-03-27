import { formatAiwfError } from "./workflow-contract.js";

function createWorkflowPanelsAdminVersionRenderers(els, deps = {}) {
  const { setStatus = () => {} } = deps;

  function renderMigrationReport(migrated) {
    if (!els.migrationSummary || !els.migrationRows) return;
    if (!migrated || !migrated.migrated) {
      els.migrationSummary.textContent = "暂无迁移";
      els.migrationRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const changes = Array.isArray(migrated.changes) ? migrated.changes : [];
    els.migrationSummary.textContent = `已迁移: ${migrated.notes.join(", ")} | 变更 ${changes.length} 项`;
    if (!changes.length) {
      els.migrationRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">无逐字段变更</td></tr>';
      return;
    }
    els.migrationRows.innerHTML = "";
    changes.forEach((change) => {
      const tr = document.createElement("tr");
      const tdPath = document.createElement("td");
      const tdBefore = document.createElement("td");
      const tdAfter = document.createElement("td");
      const tdReason = document.createElement("td");
      tdPath.textContent = String(change.path || "");
      tdBefore.textContent = String(change.before || "");
      tdAfter.textContent = String(change.after || "");
      tdReason.textContent = String(change.reason || "");
      tr.append(tdPath, tdBefore, tdAfter, tdReason);
      els.migrationRows.appendChild(tr);
    });
  }

  function renderVersionRows(items = []) {
    if (!els.versionRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.versionRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.versionRows.innerHTML = "";
    items.slice(0, 80).forEach((it) => {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      const tdTs = document.createElement("td");
      const tdOp = document.createElement("td");
      tdName.textContent = `${String(it.workflow_name || "")} (${String(it.version_id || "").slice(0, 8)})`;
      tdTs.textContent = String(it.ts || "");
      const restoreBtn = document.createElement("button");
      restoreBtn.className = "mini";
      restoreBtn.textContent = "恢复";
      restoreBtn.onclick = async () => {
        const out = await window.aiwfDesktop.restoreWorkflowVersion({ version_id: it.version_id });
        if (!out?.ok || !out?.graph) {
          setStatus(`恢复失败: ${formatAiwfError(out)}`, false);
          return;
        }
        try {
          window.aiwfDesktop.__applyRestoredWorkflowGraph?.(out.graph);
          setStatus(`已恢复版本: ${String(it.version_id || "").slice(0, 8)}`, true);
        } catch (error) {
          setStatus(`恢复失败: ${formatAiwfError(error)}`, false);
        }
      };
      tdOp.append(restoreBtn);
      tr.append(tdName, tdTs, tdOp);
      els.versionRows.appendChild(tr);
    });
  }

  function renderVersionCompare(out) {
    if (!els.versionCompareSummary || !els.versionCompareRows) return;
    if (!out?.ok) {
      els.versionCompareSummary.textContent = `版本对比失败: ${formatAiwfError(out)}`;
      els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const summary = out.summary || {};
    els.versionCompareSummary.textContent = `A:${String(summary.version_a || "").slice(0, 8)} B:${String(summary.version_b || "").slice(0, 8)} 节点变化:${Number(summary.changed_nodes || 0)} 边新增:${Number(summary.added_edges || 0)} 边删除:${Number(summary.removed_edges || 0)}`;
    const rows = Array.isArray(out.node_diff) ? out.node_diff.filter((item) => String(item.change || "") !== "same") : [];
    if (!rows.length) {
      els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无节点变化</td></tr>';
      return;
    }
    els.versionCompareRows.innerHTML = "";
    rows.slice(0, 80).forEach((row) => {
      const tr = document.createElement("tr");
      const tdId = document.createElement("td");
      const tdChange = document.createElement("td");
      const tdType = document.createElement("td");
      tdId.textContent = String(row.id || "");
      tdChange.textContent = String(row.change || "");
      tdType.textContent = `${String(row.type_a || "-")} / ${String(row.type_b || "-")}`;
      if (String(row.change || "") === "removed") tdChange.style.color = "#b42318";
      if (String(row.change || "") === "added") tdChange.style.color = "#087443";
      tr.append(tdId, tdChange, tdType);
      els.versionCompareRows.appendChild(tr);
    });
  }

  return {
    renderMigrationReport,
    renderVersionCompare,
    renderVersionRows,
  };
}

export { createWorkflowPanelsAdminVersionRenderers };
