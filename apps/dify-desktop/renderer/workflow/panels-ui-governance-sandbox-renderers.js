function createWorkflowPanelsGovernanceSandboxRenderers(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshSandboxRuleVersions = async () => {},
    refreshSandboxAlerts = async () => {},
    applySandboxRulesToUi = () => {},
    renderSandboxHealth = () => {},
  } = deps;

  function renderSandboxRows(out) {
    if (!els.sandboxRows) return;
    renderSandboxHealth(out?.health || {});
    const rows = Array.isArray(out?.by_node) ? out.by_node : [];
    if (!rows.length) {
      els.sandboxRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.sandboxRows.innerHTML = "";
    rows.slice(0, 50).forEach((r) => {
      const tr = document.createElement("tr");
      const tdNode = document.createElement("td");
      const tdCount = document.createElement("td");
      const tdRun = document.createElement("td");
      const tdOp = document.createElement("td");
      tdNode.textContent = `${String(r.node_type || "")}(${String(r.node_id || "")})`;
      tdCount.textContent = String(r.count || 0);
      tdRun.textContent = String(r.last_run_id || "").slice(0, 12);
      const muteBtn = document.createElement("button");
      muteBtn.className = "mini";
      muteBtn.textContent = "静默60m";
      muteBtn.onclick = async () => {
        const outMute = await window.aiwfDesktop.muteWorkflowSandboxAlert({
          node_type: String(r.node_type || "*"),
          node_id: String(r.node_id || "*"),
          code: "*",
          minutes: 60,
        });
        if (!outMute?.ok) {
          setStatus(`静默失败: ${outMute?.error || "unknown"}`, false);
          return;
        }
        setStatus(`已静默 ${String(r.node_type || "")}/${String(r.node_id || "")} 60分钟`, true);
        await refreshSandboxAlerts();
      };
      tdOp.appendChild(muteBtn);
      tr.append(tdNode, tdCount, tdRun, tdOp);
      els.sandboxRows.appendChild(tr);
    });
  }

  function renderSandboxRuleVersionRows(items = []) {
    if (!els.sandboxRuleVersionRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.sandboxRuleVersionRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.sandboxRuleVersionRows.innerHTML = "";
    items.slice(0, 60).forEach((it) => {
      const tr = document.createElement("tr");
      const tdVer = document.createElement("td");
      const tdTs = document.createElement("td");
      const tdOp = document.createElement("td");
      const vid = String(it.version_id || "");
      tdVer.textContent = vid.slice(0, 10);
      tdTs.textContent = String(it.ts || "");
      const rollbackBtn = document.createElement("button");
      rollbackBtn.className = "mini";
      rollbackBtn.textContent = "回滚";
      rollbackBtn.onclick = async () => {
        const out = await window.aiwfDesktop.rollbackWorkflowSandboxRuleVersion({ version_id: vid });
        if (!out?.ok) {
          setStatus(`回滚规则失败: ${out?.error || "unknown"}`, false);
          return;
        }
        applySandboxRulesToUi(out.rules || {});
        setStatus(`已回滚规则版本: ${vid.slice(0, 10)}`, true);
        await refreshSandboxRuleVersions();
        await refreshSandboxAlerts();
      };
      tdOp.appendChild(rollbackBtn);
      tr.append(tdVer, tdTs, tdOp);
      els.sandboxRuleVersionRows.appendChild(tr);
    });
  }

  function renderSandboxAutoFixRows(items = []) {
    if (!els.sandboxAutoFixRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.sandboxAutoFixRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.sandboxAutoFixRows.innerHTML = "";
    items.slice(0, 80).forEach((it) => {
      const tr = document.createElement("tr");
      const tdTs = document.createElement("td");
      const tdCnt = document.createElement("td");
      const tdAct = document.createElement("td");
      tdTs.textContent = String(it.ts || "");
      tdCnt.textContent = String(it.count || 0);
      tdAct.textContent = Array.isArray(it.actions) ? it.actions.join(",") : "";
      tr.append(tdTs, tdCnt, tdAct);
      els.sandboxAutoFixRows.appendChild(tr);
    });
  }

  return {
    renderSandboxRows,
    renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows,
  };
}

export { createWorkflowPanelsGovernanceSandboxRenderers };
