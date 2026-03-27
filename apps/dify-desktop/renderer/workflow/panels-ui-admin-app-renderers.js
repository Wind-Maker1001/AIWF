import { formatAiwfError } from "./workflow-contract.js";

function createWorkflowPanelsAdminAppRenderers(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshRunHistory = async () => {},
    refreshDiagnostics = async () => {},
    normalizeAppSchemaObject = (obj) => obj,
    renderAppSchemaForm = () => {},
    appSchemaRowsFromObject = () => [],
    renderRunParamsFormBySchema = () => {},
    collectRunParamsForm = () => ({}),
    runPayload = () => ({}),
  } = deps;

  function renderCacheStats(stats) {
    if (!els.cacheStatsText) return;
    if (!stats || typeof stats !== "object") {
      els.cacheStatsText.textContent = "缓存状态: -";
      return;
    }
    els.cacheStatsText.textContent = `缓存项:${Number(stats.entries || 0)} 命中:${Number(stats.hits || 0)} 未命中:${Number(stats.misses || 0)} 命中率:${Number(stats.hit_rate || 0)}`;
  }

  function appRunStatusText(out) {
    if (out?.ok) return "应用运行完成";
    if (out?.error || (Array.isArray(out?.error_items) && out.error_items.length)) {
      return `应用运行失败: ${formatAiwfError(out)}`;
    }
    const status = String(out?.status || out?.result?.status || "").trim();
    return status ? `应用运行结束: ${status}` : `应用运行失败: ${formatAiwfError(out)}`;
  }

  function renderAppRows(items = []) {
    if (!els.appRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.appRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.appRows.innerHTML = "";
    items.slice(0, 80).forEach((it) => {
      const tr = document.createElement("tr");
      const tdName = document.createElement("td");
      const tdTs = document.createElement("td");
      const tdOp = document.createElement("td");
      tdName.textContent = `${String(it.name || "")} (${String(it.app_id || "").slice(0, 8)})`;
      tdTs.textContent = String(it.updated_at || it.created_at || "");
      const runBtn = document.createElement("button");
      runBtn.className = "mini";
      runBtn.textContent = "运行";
      runBtn.onclick = async () => {
        renderRunParamsFormBySchema(it?.params_schema || {}, collectRunParamsForm());
        let params = collectRunParamsForm();
        if (!params || !Object.keys(params).length) {
          try { params = JSON.parse(String(els.appRunParams?.value || "{}") || "{}"); } catch { params = {}; }
        }
        const out = await window.aiwfDesktop.runWorkflowApp({
          app_id: it.app_id,
          params,
          payload: runPayload(),
        }, {});
        if (out?.result) {
          els.log.textContent = JSON.stringify(out.result, null, 2);
          if (els.timelineRunId) els.timelineRunId.value = String(out.result.run_id || "");
        }
        setStatus(appRunStatusText(out), !!out?.ok);
        await refreshRunHistory();
        await refreshDiagnostics();
      };
      const cfgBtn = document.createElement("button");
      cfgBtn.className = "mini";
      cfgBtn.style.marginLeft = "4px";
      cfgBtn.textContent = "载入参数";
      cfgBtn.onclick = () => {
        const schemaObj = normalizeAppSchemaObject(it?.params_schema || {});
        if (els.appSchemaJson) els.appSchemaJson.value = JSON.stringify(schemaObj, null, 2);
        renderAppSchemaForm(appSchemaRowsFromObject(schemaObj));
        renderRunParamsFormBySchema(schemaObj, (() => {
          try { return JSON.parse(String(els.appRunParams?.value || "{}")); } catch { return {}; }
        })());
        setStatus(`已载入应用参数: ${String(it.name || "").trim() || String(it.app_id || "").slice(0, 8)}`, true);
      };
      tdOp.append(runBtn, cfgBtn);
      tr.append(tdName, tdTs, tdOp);
      els.appRows.appendChild(tr);
    });
  }

  return {
    renderAppRows,
    renderCacheStats,
  };
}

export { createWorkflowPanelsAdminAppRenderers };
