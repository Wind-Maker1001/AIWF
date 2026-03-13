function createWorkflowPanelsUi(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshRunHistory = async () => {},
    refreshReviewQueue = async () => {},
    showReviewQueue = async () => { await refreshReviewQueue(); },
    showQualityGate = async () => {},
    refreshReviewHistory = async () => {},
    refreshQueue = async () => {},
    refreshDiagnostics = async () => {},
    refreshSandboxRuleVersions = async () => {},
    refreshSandboxAlerts = async () => {},
    applySandboxRulesToUi = () => {},
    applyRestoredGraph = () => {},
    renderSandboxHealth = () => {},
    normalizeAppSchemaObject = (obj) => obj,
    renderAppSchemaForm = () => {},
    appSchemaRowsFromObject = () => [],
    renderRunParamsFormBySchema = () => {},
    collectRunParamsForm = () => ({}),
    runPayload = () => ({}),
  } = deps;

  function statusColor(status) {
    const s = String(status || "").trim().toLowerCase();
    if (!s) return "";
    if (s === "done" || s === "passed" || s === "approved") return "#087443";
    if (s === "failed" || s === "forbidden_graph" || s === "invalid_graph" || s === "rejected") return "#b42318";
    if (s === "quality_blocked" || s === "pending_review" || s === "blocked") return "#b54708";
    if (s === "canceled" || s === "cancelled" || s === "queued") return "#5c6b7a";
    if (s === "running") return "#1d4ed8";
    return "";
  }

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
    changes.forEach((c) => {
      const tr = document.createElement("tr");
      const tdPath = document.createElement("td");
      const tdBefore = document.createElement("td");
      const tdAfter = document.createElement("td");
      const tdReason = document.createElement("td");
      tdPath.textContent = String(c.path || "");
      tdBefore.textContent = String(c.before || "");
      tdAfter.textContent = String(c.after || "");
      tdReason.textContent = String(c.reason || "");
      tr.append(tdPath, tdBefore, tdAfter, tdReason);
      els.migrationRows.appendChild(tr);
    });
  }

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
        select.title = "该运行需要走专用处理路径，节点继续选择已暂时不可用";
      }
      const btn = document.createElement("button");
      btn.className = "mini";
      btn.textContent = "续跑";
      const btnRetryFailed = document.createElement("button");
      btnRetryFailed.className = "mini";
      btnRetryFailed.style.marginRight = "4px";
      btnRetryFailed.textContent = "重试失败节点";
      const btnA = document.createElement("button");
      btnA.className = "mini";
      btnA.style.marginRight = "4px";
      btnA.textContent = "设为A";
      btnA.onclick = () => {
        if (els.compareRunA) els.compareRunA.value = String(it.run_id || "");
      };
      const btnB = document.createElement("button");
      btnB.className = "mini";
      btnB.style.marginRight = "4px";
      btnB.textContent = "设为B";
      btnB.onclick = () => {
        if (els.compareRunB) els.compareRunB.value = String(it.run_id || "");
      };
      if (runStatus === "pending_review") {
        btn.textContent = "去审核队列";
        btn.title = "该运行等待人工审核，请在审核队列中处理";
      }
      if (runStatus === "pending_review") btn.textContent = "从节点继续";
      if (!preferred.length) {
        btn.disabled = true;
        btn.title = "鏃犲彲续跑鑺傜偣";
      }
      if (!failedNodes.length) {
        btnRetryFailed.disabled = true;
        btnRetryFailed.title = "璇ヨ繍琛屾棤澶辫触鑺傜偣";
      }
      if (runStatus === "pending_review") btn.disabled = false;
      if (runStatus === "quality_blocked") {
        btn.textContent = "查看质量门禁";
        btn.title = "该运行被质量门禁拦截，请查看质量门禁报告";
        btn.disabled = false;
        btnRetryFailed.disabled = true;
      }
      btn.onclick = async () => {
        if (runStatus === "pending_review") {
          await showReviewQueue();
          setStatus(`该运行 ${String(it.run_id || "").slice(0, 10)} 等待审核，请在审核队列中处理`, false);
          return;
        }
        if (runStatus === "quality_blocked") {
          await showQualityGate(String(it.run_id || ""));
          setStatus(`该运行 ${String(it.run_id || "").slice(0, 10)} 琚川閲忛棬绂佹嫤鎴紝璇锋煡鐪嬭川閲忛棬绂佹姤鍛?`, false);
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
      tdOp.append(select, btnA, btnB, btnRetryFailed, btn);
      tr.append(tdRun, tdStatus, tdOp);
      els.runHistoryRows.appendChild(tr);
    });
  }

  function renderQueueRows(items = []) {
    if (!els.queueRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.queueRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.queueRows.innerHTML = "";
    items.slice(0, 80).forEach((it) => {
      const tr = document.createElement("tr");
      const tdTask = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdOp = document.createElement("td");
      tdTask.textContent = `${String(it.label || "task")} (${String(it.task_id || "").slice(0, 8)})`;
      tdStatus.textContent = String(it.status || "");
      if (String(it.status || "") === "failed") tdStatus.style.color = "#b42318";
      if (String(it.status || "") === "done") tdStatus.style.color = "#087443";
      const queueStatusColor = statusColor(it?.status);
      if (queueStatusColor) tdStatus.style.color = queueStatusColor;
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "mini del";
      cancelBtn.textContent = "取消";
      cancelBtn.onclick = async () => {
        const out = await window.aiwfDesktop.cancelWorkflowTask({ task_id: it.task_id });
        setStatus(out?.ok ? "已取消任务" : `取消失败: ${out?.error || "unknown"}`, !!out?.ok);
        await refreshQueue();
      };
      const retryBtn = document.createElement("button");
      retryBtn.className = "mini";
      retryBtn.style.marginLeft = "4px";
      retryBtn.textContent = "重试";
      retryBtn.onclick = async () => {
        const out = await window.aiwfDesktop.retryWorkflowTask({ task_id: it.task_id });
        setStatus(out?.ok ? "已加入重试队列" : `重试失败: ${out?.error || "unknown"}`, !!out?.ok);
        await refreshQueue();
      };
      tdOp.append(cancelBtn, retryBtn);
      tr.append(tdTask, tdStatus, tdOp);
      els.queueRows.appendChild(tr);
    });
  }

  function renderQueueControl(control) {
    if (!els.queueControlText) return;
    const paused = !!control?.paused;
    const quotas = control?.quotas && typeof control.quotas === "object" ? control.quotas : {};
    const quotaText = Object.keys(quotas).length
      ? Object.entries(quotas).map(([k, v]) => `${k}:${v}`).join(", ")
      : "默认";
    els.queueControlText.textContent = `队列状态: ${paused ? "暂停" : "运行"} | 并发配额: ${quotaText}`;
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
        if (!out?.ok) {
          setStatus(`恢复版本失败: ${out?.error || "unknown"}`, false);
          return;
        }
        store.importGraph(out.graph || {});
        selectedEdge = null;
        renderAll();
        setStatus(`已恢复版本: ${String(it.version_id || "").slice(0, 8)}`, true);
      };
      const setABtn = document.createElement("button");
      setABtn.className = "mini";
      setABtn.style.marginLeft = "4px";
      setABtn.textContent = "设A";
      setABtn.onclick = () => {
        if (els.versionCompareA) els.versionCompareA.value = String(it.version_id || "");
      };
      const setBBtn = document.createElement("button");
      setBBtn.className = "mini";
      setBBtn.style.marginLeft = "4px";
      setBBtn.textContent = "设B";
      setBBtn.onclick = () => {
        if (els.versionCompareB) els.versionCompareB.value = String(it.version_id || "");
      };
      tdOp.append(restoreBtn, setABtn, setBBtn);
      tr.append(tdName, tdTs, tdOp);
      els.versionRows.appendChild(tr);
    });
  }

  function renderVersionCompare(out) {
    if (!els.versionCompareSummary || !els.versionCompareRows) return;
    if (!out?.ok) {
      els.versionCompareSummary.textContent = `版本对比失败: ${out?.error || "unknown"}`;
      els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const s = out.summary || {};
    els.versionCompareSummary.textContent = `A:${String(s.version_a || "").slice(0, 8)} B:${String(s.version_b || "").slice(0, 8)} 节点变化:${Number(s.changed_nodes || 0)} 边新增:${Number(s.added_edges || 0)} 边删除:${Number(s.removed_edges || 0)}`;
    const rows = Array.isArray(out.node_diff) ? out.node_diff.filter((x) => String(x.change || "") !== "same") : [];
    if (!rows.length) {
      els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无节点变化</td></tr>';
      return;
    }
    els.versionCompareRows.innerHTML = "";
    rows.slice(0, 80).forEach((r) => {
      const tr = document.createElement("tr");
      const tdId = document.createElement("td");
      const tdChange = document.createElement("td");
      const tdType = document.createElement("td");
      tdId.textContent = String(r.id || "");
      tdChange.textContent = String(r.change || "");
      tdType.textContent = `${String(r.type_a || "-")} / ${String(r.type_b || "-")}`;
      if (String(r.change || "") === "removed") tdChange.style.color = "#b42318";
      if (String(r.change || "") === "added") tdChange.style.color = "#087443";
      tr.append(tdId, tdChange, tdType);
      els.versionCompareRows.appendChild(tr);
    });
  }

  function renderCacheStats(stats) {
    if (!els.cacheStatsText) return;
    if (!stats || typeof stats !== "object") {
      els.cacheStatsText.textContent = "缓存状态: -";
      return;
    }
    els.cacheStatsText.textContent = `缓存项:${Number(stats.entries || 0)} 命中:${Number(stats.hits || 0)} 未命中:${Number(stats.misses || 0)} 命中率:${Number(stats.hit_rate || 0)}`;
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
        setStatus(out?.ok ? "应用运行完成" : `应用运行失败: ${out?.error || "unknown"}`, !!out?.ok);
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
    entries.sort((a, b) => Number(b[1]?.failed || 0) - Number(a[1]?.failed || 0)).slice(0, 50).forEach(([k, v]) => {
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
        setStatus(out?.ok ? "审核已批准并自动续跑" : `审核失败: ${out?.error || "unknown"}`, !!out?.ok);
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
        setStatus(out?.ok ? "审核已驳回并自动续跑" : `审核失败: ${out?.error || "unknown"}`, !!out?.ok);
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
    renderMigrationReport,
    renderRunHistoryRows,
    renderQueueRows,
    renderQueueControl,
    renderVersionRows,
    renderVersionCompare,
    renderCacheStats,
    renderAppRows,
    renderTimelineRows,
    renderFailureRows,
    renderSandboxRows,
    renderSandboxRuleVersionRows,
    renderSandboxAutoFixRows,
    renderQualityGateRows,
    qualityGateFilterPayload,
    qualityGatePrefsPayload,
    renderAuditRows,
    renderReviewRows,
  };
}

export { createWorkflowPanelsUi };
