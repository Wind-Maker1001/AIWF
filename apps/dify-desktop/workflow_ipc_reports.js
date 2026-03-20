function createWorkflowReportSupport(deps) {
  const {
    deepClone,
    findRunById,
    listQualityRuleCenter,
    listRunBaselines,
  } = deps;

  function buildRunCompare(runA, runB) {
    const a = findRunById(runA);
    const b = findRunById(runB);
    if (!a || !b) return { ok: false, error: "run not found" };
    const nodesA = Array.isArray(a?.result?.node_runs) ? a.result.node_runs : [];
    const nodesB = Array.isArray(b?.result?.node_runs) ? b.result.node_runs : [];
    const mapB = new Map(nodesB.map((node) => [String(node.id || ""), node]));
    const nodeDiff = nodesA.map((nodeA) => {
      const nodeB = mapB.get(String(nodeA.id || ""));
      const statusA = String(nodeA.status || "");
      const statusB = String(nodeB?.status || "");
      const secondsA = Number(nodeA.seconds || 0);
      const secondsB = Number(nodeB?.seconds || 0);
      return {
        id: nodeA.id,
        type: nodeA.type,
        status_a: statusA,
        status_b: statusB,
        status_changed: statusA !== statusB,
        seconds_a: secondsA,
        seconds_b: secondsB,
        seconds_delta: Number((secondsB - secondsA).toFixed(3)),
      };
    });
    return {
      ok: true,
      summary: {
        run_a: runA,
        run_b: runB,
        status_a: String(a?.result?.status || ""),
        status_b: String(b?.result?.status || ""),
        ok_a: !!a?.result?.ok,
        ok_b: !!b?.result?.ok,
        node_count_a: nodesA.length,
        node_count_b: nodesB.length,
        changed_nodes: nodeDiff.filter((item) => item.status_changed || Math.abs(Number(item.seconds_delta || 0)) > 0.001).length,
      },
      node_diff: nodeDiff,
    };
  }

  function renderCompareMarkdown(output) {
    const summary = output.summary || {};
    const rows = Array.isArray(output.node_diff) ? output.node_diff : [];
    const lines = [
      "# AIWF 杩愯瀵规瘮鎶ュ憡",
      "",
      `- 鐢熸垚鏃堕棿: ${new Date().toISOString()}`,
      `- Run A: ${summary.run_a || "-"}`,
      `- Run B: ${summary.run_b || "-"}`,
      `- 鍙樺寲鑺傜偣鏁? ${Number(summary.changed_nodes || 0)}`,
      "",
      "| 鑺傜偣 | 鐘舵€丄 | 鐘舵€丅 | 鑰楁椂A(s) | 鑰楁椂B(s) | 螖(s) |",
      "|---|---|---:|---:|---:|---:|",
    ];
    rows.forEach((row) => {
      lines.push(`| ${String(row.id || "")}(${String(row.type || "")}) | ${String(row.status_a || "")} | ${String(row.status_b || "")} | ${Number(row.seconds_a || 0).toFixed(3)} | ${Number(row.seconds_b || 0).toFixed(3)} | ${Number(row.seconds_delta || 0).toFixed(3)} |`);
    });
    return `${lines.join("\n")}\n`;
  }

  function renderCompareHtml(output) {
    const summary = output.summary || {};
    const rows = Array.isArray(output.node_diff) ? output.node_diff : [];
    const tableRows = rows.map((row) => {
      const changed = row.status_changed || Math.abs(Number(row.seconds_delta || 0)) > 0.001;
      const bg = changed ? " style=\"background:#fff8f2\"" : "";
      return `<tr${bg}><td>${String(row.id || "")}(${String(row.type || "")})</td><td>${String(row.status_a || "")}</td><td>${String(row.status_b || "")}</td><td>${Number(row.seconds_a || 0).toFixed(3)}</td><td>${Number(row.seconds_b || 0).toFixed(3)}</td><td>${Number(row.seconds_delta || 0).toFixed(3)}</td></tr>`;
    }).join("");
    return `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8"/><title>AIWF 杩愯瀵规瘮鎶ュ憡</title><style>body{font-family:"Segoe UI","Microsoft YaHei",sans-serif;padding:16px;color:#1f2d3d}table{border-collapse:collapse;width:100%}th,td{border:1px solid #d8e1ec;padding:6px 8px;font-size:13px}th{background:#f3f7fd;text-align:left}</style></head><body><h2>AIWF 杩愯瀵规瘮鎶ュ憡</h2><p>鐢熸垚鏃堕棿: ${new Date().toISOString()}<br/>Run A: ${summary.run_a || "-"}<br/>Run B: ${summary.run_b || "-"}<br/>鍙樺寲鑺傜偣鏁? ${Number(summary.changed_nodes || 0)}</p><table><thead><tr><th>鑺傜偣</th><th>鐘舵€丄</th><th>鐘舵€丅</th><th>鑰楁椂A(s)</th><th>鑰楁椂B(s)</th><th>螖(s)</th></tr></thead><tbody>${tableRows}</tbody></table></body></html>`;
  }

  function renderPreflightMarkdown(report) {
    const safeReport = report && typeof report === "object" ? report : {};
    const issues = Array.isArray(safeReport.issues) ? safeReport.issues : [];
    const risk = safeReport && typeof safeReport.risk === "object" ? safeReport.risk : null;
    const lines = [
      "# AIWF 杩愯鍓嶉妫€鎶ュ憡",
      "",
      `- 鐢熸垚鏃堕棿: ${new Date().toISOString()}`,
      `- 棰勬鏃堕棿: ${String(safeReport.ts || "") || "-"}`,
      `- 鏄惁閫氳繃: ${safeReport.ok ? "true" : "false"}`,
      `- 闂鏁? ${issues.length}`,
      "",
      "| 绾у埆 | 绫诲瀷 | 鑺傜偣ID | 璇存槑 |",
      "|---|---|---|---|",
    ];
    if (risk) lines.splice(5, 0, `- 椋庨櫓绛夌骇: ${String(risk.label || "")} (${Number(risk.score || 0)}/100)`);
    issues.forEach((item) => {
      lines.push(`| ${String(item.level || "")} | ${String(item.kind || "")} | ${String(item.node_id || "")} | ${String(item.message || "").replace(/\|/g, "\\|")} |`);
    });
    return `${lines.join("\n")}\n`;
  }

  function renderTemplateAcceptanceMarkdown(report) {
    const safeReport = report && typeof report === "object" ? report : {};
    const before = safeReport.before && typeof safeReport.before === "object" ? safeReport.before : {};
    const after = safeReport.after && typeof safeReport.after === "object" ? safeReport.after : {};
    const fix = safeReport.auto_fix && typeof safeReport.auto_fix === "object" ? safeReport.auto_fix : {};
    return [
      "# AIWF 妯℃澘楠屾敹鎶ュ憡",
      "",
      `- 鐢熸垚鏃堕棿: ${new Date().toISOString()}`,
      `- 妯℃澘ID: ${String(safeReport.template_id || "-")}`,
      `- 妯℃澘鍚嶇О: ${String(safeReport.template_name || "-")}`,
      `- 楠屾敹缁撹: ${safeReport.accepted ? "閫氳繃" : "鏈€氳繃"}`,
      `- 棰勬鍓? ${before.ok ? "閫氳繃" : "鏈€氳繃"} / 椋庨櫓 ${Number(before?.risk?.score || 0)}/100`,
      `- 棰勬鍚? ${after.ok ? "閫氳繃" : "鏈€氳繃"} / 椋庨櫓 ${Number(after?.risk?.score || 0)}/100`,
      `- 鑷姩淇: 閲嶅杩炵嚎 ${Number(fix.removed_dup_edges || 0)}锛岃嚜鐜?${Number(fix.removed_self_loops || 0)}锛屾柇瑁傝繛绾?${Number(fix.removed_broken_edges || 0)}锛屽绔嬭妭鐐?${Number(fix.removed_isolated_nodes || 0)}`,
      "",
    ].join("\n");
  }

  function applyQualityRuleSetToPayload(payload) {
    const nextPayload = payload && typeof payload === "object" ? { ...payload } : {};
    const workflow = nextPayload.workflow && typeof nextPayload.workflow === "object" ? deepClone(nextPayload.workflow) : null;
    const qualityRuleSetId = String(nextPayload?.quality_rule_set_id || "").trim();
    if (!workflow || !qualityRuleSetId) return nextPayload;
    const hit = listQualityRuleCenter().find((item) => String(item?.id || "") === qualityRuleSetId);
    if (!hit || !hit.rules || typeof hit.rules !== "object") return nextPayload;
    const targetTypes = new Set(["quality_check_v2", "quality_check_v3", "quality_check_v4"]);
    const nodes = Array.isArray(workflow?.nodes) ? workflow.nodes : [];
    nodes.forEach((node) => {
      const type = String(node?.type || "");
      if (!targetTypes.has(type)) return;
      const config = node?.config && typeof node.config === "object" ? node.config : {};
      node.config = {
        ...config,
        rules: deepClone(hit.rules),
        rule_set_meta: {
          id: String(hit.id || ""),
          name: String(hit.name || ""),
          version: String(hit.version || "v1"),
        },
      };
    });
    nextPayload.workflow = workflow;
    return nextPayload;
  }

  function buildRunRegressionAgainstBaseline(runId, baselineId) {
    const current = findRunById(runId);
    if (!current) return { ok: false, error: `run not found: ${runId}` };
    const baseline = listRunBaselines().find((item) => String(item?.baseline_id || "") === String(baselineId || ""));
    if (!baseline) return { ok: false, error: `baseline not found: ${baselineId}` };
    const baselineRunId = String(baseline?.run_id || "");
    const compare = buildRunCompare(baselineRunId, runId);
    if (!compare?.ok) return compare;
    const changed = Array.isArray(compare.node_diff) ? compare.node_diff.filter((item) => item.status_changed || Math.abs(Number(item.seconds_delta || 0)) > 0.001) : [];
    const statusFlip = changed.filter((item) => item.status_changed);
    const perfHot = changed.filter((item) => Number(item.seconds_delta || 0) > 0.5);
    return {
      ok: true,
      baseline_id: String(baseline.baseline_id || ""),
      baseline_name: String(baseline.name || ""),
      baseline_run_id: baselineRunId,
      run_id: runId,
      compare,
      regression: {
        changed_nodes: changed.length,
        status_flip_nodes: statusFlip.length,
        perf_hot_nodes: perfHot.length,
        status_flip: statusFlip,
        perf_hot: perfHot,
      },
    };
  }

  return {
    applyQualityRuleSetToPayload,
    buildRunCompare,
    buildRunRegressionAgainstBaseline,
    renderCompareHtml,
    renderCompareMarkdown,
    renderPreflightMarkdown,
    renderTemplateAcceptanceMarkdown,
  };
}

function registerWorkflowReportIpc(ctx, deps) {
  const {
    ipcMain,
    dialog,
    app,
    fs,
    path,
  } = ctx;
  const {
    isMockIoAllowed,
    resolveMockFilePath,
    nowIso,
    appendAudit,
    findRunById,
    listRunBaselines,
    saveRunBaselines,
    buildRunCompare,
    buildRunRegressionAgainstBaseline,
    renderCompareHtml,
    renderCompareMarkdown,
    renderPreflightMarkdown,
    renderTemplateAcceptanceMarkdown,
  } = deps;

  ipcMain.handle("aiwf:compareWorkflowRuns", async (_evt, req) => {
    try {
      return buildRunCompare(String(req?.run_a || "").trim(), String(req?.run_b || "").trim());
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:listRunBaselines", async () => {
    return { ok: true, items: listRunBaselines() };
  });

  ipcMain.handle("aiwf:saveRunBaseline", async (_evt, req) => {
    try {
      const runId = String(req?.run_id || "").trim();
      if (!runId) return { ok: false, error: "run_id required" };
      const hit = findRunById(runId);
      if (!hit) return { ok: false, error: "run not found" };
      const items = listRunBaselines();
      const baselineId = String(req?.baseline_id || `${Date.now()}_${Math.random().toString(16).slice(2, 10)}`);
      const row = {
        baseline_id: baselineId,
        name: String(req?.name || hit?.workflow_id || "baseline"),
        run_id: runId,
        workflow_id: String(hit?.workflow_id || ""),
        created_at: nowIso(),
        notes: String(req?.notes || ""),
      };
      const index = items.findIndex((item) => String(item?.baseline_id || "") === baselineId);
      if (index >= 0) items[index] = row;
      else items.unshift(row);
      saveRunBaselines(items);
      appendAudit("baseline_save", { baseline_id: baselineId, run_id: runId });
      return { ok: true, item: row };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:compareRunWithBaseline", async (_evt, req) => {
    try {
      return buildRunRegressionAgainstBaseline(String(req?.run_id || "").trim(), String(req?.baseline_id || "").trim());
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:exportCompareReport", async (_evt, req) => {
    try {
      const runA = String(req?.run_a || "").trim();
      const runB = String(req?.run_b || "").trim();
      const format = String(req?.format || "md").trim().toLowerCase() === "html" ? "html" : "md";
      const output = buildRunCompare(runA, runB);
      if (!output?.ok) return output;
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const defaultName = `aiwf_compare_${runA.slice(0, 8)}_${runB.slice(0, 8)}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出运行对比报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "html" ? [{ name: "HTML", extensions: ["html"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, format === "html" ? renderCompareHtml(output) : renderCompareMarkdown(output), "utf8");
      return { ok: true, path: filePath, format };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowPreflightReport", async (_evt, req) => {
    try {
      const report = req?.report && typeof req.report === "object" ? req.report : {};
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const defaultName = `aiwf_preflight_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出预检报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, format === "json" ? `${JSON.stringify(report, null, 2)}\n` : renderPreflightMarkdown(report), "utf8");
      appendAudit("preflight_export", {
        format,
        path: filePath,
        issue_count: Array.isArray(report?.issues) ? report.issues.length : 0,
      });
      return { ok: true, path: filePath, format };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });

  ipcMain.handle("aiwf:exportWorkflowTemplateAcceptanceReport", async (_evt, req) => {
    try {
      const report = req?.report && typeof req.report === "object" ? req.report : {};
      const format = String(req?.format || "md").trim().toLowerCase() === "json" ? "json" : "md";
      const allowMockIo = isMockIoAllowed();
      let filePath = "";
      if (req?.mock && req?.path && allowMockIo) {
        const safe = resolveMockFilePath(req.path);
        if (!safe.ok) return safe;
        filePath = safe.path;
      } else {
        const defaultName = `aiwf_template_acceptance_${Date.now()}.${format}`;
        const pick = await dialog.showSaveDialog({
          title: "导出模板验收报告",
          defaultPath: path.join(app.getPath("documents"), defaultName),
          filters: format === "json" ? [{ name: "JSON", extensions: ["json"] }] : [{ name: "Markdown", extensions: ["md"] }],
          properties: ["createDirectory", "showOverwriteConfirmation"],
        });
        if (pick.canceled || !pick.filePath) return { ok: false, canceled: true };
        filePath = pick.filePath;
      }
      fs.mkdirSync(path.dirname(filePath), { recursive: true });
      fs.writeFileSync(filePath, format === "json" ? `${JSON.stringify(report, null, 2)}\n` : renderTemplateAcceptanceMarkdown(report), "utf8");
      appendAudit("template_acceptance_export", {
        format,
        path: filePath,
        template_id: String(report?.template_id || ""),
        accepted: !!report?.accepted,
      });
      return { ok: true, path: filePath, format };
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  });
}

module.exports = {
  createWorkflowReportSupport,
  registerWorkflowReportIpc,
};
