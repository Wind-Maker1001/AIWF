const BASE_PROVIDER = "base_http";
const LOCAL_PROVIDER = "local_runtime";
const {
  createWorkflowStoreRemoteError,
  workflowStoreRemoteErrorResult,
} = require("./workflow_store_remote_error");
const WORKFLOW_RUN_ENTRY_SCHEMA_VERSION = "workflow_run_entry.v1";
const WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION = "workflow_audit_event.v1";
const WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION = "workflow_run_timeline.v1";
const WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION = "workflow_failure_summary.v1";

function createWorkflowRunAuditStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    fs,
    runHistoryPath,
    workflowAuditPath,
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function normalizeRunItem(item, provider = LOCAL_PROVIDER) {
    const source = item && typeof item === "object" ? item : {};
    const runId = String(source.run_id || "").trim();
    if (!runId) throw new Error("run_id is required");
    const local = provider === LOCAL_PROVIDER;
    return {
      schema_version: String(source.schema_version || WORKFLOW_RUN_ENTRY_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || (local ? "dify-desktop" : "base-java")),
      source_of_truth: String(source.source_of_truth || (local ? "desktop.workflow_runtime.run_history" : "base-java.jobs")),
      run_id: runId,
      ts: String(source.ts || new Date().toISOString()),
      workflow_id: String(source.workflow_id || "").trim(),
      status: String(source.status || "").trim(),
      ok: !!source.ok,
      run_request_kind: String(source.run_request_kind || "").trim(),
      version_id: String(source.version_id || "").trim(),
      published_version_id: String(source.published_version_id || "").trim(),
      workflow_definition_source: String(source.workflow_definition_source || "").trim(),
      payload: clone(source.payload && typeof source.payload === "object" ? source.payload : {}),
      config: clone(source.config && typeof source.config === "object" ? source.config : {}),
      result: clone(source.result && typeof source.result === "object" ? source.result : source),
    };
  }

  function normalizeAuditEvent(item, provider = LOCAL_PROVIDER) {
    const source = item && typeof item === "object" ? item : {};
    const local = provider === LOCAL_PROVIDER;
    return {
      schema_version: String(source.schema_version || WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || (local ? "dify-desktop" : "base-java")),
      source_of_truth: String(source.source_of_truth || (local ? "desktop.workflow_runtime.audit_log" : "base-java.jobs")),
      ts: String(source.ts || new Date().toISOString()),
      action: String(source.action || "unknown").trim() || "unknown",
      detail: clone(source.detail && typeof source.detail === "object" ? source.detail : {}),
    };
  }

  function normalizeTimeline(payload, provider = LOCAL_PROVIDER) {
    const source = payload && typeof payload === "object" ? payload : {};
    const local = provider === LOCAL_PROVIDER;
    return {
      schema_version: String(source.schema_version || WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION),
      ok: source.ok !== false,
      provider: String(source.provider || provider),
      owner: String(source.owner || (local ? "dify-desktop" : "base-java")),
      source_of_truth: String(source.source_of_truth || (local ? "desktop.workflow_runtime.run_history" : "base-java.jobs")),
      run_id: String(source.run_id || "").trim(),
      status: String(source.status || "").trim(),
      timeline: Array.isArray(source.timeline) ? clone(source.timeline) : [],
    };
  }

  function normalizeFailureSummary(payload, provider = LOCAL_PROVIDER) {
    const source = payload && typeof payload === "object" ? payload : {};
    const local = provider === LOCAL_PROVIDER;
    return {
      schema_version: String(source.schema_version || WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION),
      ok: source.ok !== false,
      provider: String(source.provider || provider),
      owner: String(source.owner || (local ? "dify-desktop" : "base-java")),
      source_of_truth: String(source.source_of_truth || (local ? "desktop.workflow_runtime.run_history" : "base-java.jobs")),
      total_runs: Number(source.total_runs || 0),
      failed_runs: Number(source.failed_runs || 0),
      by_node: clone(source.by_node && typeof source.by_node === "object" ? source.by_node : {}),
    };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (!raw) return "";
    if (raw === "glue_http") {
      throw new Error("workflow run audit provider unsupported: glue_http; use base_http for remote lifecycle records");
    }
    if (raw === BASE_PROVIDER) return BASE_PROVIDER;
    if (raw === LOCAL_PROVIDER) return LOCAL_PROVIDER;
    throw new Error(`workflow run audit provider unsupported: ${raw}`);
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(
      String(merged.workflowRunAuditProvider || "")
    );
    if (explicit) return explicit;
    return LOCAL_PROVIDER;
  }

  function safeReadJsonl(filePath, limit = 200) {
    try {
      const target = typeof filePath === "function" ? filePath() : "";
      if (!target || !fs || !fs.existsSync(target)) return [];
      const lines = fs.readFileSync(target, "utf8").split(/\r?\n/).filter((item) => item.trim());
      return lines
        .slice(Math.max(0, lines.length - Math.max(1, Math.min(5000, Number(limit || 200)))))
        .map((item) => {
          try { return JSON.parse(item); } catch { return null; }
        })
        .filter(Boolean)
        .reverse();
    } catch {
      return [];
    }
  }

  function listLocalRuns(limit = 200) {
    return safeReadJsonl(runHistoryPath, limit)
      .map((item) => {
        try { return normalizeRunItem(item, LOCAL_PROVIDER); } catch { return null; }
      })
      .filter(Boolean);
  }

  function getLocalRun(runId) {
    const target = String(runId || "").trim();
    if (!target) return null;
    return listLocalRuns(5000).find((item) => String(item?.run_id || "") === target) || null;
  }

  function listLocalAuditEvents(limit = 200, action = "") {
    const targetAction = String(action || "").trim();
    return safeReadJsonl(workflowAuditPath, limit)
      .filter((item) => !targetAction || String(item?.action || "").trim() === targetAction)
      .map((item) => normalizeAuditEvent(item, LOCAL_PROVIDER));
  }

  function resolveBaseUrl(cfg = null) {
    const merged = mergedConfig(cfg);
    return String(merged.baseUrl || env.AIWF_BASE_URL || "http://127.0.0.1:18080").replace(/\/$/, "");
  }

  async function parseBaseResponse(resp) {
    const text = await resp.text();
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch {
      return { ok: false, error: text };
    }
  }

  async function baseRequest(method, route, body = null, cfg = null) {
    if (typeof fetchImpl !== "function") throw new Error("fetch is not available for base provider");
    const merged = mergedConfig(cfg);
    const headers = { "Content-Type": "application/json" };
    const apiKey = String(merged.apiKey || "").trim();
    if (apiKey) headers["X-API-Key"] = apiKey;
    const url = `${resolveBaseUrl(merged)}${route}`;
    const resp = await fetchImpl(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await parseBaseResponse(resp);
    if (!resp.ok || payload?.ok === false) {
      throw createWorkflowStoreRemoteError(payload, `workflow run audit ${method} failed: http ${resp.status}`);
    }
    return payload;
  }

  async function mirrorRun(run, payload, config, cfg = null) {
    const provider = resolveProvider(cfg || config);
    if (provider === BASE_PROVIDER) return { ok: true, provider };
    return { ok: true, provider };
  }

  async function mirrorAudit(action, detail, cfg = null) {
    const provider = resolveProvider(cfg);
    if (provider === BASE_PROVIDER) return { ok: true, provider };
    return { ok: true, provider };
  }

  async function listRuns(limit = 200, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      if (provider === LOCAL_PROVIDER) {
        return { ok: true, provider, items: listLocalRuns(limit) };
      }
      if (provider === BASE_PROVIDER) {
        const payload = await baseRequest("GET", `/api/v1/jobs/history?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}`, null, cfg);
        const items = [];
        for (const item of (Array.isArray(payload) ? payload : [])) {
          try {
            items.push(normalizeRunItem(item, BASE_PROVIDER));
          } catch {}
        }
        return { ok: true, provider, items };
      }
      return { ok: false, error: `unsupported workflow run audit provider: ${provider}` };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function getRun(runId, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      if (provider === LOCAL_PROVIDER) {
        return getLocalRun(runId);
      }
      if (provider === BASE_PROVIDER) {
        const payload = await baseRequest("GET", `/api/v1/jobs/${encodeURIComponent(String(runId || "").trim())}/record`, null, cfg);
        return payload ? normalizeRunItem(payload, BASE_PROVIDER) : null;
      }
      throw new Error(`unsupported workflow run audit provider: ${provider}`);
    } catch (error) {
      if (/run not found/i.test(String(error))) return null;
      throw error;
    }
  }

  async function getRunTimeline(runId, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      if (provider === LOCAL_PROVIDER) {
        const found = getLocalRun(runId);
        if (!found) return { ok: false, error: "run not found" };
        const rows = Array.isArray(found?.result?.node_runs) ? found.result.node_runs : [];
        return normalizeTimeline({
          ok: true,
          run_id: String(runId || "").trim(),
          status: String(found?.result?.status || found?.status || "").trim(),
          timeline: rows.map((node) => ({
            node_id: String(node?.id || ""),
            type: String(node?.type || ""),
            status: String(node?.status || ""),
            started_at: String(node?.started_at || ""),
            ended_at: String(node?.ended_at || ""),
            seconds: Number(node?.seconds || 0),
          })).sort((a, b) => String(a.started_at || "").localeCompare(String(b.started_at || ""))),
        }, LOCAL_PROVIDER);
      }
      if (provider === BASE_PROVIDER) {
        const payload = await baseRequest("GET", `/api/v1/jobs/${encodeURIComponent(String(runId || "").trim())}/timeline`, null, cfg);
        return normalizeTimeline(payload, BASE_PROVIDER);
      }
      return { ok: false, error: `unsupported workflow run audit provider: ${provider}` };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function getFailureSummary(limit = 400, cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      if (provider === LOCAL_PROVIDER) {
        const runs = listLocalRuns(limit);
        const failedRuns = runs.filter((item) => item?.ok === false || item?.result?.ok === false);
        const byNode = {};
        failedRuns.forEach((run) => {
          const nodeRuns = Array.isArray(run?.result?.node_runs) ? run.result.node_runs : [];
          nodeRuns
            .filter((node) => String(node?.status || "") === "failed")
            .forEach((node) => {
              const key = String(node?.type || "unknown");
              if (!byNode[key]) byNode[key] = { failed: 0, samples: [] };
              byNode[key].failed += 1;
              if (byNode[key].samples.length < 3) byNode[key].samples.push(String(node?.error || "").slice(0, 200));
            });
        });
        return normalizeFailureSummary({
          ok: true,
          total_runs: runs.length,
          failed_runs: failedRuns.length,
          by_node: byNode,
        }, LOCAL_PROVIDER);
      }
      if (provider === BASE_PROVIDER) {
        const payload = await baseRequest("GET", `/api/v1/jobs/failure-summary?limit=${Math.max(1, Math.min(5000, Number(limit || 400)))}`, null, cfg);
        return normalizeFailureSummary(payload, BASE_PROVIDER);
      }
      return { ok: false, error: `unsupported workflow run audit provider: ${provider}` };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  async function listAuditLogs(limit = 200, action = "", cfg = null) {
    try {
      const provider = resolveProvider(cfg);
      if (provider === LOCAL_PROVIDER) {
        return {
          ok: true,
          provider: LOCAL_PROVIDER,
          owner: "dify-desktop",
          source_of_truth: "desktop.workflow_runtime.audit_log",
          items: listLocalAuditEvents(limit, action),
        };
      }
      if (provider === BASE_PROVIDER) {
        const payload = await baseRequest(
          "GET",
          `/api/v1/jobs/audit-events?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}&action=${encodeURIComponent(String(action || "").trim())}`,
          null,
          cfg,
        );
        const items = [];
        for (const item of (Array.isArray(payload) ? payload : [])) {
          items.push(normalizeAuditEvent(item, BASE_PROVIDER));
        }
        return {
          ok: true,
          provider: BASE_PROVIDER,
          owner: "base-java",
          source_of_truth: "base-java.jobs",
          items,
        };
      }
      return { ok: false, error: `unsupported workflow run audit provider: ${provider}` };
    } catch (error) {
      return workflowStoreRemoteErrorResult(error);
    }
  }

  return {
    getFailureSummary,
    getRun,
    getRunTimeline,
    listAuditLogs,
    listRuns,
    mirrorAudit,
    mirrorRun,
    resolveBaseUrl,
    resolveProvider,
  };
}

module.exports = {
  BASE_PROVIDER,
  LOCAL_PROVIDER,
  WORKFLOW_AUDIT_EVENT_SCHEMA_VERSION,
  WORKFLOW_FAILURE_SUMMARY_SCHEMA_VERSION,
  WORKFLOW_RUN_ENTRY_SCHEMA_VERSION,
  WORKFLOW_RUN_TIMELINE_SCHEMA_VERSION,
  createWorkflowRunAuditStore,
};
