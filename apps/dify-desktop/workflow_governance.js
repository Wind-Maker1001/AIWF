const path = require("path");
const { createWorkflowStoreRemoteError } = require("./workflow_store_remote_error");
const {
  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,
  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,
  GOVERNANCE_CAPABILITY_ITEMS,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
} = require("./workflow_governance_capabilities.generated.js");
const GOVERNANCE_CONTROL_PLANE_META_ROUTE = "/governance/meta/control-plane";
const GOVERNANCE_DEFAULT_GLUE_URL = "http://127.0.0.1:18081";

function deepClone(v) {
  return JSON.parse(JSON.stringify(v));
}

function normalizeBaseUrl(url) {
  return String(url || "").trim().replace(/\/$/, "");
}

function resolveGovernanceOwnedRoutePrefix(surface = {}, preferredOwnedPrefix = "") {
  const preferred = String(preferredOwnedPrefix || "").trim();
  const owned = Array.isArray(surface?.owned_route_prefixes) ? surface.owned_route_prefixes : [];
  if (preferred) {
    const direct = owned.find((item) => String(item || "").trim() === preferred);
    if (direct) return String(direct).trim();
    const preferredLeaf = preferred.replace(/^\/+|\/+$/g, "").split("/").pop() || "";
    if (preferredLeaf) {
      const fuzzy = owned.find((item) => {
        const candidateLeaf = String(item || "").trim().replace(/^\/+|\/+$/g, "").split("/").pop() || "";
        return candidateLeaf.startsWith(preferredLeaf);
      });
      if (fuzzy) return String(fuzzy).trim();
    }
  }
  return String(surface?.route_prefix || "").trim();
}

function createGovernanceControlPlaneSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
    defaultGlueUrl = GOVERNANCE_DEFAULT_GLUE_URL,
  } = deps;

  const boundaryCache = new Map();

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function resolveGlueUrl(cfg = null) {
    const merged = mergedConfig(cfg);
    return normalizeBaseUrl(merged.glueUrl || env.AIWF_GLUE_URL || defaultGlueUrl);
  }

  function headers(apiKey) {
    const out = { "Content-Type": "application/json" };
    const key = String(apiKey || "").trim();
    if (key) out["X-API-Key"] = key;
    return out;
  }

  async function parseResponse(resp) {
    const text = await resp.text();
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch {
      return { ok: false, error: text };
    }
  }

  async function fetchBoundary(cfg = null) {
    if (typeof fetchImpl !== "function") throw new Error("fetch is not available for governance control plane support");
    const merged = mergedConfig(cfg);
    const baseUrl = resolveGlueUrl(merged);
    if (boundaryCache.has(baseUrl)) {
      return boundaryCache.get(baseUrl);
    }
    const resp = await fetchImpl(`${baseUrl}${GOVERNANCE_CONTROL_PLANE_META_ROUTE}`, {
      method: "GET",
      headers: headers(merged.apiKey),
    });
    const payload = await parseResponse(resp);
    if (!resp.ok || payload?.ok === false || !payload?.boundary || typeof payload.boundary !== "object") {
      throw new Error(String(payload?.error || `governance control plane boundary failed: http ${resp.status}`));
    }
    boundaryCache.set(baseUrl, payload.boundary);
    return payload.boundary;
  }

  async function resolveRoutePrefix(capability, options = {}) {
    const normalizedCapability = String(capability || "").trim();
    if (!normalizedCapability) throw new Error("governance capability is required");
    const boundary = await fetchBoundary(options?.cfg || null);
    const surfaces = Array.isArray(boundary?.governance_surfaces) ? boundary.governance_surfaces : [];
    const surface = surfaces.find((item) => String(item?.capability || "").trim() === normalizedCapability);
    if (!surface) {
      throw new Error(`governance boundary missing capability: ${normalizedCapability}`);
    }
    const routePrefix = resolveGovernanceOwnedRoutePrefix(surface, options?.preferredOwnedPrefix || "");
    if (!routePrefix) {
      throw new Error(`governance boundary route prefix missing for capability: ${normalizedCapability}`);
    }
    return routePrefix;
  }

  function clearBoundaryCache() {
    boundaryCache.clear();
  }

  return {
    clearBoundaryCache,
    fetchBoundary,
    resolveGlueUrl,
    resolveRoutePrefix,
  };
}

function createGovernanceGlueStoreSupport(deps = {}) {
  const {
    loadConfig = () => ({}),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
    defaultGlueUrl = GOVERNANCE_DEFAULT_GLUE_URL,
    providerConfigKey = "",
    providerEnvKey = "",
    providerLabel = "workflow store",
  } = deps;
  const governance = createGovernanceControlPlaneSupport({ loadConfig, fetchImpl, env, defaultGlueUrl });

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (!raw) return "";
    if (raw === "glue_http") return "glue_http";
    throw new Error(`${providerLabel} provider unsupported: ${raw}; use glue_http`);
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(
      (providerConfigKey ? merged[providerConfigKey] : "")
      || (providerEnvKey ? env[providerEnvKey] : "")
    );
    if (explicit) return explicit;
    return "glue_http";
  }

  async function parseResponse(resp) {
    const text = await resp.text();
    if (!text) return {};
    try {
      return JSON.parse(text);
    } catch {
      return { ok: false, error: text };
    }
  }

  async function remoteRequest(method, route, body = null, cfg = null) {
    if (typeof fetchImpl !== "function") throw new Error("fetch is not available for glue provider");
    const merged = mergedConfig(cfg);
    const headers = { "Content-Type": "application/json" };
    const apiKey = String(merged.apiKey || "").trim();
    if (apiKey) headers["X-API-Key"] = apiKey;
    const url = `${governance.resolveGlueUrl(merged)}${route}`;
    const resp = await fetchImpl(url, {
      method,
      headers,
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await parseResponse(resp);
    if (!resp.ok || payload?.ok === false) {
      throw createWorkflowStoreRemoteError(payload, `${providerLabel} ${method} failed: http ${resp.status}`);
    }
    return payload;
  }

  return {
    governance,
    mergedConfig,
    remoteRequest,
    resolveGlueUrl: governance.resolveGlueUrl,
    resolveProvider,
  };
}

function defaultGovernanceProfile() {
  return {
    version: 1,
    roles: {
      owner: { allow: ["*"] },
      analyst: { allow: ["ingest_files", "clean_md", "compute_rust", "load_rows_v2", "load_rows_v3", "transform_rows_v3", "postprocess_rows_v1", "join_rows_v2", "join_rows_v3", "join_rows_v4", "aggregate_rows_v2", "aggregate_rows_v3", "aggregate_rows_v4", "quality_check_v2", "quality_check_v3", "quality_check_v4", "sql_chart_v1", "office_slot_fill_v1", "md_output"] },
      reviewer: { allow: ["manual_review", "md_output", "ai_audit"] },
    },
    ai_budget: {
      enabled: true,
      max_calls_per_run: 2,
      max_estimated_tokens_per_run: 120000,
      max_estimated_cost_usd_per_run: 0.8,
      token_price_usd_per_1k: 0.002,
    },
    sla: {
      enabled: true,
      max_workflow_seconds: 900,
      max_node_seconds: 300,
    },
    dictionary: {
      required_fields: [],
    },
  };
}

function mergeGovernanceProfile(payload = {}, config = {}) {
  const base = defaultGovernanceProfile();
  const fromCfg = config?.governance && typeof config.governance === "object" ? config.governance : {};
  const fromPayload = payload?.governance && typeof payload.governance === "object" ? payload.governance : {};
  return {
    ...base,
    ...fromCfg,
    ...fromPayload,
    roles: {
      ...(base.roles || {}),
      ...(fromCfg.roles || {}),
      ...(fromPayload.roles || {}),
    },
    ai_budget: {
      ...(base.ai_budget || {}),
      ...(fromCfg.ai_budget || {}),
      ...(fromPayload.ai_budget || {}),
    },
    sla: {
      ...(base.sla || {}),
      ...(fromCfg.sla || {}),
      ...(fromPayload.sla || {}),
    },
    dictionary: {
      ...(base.dictionary || {}),
      ...(fromCfg.dictionary || {}),
      ...(fromPayload.dictionary || {}),
    },
  };
}

function isNodeAllowedByRole(nodeType, roleCfg) {
  const allow = Array.isArray(roleCfg?.allow) ? roleCfg.allow.map((x) => String(x || "").trim()) : [];
  const deny = Array.isArray(roleCfg?.deny) ? roleCfg.deny.map((x) => String(x || "").trim()) : [];
  if (allow.includes("*")) {
    if (deny.includes("*")) return false;
    return !deny.includes(String(nodeType || ""));
  }
  return allow.includes(String(nodeType || "")) && !deny.includes(String(nodeType || ""));
}

function authorizeGraph(graph = {}, actorRole = "owner", profile = defaultGovernanceProfile()) {
  const role = String(actorRole || "owner");
  const roleCfg = profile?.roles?.[role];
  if (!roleCfg) {
    return { ok: false, role, error: `unknown_role:${role}`, denied_nodes: [] };
  }
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const denied = nodes
    .filter((n) => !isNodeAllowedByRole(String(n?.type || ""), roleCfg))
    .map((n) => ({ id: String(n?.id || ""), type: String(n?.type || "") }));
  return {
    ok: denied.length === 0,
    role,
    denied_nodes: denied,
  };
}

function extOfFile(p) {
  const s = String(p || "").trim();
  if (!s) return "";
  return String(path.extname(s) || "").toLowerCase();
}

function classifyInputFiles(files = []) {
  const list = Array.isArray(files) ? files : [];
  const dataExt = new Set([".csv", ".tsv", ".xlsx", ".xls", ".json", ".jsonl", ".parquet", ".feather", ".orc", ".db", ".sqlite", ".sql"]);
  const docExt = new Set([".pdf", ".docx", ".doc", ".txt", ".md", ".rtf", ".pptx", ".ppt"]);
  const imageExt = new Set([".png", ".jpg", ".jpeg", ".bmp", ".gif", ".tif", ".tiff", ".webp"]);
  const out = {
    total: list.length,
    data_files: 0,
    doc_files: 0,
    image_files: 0,
    unknown_files: 0,
  };
  for (const f of list) {
    const ext = extOfFile(f);
    if (dataExt.has(ext)) out.data_files += 1;
    else if (docExt.has(ext)) out.doc_files += 1;
    else if (imageExt.has(ext)) out.image_files += 1;
    else out.unknown_files += 1;
  }
  return out;
}

function buildLineageSummary(nodeOutputs = {}) {
  const edges = [];
  const nodesSeen = new Set();
  const values = nodeOutputs && typeof nodeOutputs === "object" ? Object.values(nodeOutputs) : [];
  for (const v of values) {
    const detail = v?.detail && typeof v.detail === "object" ? v.detail : v;
    const candEdges = Array.isArray(detail?.edges) ? detail.edges : (Array.isArray(detail?.lineage_edges) ? detail.lineage_edges : []);
    const candNodes = Array.isArray(detail?.nodes) ? detail.nodes : (Array.isArray(detail?.lineage_nodes) ? detail.lineage_nodes : []);
    for (const n of candNodes) nodesSeen.add(String(n?.id || n || "").trim());
    for (const e of candEdges) {
      const from = String(e?.from || e?.src || "").trim();
      const to = String(e?.to || e?.dst || "").trim();
      if (!from || !to) continue;
      edges.push({ from, to, kind: String(e?.kind || e?.type || "field") });
      nodesSeen.add(from);
      nodesSeen.add(to);
    }
  }
  return {
    nodes: Array.from(nodesSeen).filter(Boolean),
    edges,
    edge_count: edges.length,
    node_count: nodesSeen.size,
  };
}

function evaluateSla(nodeRuns = [], profile = defaultGovernanceProfile()) {
  const sla = profile?.sla && typeof profile.sla === "object" ? profile.sla : {};
  if (sla.enabled === false) return { enabled: false, passed: true, reasons: [] };
  const maxWorkflowSeconds = Number.isFinite(Number(sla.max_workflow_seconds)) ? Number(sla.max_workflow_seconds) : 900;
  const maxNodeSeconds = Number.isFinite(Number(sla.max_node_seconds)) ? Number(sla.max_node_seconds) : 300;
  const totalSeconds = (Array.isArray(nodeRuns) ? nodeRuns : []).reduce((acc, n) => acc + Number(n?.seconds || 0), 0);
  const slowNodes = (Array.isArray(nodeRuns) ? nodeRuns : [])
    .filter((n) => Number(n?.seconds || 0) > maxNodeSeconds)
    .map((n) => ({ id: String(n?.id || ""), type: String(n?.type || ""), seconds: Number(n?.seconds || 0) }));
  const reasons = [];
  if (totalSeconds > maxWorkflowSeconds) reasons.push(`workflow_timeout:${totalSeconds.toFixed(3)}>${maxWorkflowSeconds}`);
  if (slowNodes.length) reasons.push(`node_timeout:${slowNodes.map((x) => `${x.id}:${x.seconds}`).join(",")}`);
  return {
    enabled: true,
    passed: reasons.length === 0,
    reasons,
    total_seconds: Number(totalSeconds.toFixed(3)),
    max_workflow_seconds: maxWorkflowSeconds,
    max_node_seconds: maxNodeSeconds,
    slow_nodes: slowNodes,
  };
}

function initAiBudgetState(profile = defaultGovernanceProfile()) {
  const b = profile?.ai_budget && typeof profile.ai_budget === "object" ? profile.ai_budget : {};
  return {
    enabled: b.enabled !== false,
    calls: 0,
    estimated_tokens: 0,
    estimated_cost_usd: 0,
    max_calls_per_run: Number.isFinite(Number(b.max_calls_per_run)) ? Math.max(1, Math.floor(Number(b.max_calls_per_run))) : 2,
    max_estimated_tokens_per_run: Number.isFinite(Number(b.max_estimated_tokens_per_run)) ? Math.max(1000, Math.floor(Number(b.max_estimated_tokens_per_run))) : 120000,
    max_estimated_cost_usd_per_run: Number.isFinite(Number(b.max_estimated_cost_usd_per_run)) ? Math.max(0, Number(b.max_estimated_cost_usd_per_run)) : 0.8,
    token_price_usd_per_1k: Number.isFinite(Number(b.token_price_usd_per_1k)) ? Math.max(0, Number(b.token_price_usd_per_1k)) : 0.002,
  };
}

module.exports = {
  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,
  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,
  GOVERNANCE_CAPABILITY_ITEMS,
  GOVERNANCE_CAPABILITIES,
  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,
  GOVERNANCE_CONTROL_PLANE_META_ROUTE,
  GOVERNANCE_DEFAULT_GLUE_URL,
  defaultGovernanceProfile,
  mergeGovernanceProfile,
  authorizeGraph,
  classifyInputFiles,
  buildLineageSummary,
  evaluateSla,
  initAiBudgetState,
  createGovernanceControlPlaneSupport,
  createGovernanceGlueStoreSupport,
  resolveGovernanceOwnedRoutePrefix,
  deepClone,
};
