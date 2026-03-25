const GLUE_PROVIDER = "glue_http";
const WORKFLOW_VERSION_SCHEMA_VERSION = "workflow_version_snapshot.v1";
const GLUE_DEFAULT_URL = "http://127.0.0.1:18081";
const { createGovernanceControlPlaneSupport, GOVERNANCE_CAPABILITIES } = require("./workflow_governance");

function createWorkflowVersionStore(deps = {}) {
  const {
    loadConfig = () => ({}),
    fetchImpl = typeof fetch === "function" ? fetch : null,
    env = process.env,
  } = deps;
  const governance = createGovernanceControlPlaneSupport({ loadConfig, fetchImpl, env, defaultGlueUrl: GLUE_DEFAULT_URL });

  function clone(value) {
    return JSON.parse(JSON.stringify(value));
  }

  function mergedConfig(cfg = null) {
    return { ...loadConfig(), ...(cfg && typeof cfg === "object" ? cfg : {}) };
  }

  function normalizeProvider(value) {
    const raw = String(value || "").trim().toLowerCase();
    if (raw === GLUE_PROVIDER) return GLUE_PROVIDER;
    if (raw === "local_legacy") throw new Error("workflow version local_legacy provider has been retired; use glue_http");
    return "";
  }

  function resolveProvider(cfg = null) {
    const merged = mergedConfig(cfg);
    const explicit = normalizeProvider(merged.workflowVersionProvider || env.AIWF_WORKFLOW_VERSION_PROVIDER);
    if (explicit) return explicit;
    return GLUE_PROVIDER;
  }

  function resolveGlueUrl(cfg = null) {
    return governance.resolveGlueUrl(cfg);
  }

  function headers(apiKey) {
    const out = { "Content-Type": "application/json" };
    const key = String(apiKey || "").trim();
    if (key) out["X-API-Key"] = key;
    return out;
  }

  function normalizeVersionItem(item, provider, existing = null) {
    const source = item && typeof item === "object" ? item : {};
    const current = existing && typeof existing === "object" ? existing : {};
    const versionId = String(source.version_id || current.version_id || "").trim();
    const graph = source.graph !== undefined ? source.graph : current.graph;
    if (!versionId) throw new Error("version_id is required");
    if (!graph || typeof graph !== "object") throw new Error("workflow version graph must be an object");
    if (!Array.isArray(graph.nodes)) throw new Error("workflow version graph requires nodes array");
    if (!Array.isArray(graph.edges)) throw new Error("workflow version graph requires edges array");
    if (!String(graph.workflow_id || "").trim()) throw new Error("workflow version graph requires workflow_id");
    if (!String(graph.version || "").trim()) throw new Error("workflow version graph requires version");
    return {
      schema_version: String(source.schema_version || current.schema_version || WORKFLOW_VERSION_SCHEMA_VERSION),
      provider,
      owner: String(source.owner || current.owner || (provider === GLUE_PROVIDER ? "glue-python" : "desktop.local_legacy")),
      source_of_truth: String(
        source.source_of_truth
          || current.source_of_truth
          || (provider === GLUE_PROVIDER ? "glue-python.governance.workflow_versions" : "desktop.workflow_store.workflow_versions")
      ),
      version_id: versionId,
      ts: String(source.ts || current.ts || new Date().toISOString()),
      workflow_name: String(source.workflow_name || current.workflow_name || graph.name || graph.workflow_id || "").trim(),
      workflow_id: String(source.workflow_id || current.workflow_id || graph.workflow_id || "").trim(),
      path: String(source.path || current.path || "").trim(),
      graph: clone(graph),
    };
  }

  function compareVersionItems(itemA, itemB) {
    const graphA = itemA?.graph && typeof itemA.graph === "object" ? itemA.graph : {};
    const graphB = itemB?.graph && typeof itemB.graph === "object" ? itemB.graph : {};
    const nodesA = Array.isArray(graphA.nodes) ? graphA.nodes : [];
    const nodesB = Array.isArray(graphB.nodes) ? graphB.nodes : [];
    const edgesA = Array.isArray(graphA.edges) ? graphA.edges : [];
    const edgesB = Array.isArray(graphB.edges) ? graphB.edges : [];
    const mapA = new Map(nodesA.map((node) => [String(node?.id || ""), node]));
    const mapB = new Map(nodesB.map((node) => [String(node?.id || ""), node]));
    const nodeIds = Array.from(new Set([...mapA.keys(), ...mapB.keys()])).sort();
    const nodeDiff = nodeIds.map((id) => {
      const nodeA = mapA.get(id);
      const nodeB = mapB.get(id);
      if (!nodeA) return { id, change: "added", type_a: "", type_b: String(nodeB?.type || "") };
      if (!nodeB) return { id, change: "removed", type_a: String(nodeA?.type || ""), type_b: "" };
      const typeChanged = String(nodeA?.type || "") !== String(nodeB?.type || "");
      const configChanged = JSON.stringify(nodeA?.config || {}, Object.keys(nodeA?.config || {}).sort())
        !== JSON.stringify(nodeB?.config || {}, Object.keys(nodeB?.config || {}).sort());
      return {
        id,
        change: typeChanged || configChanged ? "updated" : "same",
        type_a: String(nodeA?.type || ""),
        type_b: String(nodeB?.type || ""),
        type_changed: typeChanged,
        config_changed: configChanged,
      };
    });
    const edgeKey = (edge) => `${String(edge?.from || "")}->${String(edge?.to || "")}:${JSON.stringify(edge?.when ?? null)}`;
    const setA = new Set(edgesA.map(edgeKey));
    const setB = new Set(edgesB.map(edgeKey));
    const addedEdges = Array.from(setB).filter((item) => !setA.has(item));
    const removedEdges = Array.from(setA).filter((item) => !setB.has(item));
    return {
      ok: true,
      provider: "",
      summary: {
        version_a: String(itemA?.version_id || ""),
        version_b: String(itemB?.version_id || ""),
        nodes_a: nodesA.length,
        nodes_b: nodesB.length,
        edges_a: edgesA.length,
        edges_b: edgesB.length,
        changed_nodes: nodeDiff.filter((item) => item.change !== "same").length,
        added_edges: addedEdges.length,
        removed_edges: removedEdges.length,
      },
      node_diff: nodeDiff,
      added_edges: addedEdges,
      removed_edges: removedEdges,
    };
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
    const url = `${resolveGlueUrl(merged)}${route}`;
    const resp = await fetchImpl(url, {
      method,
      headers: headers(merged.apiKey),
      body: body ? JSON.stringify(body) : undefined,
    });
    const payload = await parseResponse(resp);
    if (!resp.ok || payload?.ok === false) {
      throw new Error(String(payload?.error || `workflow version ${method} failed: http ${resp.status}`));
    }
    return payload;
  }

  async function remoteListVersions(limit = 200, workflowName = "", cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_VERSIONS.capability, { cfg });
    const payload = await remoteRequest(
      "GET",
      `${routePrefix}?limit=${Math.max(1, Math.min(5000, Number(limit || 200)))}&workflow_name=${encodeURIComponent(String(workflowName || ""))}`,
      null,
      cfg
    );
    const items = [];
    for (const item of (Array.isArray(payload?.items) ? payload.items : [])) {
      try {
        items.push(normalizeVersionItem(item, GLUE_PROVIDER, item));
      } catch {}
    }
    return { ok: true, provider: GLUE_PROVIDER, items };
  }

  async function remoteGetVersion(versionId, cfg = null) {
    const id = String(versionId || "").trim();
    if (!id) return null;
    try {
      const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_VERSIONS.capability, { cfg });
      const payload = await remoteRequest("GET", `${routePrefix}/${encodeURIComponent(id)}`, null, cfg);
      return payload?.item ? normalizeVersionItem(payload.item, GLUE_PROVIDER, payload.item) : null;
    } catch (error) {
      if (/version not found/i.test(String(error))) return null;
      throw error;
    }
  }

  async function remoteRecordVersion(item, cfg = null) {
    const normalized = normalizeVersionItem(item, GLUE_PROVIDER, item);
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_VERSIONS.capability, { cfg });
    const payload = await remoteRequest(
      "PUT",
      `${routePrefix}/${encodeURIComponent(normalized.version_id)}`,
      {
        version: {
          version_id: normalized.version_id,
          ts: normalized.ts,
          workflow_id: normalized.workflow_id,
          workflow_name: normalized.workflow_name,
          path: normalized.path,
          graph: clone(normalized.graph),
        },
      },
      cfg
    );
    return {
      ok: true,
      provider: GLUE_PROVIDER,
      item: normalizeVersionItem(payload?.item || normalized, GLUE_PROVIDER, payload?.item || normalized),
    };
  }

  async function remoteCompareVersions(versionA, versionB, cfg = null) {
    const routePrefix = await governance.resolveRoutePrefix(GOVERNANCE_CAPABILITIES.WORKFLOW_VERSIONS.capability, { cfg });
    const payload = await remoteRequest(
      "POST",
      `${routePrefix}/compare`,
      { version_a: String(versionA || "").trim(), version_b: String(versionB || "").trim() },
      cfg
    );
    return payload;
  }

  async function recordVersion(item, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteRecordVersion(item, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function listVersions(limit = 200, workflowName = "", cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteListVersions(limit, workflowName, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  async function getVersion(versionId, cfg = null) {
    _ = resolveProvider(cfg);
    return remoteGetVersion(versionId, cfg);
  }

  async function compareVersions(versionA, versionB, cfg = null) {
    try {
      _ = resolveProvider(cfg);
      return await remoteCompareVersions(versionA, versionB, cfg);
    } catch (error) {
      return { ok: false, error: String(error) };
    }
  }

  return {
    compareVersions,
    getVersion,
    listVersions,
    recordVersion,
    resolveGlueUrl,
    resolveProvider,
  };
}

module.exports = {
  GLUE_PROVIDER,
  WORKFLOW_VERSION_SCHEMA_VERSION,
  createWorkflowVersionStore,
};
