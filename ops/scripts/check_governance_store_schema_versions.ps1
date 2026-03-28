param(
  [string]$RepoRoot = "",
  [string[]]$RequireRuntimeOutputs = @()
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}

$requiredOutputsJson = ConvertTo-Json -InputObject @($RequireRuntimeOutputs) -Compress

$nodeScript = @'
const fs = require("fs");
const path = require("path");

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).filter(Boolean))).sort();
}

function failPayload(payload) {
  console.log(JSON.stringify(payload));
  process.exit(1);
}

(async () => {
  const repoRoot = process.argv[2];
  const requiredRuntimeOutputs = JSON.parse(process.env.AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON || "[]");

  const modules = [
    { name: "workflow_app_registry_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_app_registry_store.js") },
    { name: "workflow_quality_rule_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_quality_rule_store.js") },
    { name: "workflow_manual_review_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_manual_review_store.js") },
    { name: "workflow_version_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_version_store.js") },
    { name: "workflow_run_baseline_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_run_baseline_store.js") },
    { name: "workflow_sandbox_rule_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_sandbox_rule_store.js") },
    { name: "workflow_sandbox_autofix_store", file: path.join(repoRoot, "apps", "dify-desktop", "workflow_sandbox_autofix_store.js") },
  ];

  const missingSourceSchemaVersionModules = modules.filter((entry) => {
    const text = fs.readFileSync(entry.file, "utf8");
    return !text.includes("schema_version");
  }).map((entry) => entry.name);

  const {
    createWorkflowAppRegistryStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_app_registry_store.js"));
  const {
    createWorkflowQualityRuleSetSupport,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_quality_rule_store.js"));
  const {
    createWorkflowManualReviewStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_manual_review_store.js"));
  const {
    createWorkflowVersionStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_version_store.js"));
  const {
    createWorkflowRunBaselineStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_run_baseline_store.js"));
  const {
    createWorkflowSandboxRuleStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_sandbox_rule_store.js"));
  const {
    createWorkflowSandboxAutoFixStore,
  } = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_sandbox_autofix_store.js"));

  function jsonResponse(status, payload) {
    return {
      ok: status >= 200 && status < 300,
      status,
      async text() {
        return JSON.stringify(payload);
      },
    };
  }

  function governanceBoundaryResponse(capability, routePrefix, ownedRoutePrefixes = [routePrefix]) {
    return jsonResponse(200, {
      ok: true,
      boundary: {
        governance_surfaces: [
          {
            capability,
            route_prefix: routePrefix,
            owned_route_prefixes: ownedRoutePrefixes,
          },
        ],
      },
    });
  }

  function graph() {
    return {
      workflow_id: "wf_schema_gate",
      version: "1.0.0",
      name: "Schema Gate",
      nodes: [{ id: "n1", type: "ingest_files", config: {} }],
      edges: [],
    };
  }

  const runtimeCoveredOutputs = [];
  const missingRuntimeSchemaVersionOutputs = [];
  function recordOutput(name, ok) {
    if (ok) runtimeCoveredOutputs.push(name);
    else missingRuntimeSchemaVersionOutputs.push(name);
  }

  const appStore = createWorkflowAppRegistryStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    validateWorkflowGraph: () => {},
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_apps", "/governance/workflow-apps");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/workflow-apps?")) {
        return jsonResponse(200, {
          ok: true,
          items: [{
            app_id: "app_schema_gate",
            schema_version: "workflow_app_registry_entry.v1",
            workflow_id: "wf_schema_gate",
            name: "Schema Gate App",
            params_schema: {},
            template_policy: {},
            graph: graph(),
          }],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected workflow app request: ${method} ${url}` });
    },
  });
  const apps = await appStore.listApps(20, { mode: "base_api" });
  recordOutput("workflow_app_registry_item", !!apps?.items?.[0]?.schema_version);

  const qualityStore = createWorkflowQualityRuleSetSupport({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("quality_rule_sets", "/governance/quality-rule-sets");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/quality-rule-sets?")) {
        return jsonResponse(200, {
          ok: true,
          sets: [{
            id: "finance_strict_v1",
            schema_version: "quality_rule_set.v1",
            name: "Finance Strict",
            version: "v1",
            scope: "workflow",
            rules: {},
          }],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected quality rule request: ${method} ${url}` });
    },
  });
  const quality = await qualityStore.listQualityRuleSets({ mode: "base_api" });
  recordOutput("workflow_quality_rule_set", !!quality?.sets?.[0]?.schema_version);

  const manualReviewStore = createWorkflowManualReviewStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("manual_reviews", "/governance/manual-reviews");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/manual-reviews?limit=")) {
        return jsonResponse(200, {
          ok: true,
          items: [{
            run_id: "run_schema_gate",
            review_key: "gate_a",
            schema_version: "manual_review_item.v1",
            node_id: "n1",
            status: "pending",
          }],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected manual review request: ${method} ${url}` });
    },
  });
  const reviewQueue = await manualReviewStore.listQueue(20, { mode: "base_api" });
  recordOutput("workflow_manual_review_item", !!reviewQueue?.items?.[0]?.schema_version);

  const versionStore = createWorkflowVersionStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("workflow_versions", "/governance/workflow-versions");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/workflow-versions?")) {
        return jsonResponse(200, {
          ok: true,
          items: [{
            version_id: "ver_schema_gate",
            schema_version: "workflow_version_snapshot.v1",
            workflow_id: "wf_schema_gate",
            workflow_name: "Schema Gate",
            graph: graph(),
          }],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected workflow version request: ${method} ${url}` });
    },
  });
  const versions = await versionStore.listVersions(20, "", { mode: "base_api" });
  recordOutput("workflow_version_snapshot", !!versions?.items?.[0]?.schema_version);

  const baselineStore = createWorkflowRunBaselineStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse("run_baselines", "/governance/run-baselines");
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.includes("/governance/run-baselines?")) {
        return jsonResponse(200, {
          ok: true,
          items: [{
            baseline_id: "baseline_schema_gate",
            schema_version: "run_baseline_entry.v1",
            run_id: "run_schema_gate",
            workflow_id: "wf_schema_gate",
            name: "Baseline",
          }],
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected run baseline request: ${method} ${url}` });
    },
  });
  const baselines = await baselineStore.list(20, { mode: "base_api" });
  recordOutput("workflow_run_baseline_entry", !!baselines?.items?.[0]?.schema_version);

  const sandboxSupport = {
    normalizeSandboxAlertRules(input) {
      const source = input && typeof input === "object" ? input : {};
      return {
        whitelist_codes: Array.isArray(source.whitelist_codes) ? source.whitelist_codes : [],
        whitelist_node_types: Array.isArray(source.whitelist_node_types) ? source.whitelist_node_types : [],
        whitelist_keys: Array.isArray(source.whitelist_keys) ? source.whitelist_keys : [],
        mute_until_by_key: source.mute_until_by_key && typeof source.mute_until_by_key === "object" ? source.mute_until_by_key : {},
      };
    },
    applySandboxAutoFixPayload(payload, state) {
      return { ...(payload || {}), forced_isolation_mode: String(state?.forced_isolation_mode || "") };
    },
    async maybeApplySandboxAutoFix(_run, _payload, options = null) {
      const nextState = options?.state || {};
      if (typeof options?.persistState === "function") {
        await options.persistState(nextState);
      }
      return { ok: true };
    },
  };

  const sandboxRuleStore = createWorkflowSandboxRuleStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    sandboxSupport,
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse(
          "workflow_sandbox_rules",
          "/governance/workflow-sandbox/rules",
          ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"]
        );
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.endsWith("/governance/workflow-sandbox/rules")) {
        return jsonResponse(200, {
          ok: true,
          schema_version: "workflow_sandbox_rules.v1",
          rules: {
            whitelist_codes: ["sandbox_limit_exceeded:output"],
            whitelist_node_types: ["ai_refine"],
            whitelist_keys: [],
            mute_until_by_key: {},
          },
        });
      }
      if (method === "GET" && url.includes("/governance/workflow-sandbox/rule-versions?")) {
        return jsonResponse(200, {
          ok: true,
          items: [
            {
              version_id: "ver_a",
              schema_version: "workflow_sandbox_rule_version.v1",
              ts: "2026-03-24T00:00:00Z",
              rules: { whitelist_codes: [], whitelist_node_types: [], whitelist_keys: [], mute_until_by_key: {} },
              meta: {},
            },
            {
              version_id: "ver_b",
              schema_version: "workflow_sandbox_rule_version.v1",
              ts: "2026-03-24T00:10:00Z",
              rules: { whitelist_codes: [], whitelist_node_types: ["ai_refine"], whitelist_keys: [], mute_until_by_key: {} },
              meta: {},
            },
          ],
        });
      }
      if (method === "POST" && url.endsWith("/governance/workflow-sandbox/rule-versions/ver_a/rollback")) {
        return jsonResponse(200, {
          ok: true,
          schema_version: "workflow_sandbox_rules.v1",
          rules: { whitelist_codes: [], whitelist_node_types: [], whitelist_keys: [], mute_until_by_key: {} },
          version_id: "ver_rollback",
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected sandbox rule request: ${method} ${url}` });
    },
  });
  const ruleState = await sandboxRuleStore.getRules({ mode: "base_api" });
  recordOutput("workflow_sandbox_rules", !!ruleState?.schema_version);
  const ruleVersions = await sandboxRuleStore.listVersions(20, { mode: "base_api" });
  recordOutput("workflow_sandbox_rule_version", !!ruleVersions?.items?.[0]?.schema_version);
  const comparedRules = await sandboxRuleStore.compareVersions("ver_a", "ver_b", { mode: "base_api" });
  recordOutput("workflow_sandbox_rule_compare", !!comparedRules?.schema_version);
  const rolledBackRules = await sandboxRuleStore.rollbackVersion("ver_a", { mode: "base_api" });
  recordOutput("workflow_sandbox_rule_rollback", !!rolledBackRules?.schema_version);

  const sandboxAutoFixStore = createWorkflowSandboxAutoFixStore({
    loadConfig: () => ({ mode: "base_api", glueUrl: "http://127.0.0.1:18081" }),
    sandboxSupport,
    fetchImpl: async (url, init = {}) => {
      if (url.endsWith("/governance/meta/control-plane")) {
        return governanceBoundaryResponse(
          "workflow_sandbox_autofix",
          "/governance/workflow-sandbox/autofix-state",
          ["/governance/workflow-sandbox/autofix-state", "/governance/workflow-sandbox/autofix-actions"]
        );
      }
      const method = String(init.method || "GET").toUpperCase();
      if (method === "GET" && url.endsWith("/governance/workflow-sandbox/autofix-state")) {
        return jsonResponse(200, {
          ok: true,
          state: {
            schema_version: "workflow_sandbox_autofix_state.v1",
            violation_events: [],
            forced_isolation_mode: "process",
            forced_until: "",
            last_actions: [],
            green_streak: 0,
          },
        });
      }
      if (method === "PUT" && url.endsWith("/governance/workflow-sandbox/autofix-state")) {
        const body = JSON.parse(String(init.body || "{}"));
        return jsonResponse(200, { ok: true, state: body });
      }
      if (method === "GET" && url.includes("/governance/workflow-sandbox/autofix-actions?")) {
        return jsonResponse(200, {
          ok: true,
          items: [{
            schema_version: "workflow_sandbox_autofix_action.v1",
            ts: "2026-03-24T00:10:00Z",
            actions: ["pause_queue"],
          }],
          forced_isolation_mode: "process",
          forced_until: "",
        });
      }
      return jsonResponse(500, { ok: false, error: `unexpected sandbox autofix request: ${method} ${url}` });
    },
  });
  const autofixState = await sandboxAutoFixStore.getState({ mode: "base_api" });
  recordOutput("workflow_sandbox_autofix_state", !!autofixState?.state?.schema_version);
  const autofixActions = await sandboxAutoFixStore.listActions(20, { mode: "base_api" });
  recordOutput("workflow_sandbox_autofix_action", !!autofixActions?.items?.[0]?.schema_version);
  const persistedAutofixState = await sandboxAutoFixStore.persistStateMirror({
    violation_events: [],
    forced_isolation_mode: "",
    forced_until: "",
    last_actions: [],
    green_streak: 0,
  }, { mode: "base_api" });
  recordOutput("workflow_sandbox_autofix_persisted_state", !!persistedAutofixState?.state?.schema_version);

  const requiredOutputSet = uniqueSorted([
    "workflow_app_registry_item",
    "workflow_quality_rule_set",
    "workflow_manual_review_item",
    "workflow_version_snapshot",
    "workflow_run_baseline_entry",
    "workflow_sandbox_rules",
    "workflow_sandbox_rule_version",
    "workflow_sandbox_rule_compare",
    "workflow_sandbox_rule_rollback",
    "workflow_sandbox_autofix_state",
    "workflow_sandbox_autofix_action",
    "workflow_sandbox_autofix_persisted_state",
  ]);
  const coveredOutputSet = uniqueSorted(runtimeCoveredOutputs);
  const missingRequiredRuntimeOutputs = uniqueSorted(requiredRuntimeOutputs.filter((name) => !coveredOutputSet.includes(String(name))));
  const issues = [];
  if (missingSourceSchemaVersionModules.length > 0) {
    issues.push(`source modules missing schema_version markers: ${missingSourceSchemaVersionModules.join(", ")}`);
  }
  if (missingRuntimeSchemaVersionOutputs.length > 0) {
    issues.push(`runtime outputs missing schema_version: ${uniqueSorted(missingRuntimeSchemaVersionOutputs).join(", ")}`);
  }
  if (missingRequiredRuntimeOutputs.length > 0) {
    issues.push(`required runtime outputs missing: ${missingRequiredRuntimeOutputs.join(", ")}`);
  }

  const payload = {
    status: issues.length > 0 ? "failed" : "passed",
    requiredStoreModules: modules.map((entry) => entry.name),
    sourceModuleCount: modules.length,
    sourceSchemaVersionCount: modules.length - missingSourceSchemaVersionModules.length,
    requiredRuntimeOutputs: requiredOutputSet,
    runtimeCheckCount: requiredOutputSet.length,
    runtimeSchemaVersionCount: coveredOutputSet.length,
    drift: {
      missingSourceSchemaVersionModules: uniqueSorted(missingSourceSchemaVersionModules),
      missingRuntimeSchemaVersionOutputs: uniqueSorted(missingRuntimeSchemaVersionOutputs),
      missingRequiredRuntimeOutputs,
    },
    issues: uniqueSorted(issues),
  };

  if (payload.status !== "passed") {
    failPayload(payload);
  }
  console.log(JSON.stringify(payload));
})().catch((error) => {
  failPayload({
    status: "failed",
    requiredStoreModules: [],
    sourceModuleCount: 0,
    sourceSchemaVersionCount: 0,
    requiredRuntimeOutputs: [],
    runtimeCheckCount: 0,
    runtimeSchemaVersionCount: 0,
    drift: {
      missingSourceSchemaVersionModules: [],
      missingRuntimeSchemaVersionOutputs: [],
      missingRequiredRuntimeOutputs: [],
    },
    issues: [error && error.stack ? error.stack : String(error)],
  });
});
'@

$env:AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON = $requiredOutputsJson
try {
  $nodeScript | node - $RepoRoot
}
finally {
  Remove-Item Env:AIWF_REQUIRED_RUNTIME_OUTPUTS_JSON -ErrorAction SilentlyContinue
}
if ($LASTEXITCODE -ne 0) {
  throw "governance store schema version checks failed"
}

Ok "governance store schema version check passed"
