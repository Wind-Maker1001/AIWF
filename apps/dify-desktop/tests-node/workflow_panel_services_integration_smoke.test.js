const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPanelServicesModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-panel-services.js")).href;
  return import(file);
}

function createElementStub() {
  return {
    value: "",
    checked: false,
    textContent: "",
    innerHTML: "",
    style: {},
    appendChild() {},
  };
}

test("workflow panel services smoke wires child panel modules together", async () => {
  const { createWorkflowPanelServices } = await loadPanelServicesModule();
  const prevWindow = global.window;
  const prevLocalStorage = global.localStorage;

  const rows = {
    gate: [],
    sandbox: [],
    sandboxVersions: [],
    sandboxFix: [],
    timeline: [],
    failure: [],
    audit: [],
    runs: [],
    queue: [],
    queueControl: [],
    review: [],
    versions: [],
    compare: [],
    cache: [],
  };
  const statuses = [];
  const savedRuleSets = [];
  const removedRuleSets = [];
  const exported = [];
  const localStore = new Map();

  global.localStorage = {
    getItem: (key) => localStore.get(key) || null,
    setItem: (key, value) => localStore.set(key, value),
  };

  global.window = {
    aiwfDesktop: {
      listWorkflowQualityGateReports: async () => ({ items: [{ run_id: "run_gate" }] }),
      exportWorkflowQualityGateReports: async () => ({ ok: true, path: "D:/exports/gate.json" }),
      getWorkflowSandboxAlerts: async () => ({ by_node: [{ node_type: "clean_md", node_id: "n1", count: 1 }], health: {}, rules: { x: 1 } }),
      exportWorkflowSandboxAuditReport: async () => ({ ok: true, path: "D:/exports/sandbox.md" }),
      getWorkflowSandboxAlertRules: async () => ({ ok: true, rules: { whitelist_codes: ["a"] } }),
      listWorkflowSandboxRuleVersions: async () => ({ items: [{ version_id: "ver_1", ts: "2026-03-20" }] }),
      setWorkflowSandboxAlertRules: async () => ({ ok: true, rules: { whitelist_codes: ["b"] } }),
      muteWorkflowSandboxAlert: async () => ({ ok: true, key: "mute_1", mute_until: "2026-03-20T01:00:00Z" }),
      exportWorkflowSandboxPreset: async () => ({ ok: true, path: "D:/exports/preset.json" }),
      importWorkflowSandboxPreset: async () => ({ ok: true, path: "D:/imports/preset.json", preset: { thresholds: { yellow: 2 } } }),
      listWorkflowSandboxAutoFixActions: async () => ({ items: [{ ts: "2026-03-20", count: 1, actions: ["pause"] }] }),
      getWorkflowRunTimeline: async () => ({ ok: true, timeline: [{ node_id: "n1", type: "clean_md", status: "ok", seconds: 1 }] }),
      getWorkflowFailureSummary: async () => ({ by_node: { n2: { failed: 1, samples: ["boom"] } } }),
      listWorkflowAuditLogs: async () => ({ items: [{ ts: "2026-03-20", action: "run", detail: { ok: true } }] }),
      listWorkflowRuns: async () => ({ items: [{ run_id: "run_1", status: "done" }] }),
      listWorkflowQueue: async () => ({ items: [{ task_id: "task_1", status: "queued" }], control: { paused: false } }),
      setWorkflowQueueControl: async ({ paused }) => ({ ok: paused }),
      listManualReviews: async () => ({ items: [{ run_id: "run_1", review_key: "gate_a", status: "pending" }] }),
      listWorkflowVersions: async () => ({ items: [{ version_id: "ver_a", workflow_name: "wf-a" }] }),
      compareWorkflowVersions: async () => ({ ok: true, summary: { version_a: "ver_a", version_b: "ver_b" }, node_diff: [] }),
      getWorkflowNodeCacheStats: async () => ({ stats: { entries: 1, hits: 2, misses: 0, hit_rate: 1 } }),
      clearWorkflowNodeCache: async () => ({ ok: true, stats: { entries: 0, hits: 0, misses: 0, hit_rate: 0 } }),
      listQualityRuleSets: async () => ({ sets: [{ id: "set_a", name: "Set A", version: "v1" }] }),
      saveQualityRuleSet: async (payload) => { savedRuleSets.push(payload); return { ok: true }; },
      removeQualityRuleSet: async ({ id }) => { removedRuleSets.push(id); return { ok: true }; },
    },
  };

  try {
    const els = {
      qualityGateRunIdFilter: { value: "run_gate" },
      qualityGateStatusFilter: { value: "blocked" },
      qualityGateExportFormat: { value: "json" },
      sandboxExportFormat: { value: "md" },
      sandboxPreset: { value: "balanced" },
      sandboxMuteNodeType: { value: "clean_md" },
      sandboxMuteNodeId: { value: "n1" },
      sandboxMuteCode: { value: "*" },
      sandboxMuteMinutes: { value: "30" },
      timelineRunId: { value: "run_timeline" },
      versionCompareA: { value: "ver_a" },
      versionCompareB: { value: "ver_b" },
      qualityRuleSetId: { value: "set_a" },
      qualityRuleSetSelect: { value: "set_a", innerHTML: "", appendChild(node) { exported.push({ option: node.value }); } },
      sandboxThresholdYellow: createElementStub(),
      sandboxThresholdRed: createElementStub(),
      sandboxDedupWindowSec: createElementStub(),
    };

    const services = createWorkflowPanelServices({
      els,
      store: {
        exportGraph: () => ({
          nodes: [{ id: "n1", type: "quality_check_v3", config: { rules: { required_columns: ["amount"] } } }],
        }),
      },
      qualityGatePrefsKey: "prefs.key",
      setStatus: (text, ok) => statuses.push({ text, ok }),
      qualityGateFilterPayload: () => ({ run_id: "run_gate", status: "blocked" }),
      qualityGatePrefsPayload: () => ({ filter: { run_id: "run_gate", status: "blocked" }, format: "json" }),
      renderQualityGateRows: (items) => rows.gate.push(items),
      sandboxThresholdsPayload: () => ({ yellow: 1, red: 3 }),
      sandboxDedupWindowSec: () => 60,
      sandboxRulesPayloadFromUi: () => ({ whitelist_codes: ["x"] }),
      applySandboxRulesToUi: (rules) => rows.sandbox.push({ rules }),
      applySandboxPresetToUi: (preset) => rows.sandbox.push({ preset }),
      currentSandboxPresetPayload: () => ({ thresholds: { yellow: 1 } }),
      applySandboxPresetPayload: (preset) => rows.sandbox.push({ presetImported: preset }),
      renderSandboxRows: (out) => rows.sandbox.push({ alerts: out }),
      renderSandboxRuleVersionRows: (items) => rows.sandboxVersions.push(items),
      renderSandboxAutoFixRows: (items) => rows.sandboxFix.push(items),
      renderTimelineRows: (out) => rows.timeline.push(out),
      renderFailureRows: (out) => rows.failure.push(out),
      renderAuditRows: (items) => rows.audit.push(items),
      renderRunHistoryRows: (items) => rows.runs.push(items),
      renderQueueRows: (items) => rows.queue.push(items),
      renderQueueControl: (control) => rows.queueControl.push(control),
      renderReviewRows: (items) => rows.review.push(items),
      renderVersionRows: (items) => rows.versions.push(items),
      renderVersionCompare: (out) => rows.compare.push(out),
      renderCacheStats: (stats) => rows.cache.push(stats),
    });

    services.saveQualityGatePrefs();
    services.loadQualityGatePrefs();
    await services.refreshQualityGateReports();
    await services.exportQualityGateReports();
    await services.refreshSandboxAlerts();
    await services.loadSandboxRules();
    await services.refreshSandboxRuleVersions();
    await services.saveSandboxRules();
    await services.applySandboxPreset();
    await services.applySandboxMute();
    await services.exportSandboxPreset();
    await services.importSandboxPreset();
    await services.refreshSandboxAutoFixLog();
    await services.refreshTimeline();
    await services.refreshFailureSummary();
    await services.refreshAudit();
    await services.refreshRunHistory();
    await services.refreshQueue();
    await services.pauseQueue();
    await services.resumeQueue();
    await services.refreshReviewQueue();
    await services.refreshVersions();
    await services.compareVersions();
    await services.refreshCacheStats();
    await services.clearCache();
    services.handleQualityRuleSetSelectChange();
    await services.refreshQualityRuleSets();
    await services.saveQualityRuleSetFromGraph();
    await services.removeQualityRuleSetCurrent();

    assert.equal(localStore.has("prefs.key"), true);
    assert.equal(els.qualityGateRunIdFilter.value, "run_gate");
    assert.equal(rows.gate.length > 0, true);
    assert.equal(rows.runs.length > 0, true);
    assert.equal(rows.queue.length > 0, true);
    assert.equal(rows.review.length > 0, true);
    assert.equal(rows.versions.length > 0, true);
    assert.equal(rows.compare[0]?.ok, true);
    assert.equal(rows.cache.length > 0, true);
    assert.equal(savedRuleSets.length, 1);
    assert.deepEqual(removedRuleSets, ["set_a"]);
    assert.ok(statuses.some((item) => item.text.includes("质量规则集已保存")));
  } finally {
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
    if (typeof prevLocalStorage === "undefined") delete global.localStorage;
    else global.localStorage = prevLocalStorage;
  }
});
