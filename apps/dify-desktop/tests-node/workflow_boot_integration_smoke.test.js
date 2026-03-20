const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadBootModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-boot.js")).href;
  return import(file);
}

function createElementStub() {
  return {
    handlers: {},
    value: "",
    checked: false,
    disabled: false,
    className: "",
    textContent: "",
    innerHTML: "",
    style: {},
    dataset: {},
    clientWidth: 800,
    clientHeight: 600,
    addEventListener(eventName, handler) {
      this.handlers[eventName] = handler;
    },
    appendChild() {},
    append() {},
    querySelector() { return null; },
    querySelectorAll() { return []; },
    remove() {},
    scrollIntoView() {},
    getAttribute(name) { return this.dataset[name]; },
  };
}

function createElementsProxy(seed = {}) {
  const store = { ...seed };
  return new Proxy(store, {
    get(target, prop) {
      if (!(prop in target)) target[prop] = createElementStub();
      return target[prop];
    },
  });
}

test("workflow boot smoke wires startup toolbar and editor interactions without GUI", async () => {
  const { bootWorkflowApp } = await loadBootModule();
  const statuses = [];
  const calls = [];
  const windowHandlers = {};
  const prevWindow = global.window;
  global.window = {
    addEventListener(eventName, handler) {
      windowHandlers[eventName] = handler;
    },
    aiwfDesktop: {},
  };

  const els = createElementsProxy({
    edgeWhenKind: { ...createElementStub(), value: "rule" },
    appSchemaJson: { ...createElementStub(), value: '{"title":{"type":"string"}}' },
    compareOnlyChanged: createElementStub(),
    compareOnlyStatusChanged: createElementStub(),
    compareMinDelta: createElementStub(),
    snapGrid: { ...createElementStub(), checked: true },
    canvasWrap: createElementStub(),
  });

  try {
    bootWorkflowApp({
      els,
      setStatus: (text, ok) => statuses.push({ text, ok }),
      handleAddNode: () => calls.push("add"),
      resetWorkflow: () => calls.push("reset"),
      clearWorkflow: () => calls.push("clear"),
      runWorkflowPreflight: async () => ({ ok: true, issues: [{ level: "warning" }] }),
      exportPreflightReport: () => calls.push("preflightExport"),
      autoFixGraphStructure: () => ({ changed: false }),
      setLastAutoFixSummary: () => calls.push("fixSummary"),
      renderAutoFixDiff: () => calls.push("fixDiff"),
      runWorkflow: () => calls.push("run"),
      enqueueWorkflowRun: () => calls.push("enqueue"),
      refreshQueue: async () => calls.push("queue"),
      pauseQueue: () => calls.push("pause"),
      resumeQueue: () => calls.push("resume"),
      refreshVersions: async () => calls.push("versions"),
      compareVersions: () => calls.push("compareVersions"),
      refreshCacheStats: async () => calls.push("cacheStats"),
      clearCache: () => calls.push("clearCache"),
      handleAppSchemaAdd: () => calls.push("schemaAdd"),
      handleAppSchemaSyncJson: () => calls.push("schemaSync"),
      handleAppSchemaFromJson: () => calls.push("schemaFromJson"),
      handleAppRunSyncJson: () => calls.push("runSync"),
      handleAppRunFromJson: () => calls.push("runFromJson"),
      publishApp: () => calls.push("publish"),
      refreshApps: async () => calls.push("apps"),
      refreshTimeline: async () => calls.push("timeline"),
      refreshFailureSummary: async () => calls.push("failure"),
      refreshSandboxAlerts: async () => calls.push("sandboxAlerts"),
      exportSandboxAudit: () => calls.push("sandboxExport"),
      loadSandboxRules: () => calls.push("sandboxLoad"),
      saveSandboxRules: () => calls.push("sandboxSave"),
      applySandboxPreset: () => calls.push("sandboxPreset"),
      applySandboxMute: () => calls.push("sandboxMute"),
      refreshSandboxRuleVersions: async () => calls.push("sandboxVersions"),
      exportSandboxPreset: () => calls.push("sandboxPresetExport"),
      importSandboxPreset: () => calls.push("sandboxPresetImport"),
      refreshSandboxAutoFixLog: async () => calls.push("sandboxFixLog"),
      refreshQualityGateReports: async () => calls.push("qualityGate"),
      exportQualityGateReports: () => calls.push("qualityGateExport"),
      saveQualityGatePrefs: () => calls.push("qualityGatePrefs"),
      refreshAudit: async () => calls.push("audit"),
      refreshDiagnostics: async () => calls.push("diag"),
      refreshRunHistory: async () => calls.push("history"),
      refreshReviewQueue: async () => calls.push("reviewQueue"),
      refreshReviewHistory: async () => calls.push("reviewHistory"),
      exportReviewHistory: () => calls.push("reviewExport"),
      compareRuns: () => calls.push("compareRuns"),
      saveCurrentRunAsBaseline: () => calls.push("saveBaseline"),
      compareWithLatestBaseline: () => calls.push("compareBaseline"),
      loadLineageForRunA: () => calls.push("lineage"),
      exportCompareReport: () => calls.push("compareExport"),
      renderCompareResult: (out) => calls.push({ compareRender: out }),
      getLastCompareResult: () => ({ ok: true, summary: { changed_nodes: 1 } }),
      exportJson: () => calls.push("exportJson"),
      saveFlow: () => calls.push("saveFlow"),
      loadFlow: () => calls.push("loadFlow"),
      applySelectedTemplate: () => calls.push("applyTemplate"),
      saveCurrentAsTemplate: () => calls.push("saveTemplate"),
      installTemplatePack: () => calls.push("installTemplatePack"),
      removeTemplatePackByCurrentTemplate: () => calls.push("removeTemplatePack"),
      exportTemplatePackByCurrentTemplate: () => calls.push("exportTemplatePack"),
      runTemplateAcceptance: () => calls.push("templateAcceptance"),
      exportTemplateAcceptanceReport: () => calls.push("templateAcceptanceExport"),
      renderTemplateParamsForm: () => calls.push("templateParams"),
      handleQualityRuleSetSelectChange: () => calls.push("ruleSetSelect"),
      refreshQualityRuleSets: async () => calls.push("ruleSets"),
      saveQualityRuleSetFromGraph: () => calls.push("ruleSetSave"),
      removeQualityRuleSetCurrent: () => calls.push("ruleSetRemove"),
      renderPalette: () => calls.push("palette"),
      applyDeepSeekDefaults: () => calls.push("deepSeek"),
      refreshOfflineBoundaryHint: () => calls.push("boundary"),
      store: { ok: true },
      canvas: { getZoom: () => 1 },
      singleSelectedNode: () => null,
      renderNodeConfigEditor: () => calls.push("nodeEditor"),
      renderAll: () => calls.push("renderAll"),
      setZoom: (zoom, point) => calls.push({ zoom, point }),
      fitCanvasToView: () => calls.push("fit"),
      applyArrange: (mode) => calls.push({ arrange: mode }),
      selectedEdgeRef: { get: () => null, set: () => {} },
      defaultNodeConfig: () => ({}),
      parseNodeConfigText: () => ({}),
      parseNodeConfigForm: () => ({}),
      prettyJson: () => "{}",
      renderNodeConfigForm: () => calls.push("nodeForm"),
      setCfgMode: (mode) => calls.push({ cfgMode: mode }),
      getCfgViewMode: () => "form",
      parseEdgeWhenText: () => null,
      applyEdgeWhenToBuilder: () => calls.push("edgeBuilder"),
      renderEdgeConfigEditor: () => calls.push("edgeEditor"),
      setEdgeWhenBuilderVisibility: (kind) => calls.push({ edgeKind: kind }),
      syncEdgeTextFromBuilder: () => calls.push("edgeSync"),
      syncCanvasPanels: () => calls.push("syncPanels"),
      handleCanvasDragOver: () => calls.push("dragOver"),
      handleCanvasDrop: () => calls.push("drop"),
      renderTemplateSelect: () => calls.push("templateSelect"),
      refreshTemplateMarketplace: async () => calls.push("templateMarket"),
      rebuildEdgeHints: (edge) => calls.push({ hints: edge }),
      renderNodeRuns: () => calls.push("nodeRuns"),
      renderDiagRuns: () => calls.push("diagRuns"),
      renderRunHistoryRows: () => calls.push("runHistoryRows"),
      renderQueueRows: () => calls.push("queueRows"),
      renderQueueControl: () => calls.push("queueControl"),
      renderVersionRows: () => calls.push("versionRows"),
      renderVersionCompare: (out) => calls.push({ versionCompare: out }),
      renderReviewRows: () => calls.push("reviewRows"),
      renderCacheStats: () => calls.push("cacheRender"),
      renderAppSchemaForm: () => calls.push("schemaRender"),
      syncAppSchemaJsonFromForm: () => { els.appSchemaJson.value = '{"title":{"type":"string"}}'; calls.push("schemaJsonSync"); },
      renderRunParamsFormBySchema: () => calls.push("runParamsRender"),
      renderAppRows: () => calls.push("appRows"),
      renderTimelineRows: () => calls.push("timelineRows"),
      renderFailureRows: () => calls.push("failureRows"),
      renderSandboxHealth: () => calls.push("sandboxHealth"),
      sandboxThresholdsPayload: () => ({ yellow: 1 }),
      sandboxDedupWindowSec: () => 60,
      renderSandboxRows: () => calls.push("sandboxRows"),
      renderSandboxRuleVersionRows: () => calls.push("sandboxRuleRows"),
      renderSandboxAutoFixRows: () => calls.push("sandboxFixRows"),
      renderQualityGateRows: () => calls.push("qualityGateRows"),
      loadQualityGatePrefs: () => calls.push("loadQualityPrefs"),
      renderAuditRows: () => calls.push("auditRows"),
      renderReviewHistoryRows: () => calls.push("reviewHistoryRows"),
      renderMigrationReport: () => calls.push("migration"),
      renderPreflightReport: () => calls.push("preflightRender"),
      renderAutoFixDiff: () => calls.push("autoFixRender"),
    });

    await els.btnPreflight.handlers.click();
    els.btnRun.handlers.click();
    els.btnQueuePause.handlers.click();
    els.btnCfgJson.handlers.click();
    els.compareOnlyChanged.handlers.change();
    let prevented = false;
    els.canvasWrap.handlers.wheel({
      ctrlKey: true,
      deltaY: -1,
      clientX: 12,
      clientY: 34,
      preventDefault() { prevented = true; },
    });

    assert.equal(prevented, true);
    assert.ok(statuses.some((item) => item.text === "就绪。可拖拽节点并连线后运行。"));
    assert.ok(statuses.some((item) => item.text === "预检通过（1 条警告）"));
    assert.ok(calls.includes("run"));
    assert.ok(calls.includes("pause"));
    assert.ok(calls.some((item) => item?.cfgMode === "json"));
    assert.ok(calls.some((item) => item?.compareRender?.ok === true));
    assert.ok(calls.some((item) => item?.zoom === 1.08));
    assert.ok(windowHandlers.resize);
  } finally {
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
  }
});
