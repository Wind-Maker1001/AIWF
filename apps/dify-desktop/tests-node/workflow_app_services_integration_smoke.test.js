const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppServicesModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-services.js")).href;
  return import(file);
}

class FakeElement {
  constructor(tagName = "div") {
    this.tagName = String(tagName).toUpperCase();
    this.children = [];
    this.style = {};
    this.className = "";
    this.textContent = "";
    this.value = "";
    this.checked = false;
    this.disabled = false;
    this.innerHTML = "";
    this.dataset = {};
    this.handlers = {};
    this.clientWidth = 800;
    this.clientHeight = 600;
  }

  append(...nodes) {
    this.children.push(...nodes);
  }

  appendChild(node) {
    this.children.push(node);
  }

  addEventListener(eventName, handler) {
    this.handlers[eventName] = handler;
  }

  querySelectorAll() {
    return [];
  }

  querySelector() {
    return null;
  }

  scrollIntoView() {}
}

function createElementsProxy(seed = {}) {
  const base = { ...seed };
  return new Proxy(base, {
    get(target, prop) {
      if (!(prop in target)) target[prop] = new FakeElement();
      return target[prop];
    },
  });
}

test("workflow app services smoke wires core panels and late services together", async () => {
  const { createWorkflowAppServices } = await loadAppServicesModule();
  const prevWindow = global.window;
  const prevDocument = global.document;
  const prevFetch = global.fetch;

  global.document = {
    createElement(tag) {
      return new FakeElement(tag);
    },
  };

  global.fetch = async () => ({
    ok: true,
    async json() {
      return { items: [] };
    },
  });

  const savedNames = [];
  const appRowRenders = [];
  const versionRenders = [];
  const queueControlRenders = [];
  const statusCalls = [];

  global.window = {
    localStorage: {
      getItem: () => null,
      setItem: () => {},
    },
    aiwfDesktop: {
      listTemplateMarketplace: async () => ({
        items: [{ id: "pack_1", name: "Pack", templates: [{ id: "tpl_1", name: "Template One" }] }],
      }),
      listWorkflowVersions: async () => ({
        items: [{ version_id: "ver_1", workflow_name: "Flow Smoke", ts: "2026-03-20T00:00:00Z" }],
      }),
      listWorkflowQueue: async () => ({
        items: [{ task_id: "task_1", label: "Task", status: "queued" }],
        control: { paused: false, quotas: { ai: 2 } },
      }),
      listWorkflowApps: async () => ({
        items: [{ app_id: "app_1", name: "Smoke App", updated_at: "2026-03-20T00:00:00Z" }],
      }),
    },
  };

  try {
    const els = createElementsProxy({
      workflowName: { ...new FakeElement("input"), value: "Flow Smoke" },
      templateSelect: new FakeElement("select"),
      templateParamsForm: null,
      templateParams: { value: "" },
      appRunParams: { value: "{}" },
      publishRequirePreflight: { checked: true },
      versionRows: new FakeElement("tbody"),
      queueRows: new FakeElement("tbody"),
      queueControlText: { textContent: "" },
      appRows: new FakeElement("tbody"),
      btnReviewsRefresh: new FakeElement("button"),
      btnQualityGateRefresh: new FakeElement("button"),
      qualityGateRunIdFilter: { value: "" },
      qualityGateStatusFilter: { value: "" },
      sandboxRows: new FakeElement("tbody"),
      sandboxRuleVersionRows: new FakeElement("tbody"),
      sandboxAutoFixRows: new FakeElement("tbody"),
      timelineRows: new FakeElement("tbody"),
      failureRows: new FakeElement("tbody"),
      auditRows: new FakeElement("tbody"),
      runHistoryRows: new FakeElement("tbody"),
      reviewRows: new FakeElement("tbody"),
      versionCompareSummary: { textContent: "" },
      versionCompareRows: new FakeElement("tbody"),
      cacheStatsText: { textContent: "" },
      reviewHistoryRows: new FakeElement("tbody"),
      compareSummary: { textContent: "" },
      compareRows: new FakeElement("tbody"),
      nodeConfigForm: new FakeElement("div"),
      nodeConfig: { value: "", disabled: false, style: {} },
      btnApplyNodeCfg: new FakeElement("button"),
      btnResetNodeCfg: new FakeElement("button"),
      btnFormatNodeCfg: new FakeElement("button"),
      selectedNodeInfo: { textContent: "" },
      inputMapRows: new FakeElement("tbody"),
      outputMapRows: new FakeElement("tbody"),
      selectedEdgeInfo: { textContent: "" },
      edgeWhenText: { value: "" },
      appSchemaForm: new FakeElement("div"),
      appSchemaJson: { value: "{}" },
      appRunParamsForm: new FakeElement("div"),
      canvasWrap: new FakeElement("div"),
      snapGrid: { checked: true },
      log: { textContent: "" },
      migrationSummary: { textContent: "" },
      migrationRows: new FakeElement("tbody"),
      sandboxHealthText: { textContent: "", style: {} },
    });

    const store = {
      state: { graph: { name: "Flow Smoke", workflow_id: "wf_1", nodes: [], edges: [] } },
      setWorkflowName: (name) => savedNames.push(name),
      exportGraph: () => ({ workflow_id: "wf_1", name: "Flow Smoke", nodes: [], edges: [] }),
      addNode: () => "n1",
      getNode: () => null,
      getEdge: () => null,
    };
    const canvas = {
      setSelectedIds() {},
      clientToWorld: () => ({ x: 100, y: 80 }),
      getSelectedIds: () => [],
    };
    const graphShellApi = {
      applyRestoredWorkflowGraph: (graph) => { appRowRenders.push({ graph }); },
    };
    const state = {
      getLastCompareResult: () => null,
      setLastCompareResult: () => {},
      getSelectedEdge: () => null,
      setSelectedEdge: () => {},
      getCfgViewMode: () => "form",
      setCfgViewMode: () => {},
      getLastPreflightReport: () => null,
      getLastTemplateAcceptanceReport: () => null,
      setLastAutoFixSummary: () => {},
      setLastTemplateAcceptanceReport: () => {},
      setLastPreflightReport: () => {},
    };

    const services = createWorkflowAppServices({
      els,
      store,
      canvas,
      setStatus: (text, ok) => statusCalls.push({ text, ok }),
      renderAll: () => {},
      refreshOfflineBoundaryHint: () => {},
      graphShellApi,
      state,
      staticConfig: {
        templateStorageKey: "templates.v1",
        builtinTemplates: [],
        nodeFormSchemas: {},
        edgeHintsByNodeType: {},
        qualityGatePrefsKey: "prefs.v1",
        nodeCatalog: [{ type: "clean_md", name: "Clean" }],
      },
      defaultNodeConfig: () => ({}),
      migrateLoadedWorkflowGraph: (graph) => ({ migrated: false, graph, notes: [] }),
      renderNodeRuns: () => {},
      refreshDiagnostics: async () => {},
    });

    const payload = services.runPayload({ params: { strict_output_gate: true } });
    assert.equal(payload.workflow_id, "wf_1");
    assert.deepEqual(savedNames, ["Flow Smoke"]);

    await services.refreshTemplateMarketplace();
    services.renderTemplateSelect();
    assert.ok(els.templateSelect.children.some((node) => node.value === "tpl_1"));

    await services.refreshVersions();
    assert.equal(els.versionRows.children.length, 1);

    await services.refreshQueue();
    assert.equal(els.queueRows.children.length, 1);
    assert.match(els.queueControlText.textContent, /队列状态/);

    await services.refreshApps();
    assert.equal(els.appRows.children.length, 1);

    assert.equal(typeof services.renderMigrationReportImpl, "function");
  } finally {
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevFetch === "undefined") delete global.fetch;
    else global.fetch = prevFetch;
  }
});
