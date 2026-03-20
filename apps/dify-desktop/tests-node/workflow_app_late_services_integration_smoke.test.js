const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadLateServicesModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-late-services.js")).href;
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
    this.innerHTML = "";
    this.dataset = {};
    this.draggable = false;
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

  querySelectorAll(selector) {
    if (selector === ".palette-item") return this.children.filter((child) => child.className === "palette-item");
    return [];
  }

  querySelector() {
    return null;
  }

  scrollTo(opts) {
    this.scrollLeft = opts?.left || 0;
    this.scrollTop = opts?.top || 0;
  }

  getBoundingClientRect() {
    return { left: 10, top: 20, width: this.clientWidth, height: this.clientHeight };
  }
}

test("workflow late services smoke wires palette flow io canvas view and app publish", async () => {
  const { createWorkflowLateServices } = await loadLateServicesModule();
  const prevWindow = global.window;
  const prevDocument = global.document;

  global.document = {
    createElement(tag) {
      return new FakeElement(tag);
    },
  };

  const statuses = [];
  const calls = [];
  const store = {
    state: { graph: { name: "Flow Smoke", nodes: [], edges: [] } },
    addNode(type, x, y, config) {
      calls.push({ addNode: { type, x, y, config } });
      return "n1";
    },
    exportGraph() {
      return {
        workflow_id: "wf_smoke",
        name: "Flow Smoke",
        nodes: [
          { id: "n1", type: "clean_md", x: 10, y: 10, config: {} },
          { id: "n2", type: "clean_md", x: 10, y: 10, config: {} },
        ],
        edges: [
          { from: "n1", to: "n2" },
          { from: "n1", to: "n2" },
          { from: "n2", to: "n2" },
          { from: "x", to: "n1" },
        ],
      };
    },
    getNode(id) {
      if (id === "n1") return { id: "n1", x: 120, y: 160 };
      return null;
    },
  };

  const canvas = {
    zoom: 1,
    getZoom() { return this.zoom; },
    setSnap(value) { calls.push({ snap: value }); },
    setArrangePolicy(value) { calls.push({ arrangePolicy: value }); },
    setZoom(value, focus) { this.zoom = value; calls.push({ zoom: value, focus }); },
    fitToView() { calls.push("fit"); return true; },
    alignSelected(mode) { calls.push({ align: mode }); return { ok: true, moved: 1, total: 1 }; },
    setSelectedIds(ids) { calls.push({ selected: ids }); },
  };

  global.window = {
    aiwfDesktop: {
      listWorkflowApps: async () => ({ items: [{ app_id: "app_1", name: "Demo App" }] }),
      saveWorkflow: async () => ({ ok: true, path: "D:/flows/smoke.json" }),
    },
  };

  try {
    const els = {
      log: { textContent: "" },
      workflowName: { value: "Flow Smoke" },
      nodeType: { value: "ds_refine" },
      aiEndpoint: { value: "" },
      aiKey: { value: "" },
      aiModel: { value: "" },
      palette: new FakeElement("div"),
      canvasWrap: new FakeElement("div"),
      zoomText: { textContent: "" },
      appRows: new FakeElement("tbody"),
      appPublishName: { value: "Smoke App" },
      appSchemaJson: { value: "{}" },
      appRunParams: { value: "{}" },
      preflightSummary: { textContent: "", style: {} },
      preflightRisk: { textContent: "", style: {} },
      preflightRows: new FakeElement("tbody"),
      preflightFixSummary: { textContent: "", style: {} },
      preflightFixRows: new FakeElement("tbody"),
      snapGrid: { checked: true },
    };

    const late = createWorkflowLateServices({
      els,
      store,
      canvas,
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => store.exportGraph(),
      refreshVersions: async () => { calls.push("refreshVersions"); },
      migrateLoadedWorkflowGraph: (graph) => ({ migrated: false, graph, notes: [] }),
      applyLoadedWorkflowGraph: (graph) => { calls.push({ applyGraph: graph }); },
      getLoadedWorkflowName: () => "Loaded Flow",
      renderMigrationReport: (report) => { calls.push({ migration: report }); },
      nodeCatalog: [{ type: "ai_refine", name: "AI Refine", desc: "demo" }],
      defaultNodeConfigFn: () => ({ provider_name: "DeepSeek", ai_model: "deepseek-chat" }),
      renderAll: () => { calls.push("renderAll"); },
      renderNodeConfigEditor: () => { calls.push("nodeEditor"); },
      renderEdgeConfigEditor: () => { calls.push("edgeEditor"); },
      refreshOfflineBoundaryHint: () => { calls.push("boundary"); },
      getNode: (id) => store.getNode(id),
      selectNodeIds: (ids) => { calls.push({ selectNodeIds: ids }); },
      computeDropPosition: () => ({ x: 70, y: 80 }),
      allTemplates: () => [],
      currentTemplateGovernance: () => ({ mode: "strict" }),
      parseRunParamsLoose: () => ({}),
      collectAppSchemaFromForm: () => ({}),
      normalizeAppSchemaObject: (obj) => obj,
      getLastPreflightReport: () => null,
      getLastTemplateAcceptanceReport: () => null,
      setLastAutoFixSummary: (summary) => { calls.push({ fixSummary: summary }); },
      setLastTemplateAcceptanceReport: (report) => { calls.push({ acceptance: report }); },
      setLastPreflightReport: (report) => { calls.push({ preflight: report }); },
      appSchemaRowsFromObject: () => [],
      renderAppSchemaForm: () => {},
      syncAppSchemaJsonFromForm: () => {},
      syncAppSchemaFormFromJson: () => {},
      syncRunParamsJsonFromForm: () => {},
      syncRunParamsFormFromJson: () => {},
      runPayload: () => ({ workflow_id: "wf_smoke" }),
      renderNodeRuns: () => {},
      refreshDiagnostics: async () => { calls.push("diag"); },
      refreshRunHistory: async () => { calls.push("history"); },
      refreshReviewQueue: async () => { calls.push("review"); },
      refreshQueue: async () => { calls.push("queue"); },
      renderAppRows: (items) => { calls.push({ appRows: items }); },
    });

    late.renderPalette();
    assert.ok(els.palette.children.length >= 1);

    late.handleAddNode();
    assert.ok(calls.some((item) => item?.addNode?.type === "ai_refine"));

    late.exportJson();
    assert.match(els.log.textContent, /"workflow_id": "wf_smoke"/);

    await late.saveFlow();
    assert.ok(statuses.some((item) => item.ok === true));
    assert.ok(calls.includes("refreshVersions"));

    late.setZoom(1.25);
    assert.equal(els.zoomText.textContent, "125%");

    late.focusNodeInCanvas("n1");
    assert.ok(calls.some((item) => Array.isArray(item?.selectNodeIds) && item.selectNodeIds[0] === "n1"));

    const fix = late.autoFixGraphStructure();
    assert.equal(fix.changed, true);
    assert.ok(calls.some((item) => item?.applyGraph));

    await late.refreshApps();
    assert.ok(calls.some((item) => Array.isArray(item?.appRows)));
  } finally {
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
  }
});
