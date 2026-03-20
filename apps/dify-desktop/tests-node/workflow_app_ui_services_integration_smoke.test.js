const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppUiServicesModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-ui-services.js")).href;
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
  }

  append(...nodes) {
    this.children.push(...nodes);
  }

  appendChild(node) {
    this.children.push(node);
  }
}

test("workflow app ui services smoke wires status diagnostics connectivity and debug api", async () => {
  const { createWorkflowUiServices } = await loadAppUiServicesModule();

  const prevWindow = global.window;
  const prevDocument = global.document;
  const prevFetch = global.fetch;

  global.document = {
    createElement(tag) {
      return new FakeElement(tag);
    },
  };

  const renderCalls = [];
  const store = {
    exportGraph: () => ({
      workflow_id: "wf_1",
      nodes: [
        { id: "n1", type: "ai_refine" },
        { id: "n2", type: "clean_md" },
      ],
      edges: [],
    }),
    hasEdge: () => false,
    linkToFrom: () => ({ ok: true }),
    unlink: () => renderCalls.push("unlink"),
    importGraph: (graph) => { renderCalls.push({ import: graph }); },
  };
  const canvas = {
    getRouteMetrics: () => ({ edges: 2 }),
    setSelectedIds: (ids) => { renderCalls.push({ select: ids }); },
    getSelectedIds: () => ["n1"],
  };

  global.fetch = async () => ({
    ok: true,
    async json() {
      return { items: [{ operator: "transform_rows_v3", calls: 2, err: 1, p95_ms: 500 }] };
    },
  });

  global.window = {
    location: { search: "?debug=1" },
    aiwfDesktop: {
      getWorkflowDiagnostics: async () => ({
        by_chiplet: {
          clean_md: { failure_rate: 0.1, seconds_avg: 0.25 },
        },
      }),
      getWorkflowPerfDashboard: async () => ({
        items: [{ chiplet: "clean_md", error_rate: 0.2, p95_seconds: 0.4, retry_rate: 0.05, fallback_rate: 0 }],
      }),
    },
  };

  try {
    const els = {
      status: new FakeElement("div"),
      nodeRuns: new FakeElement("tbody"),
      diagRuns: new FakeElement("tbody"),
      aiEndpoint: { value: "" },
      aiModel: { value: "" },
      offlineBoundaryHint: { textContent: "" },
      rustEndpoint: { value: "http://localhost:8000" },
    };

    const ui = createWorkflowUiServices({
      els,
      store,
      canvas,
      renderAll: () => renderCalls.push("renderAll"),
    });

    ui.setStatus("ready", true);
    assert.equal(els.status.className, "status ok");
    assert.equal(els.status.textContent, "ready");

    ui.refreshOfflineBoundaryHint();
    assert.match(els.offlineBoundaryHint.textContent, /离线能力边界/);

    ui.applyDeepSeekDefaults();
    assert.equal(els.aiEndpoint.value, "https://api.deepseek.com/v1/chat/completions");
    assert.equal(els.aiModel.value, "deepseek-chat");
    assert.equal(els.status.textContent, "已填充 DeepSeek 接口参数（请确认 API Key）");

    ui.renderNodeRuns([{ type: "clean_md", status: "ok", seconds: 0.2, output_bytes: 1024, error_kind: "" }]);
    assert.equal(els.nodeRuns.children.length, 1);

    await ui.refreshDiagnostics();
    assert.ok(els.diagRuns.children.length >= 1);

    assert.equal(typeof global.window.__aiwfDebug.tryLink, "function");
    assert.deepEqual(global.window.__aiwfDebug.routeStats(), { edges: 2 });
    assert.deepEqual(global.window.__aiwfDebug.selectNodes(["n1", "n2"]), ["n1"]);
    const linked = global.window.__aiwfDebug.tryLink("n1", "n2");
    assert.equal(linked.ok, true);
    assert.ok(renderCalls.includes("renderAll"));
  } finally {
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevFetch === "undefined") delete global.fetch;
    else global.fetch = prevFetch;
  }
});
