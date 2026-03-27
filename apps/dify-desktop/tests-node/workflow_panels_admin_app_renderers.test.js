const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

class FakeElement {
  constructor(tagName = "div") {
    this.tagName = String(tagName).toUpperCase();
    this.children = [];
    this.style = {};
    this.className = "";
    this.textContent = "";
    this.value = "";
    this.innerHTML = "";
  }

  append(...nodes) {
    this.children.push(...nodes);
  }

  appendChild(node) {
    this.children.push(node);
  }
}

async function loadModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/panels-ui-admin-app-renderers.js")).href;
  return import(file);
}

test("workflow admin app renderers treat failed app runs as failure", async () => {
  const { createWorkflowPanelsAdminAppRenderers } = await loadModule();
  const statuses = [];
  const refreshes = [];
  const prevDocument = global.document;
  const prevWindow = global.window;
  global.document = {
    createElement(tagName) {
      return new FakeElement(tagName);
    },
  };
  global.window = {
    aiwfDesktop: {
      runWorkflowApp: async () => ({
        ok: false,
        app_id: "app_1",
        provider: "glue_http",
        result: {
          ok: false,
          run_id: "run_fail_1",
          status: "failed",
          error: "boom",
        },
        run_id: "run_fail_1",
        status: "failed",
        error: "boom",
      }),
    },
  };

  try {
    const els = {
      appRows: new FakeElement("tbody"),
      appRunParams: { value: "{}" },
      log: { textContent: "" },
      timelineRunId: { value: "" },
      appSchemaJson: { value: "" },
    };
    const renderers = createWorkflowPanelsAdminAppRenderers(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      refreshRunHistory: async () => refreshes.push("history"),
      refreshDiagnostics: async () => refreshes.push("diag"),
      normalizeAppSchemaObject: (obj) => obj,
      renderAppSchemaForm: () => {},
      appSchemaRowsFromObject: () => [],
      renderRunParamsFormBySchema: () => {},
      collectRunParamsForm: () => ({}),
      runPayload: () => ({}),
    });

    renderers.renderAppRows([{ app_id: "app_1", name: "Finance App", params_schema: {} }]);
    const runBtn = els.appRows.children[0].children[2].children[0];
    await runBtn.onclick();

    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].ok, false);
    assert.match(statuses[0].text, /应用运行失败/i);
    assert.match(els.log.textContent, /"run_id": "run_fail_1"/);
    assert.equal(els.timelineRunId.value, "run_fail_1");
    assert.deepEqual(refreshes, ["history", "diag"]);
  } finally {
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
  }
});

test("workflow admin app renderers show terminal app run status when run is not ok but has no error", async () => {
  const { createWorkflowPanelsAdminAppRenderers } = await loadModule();
  const statuses = [];
  const prevDocument = global.document;
  const prevWindow = global.window;
  global.document = {
    createElement(tagName) {
      return new FakeElement(tagName);
    },
  };
  global.window = {
    aiwfDesktop: {
      runWorkflowApp: async () => ({
        ok: false,
        app_id: "app_1",
        provider: "glue_http",
        result: {
          ok: false,
          run_id: "run_pending_1",
          status: "pending_review",
        },
        run_id: "run_pending_1",
        status: "pending_review",
        error: "",
      }),
    },
  };

  try {
    const els = {
      appRows: new FakeElement("tbody"),
      appRunParams: { value: "{}" },
      log: { textContent: "" },
      timelineRunId: { value: "" },
      appSchemaJson: { value: "" },
    };
    const renderers = createWorkflowPanelsAdminAppRenderers(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      refreshRunHistory: async () => {},
      refreshDiagnostics: async () => {},
      normalizeAppSchemaObject: (obj) => obj,
      renderAppSchemaForm: () => {},
      appSchemaRowsFromObject: () => [],
      renderRunParamsFormBySchema: () => {},
      collectRunParamsForm: () => ({}),
      runPayload: () => ({}),
    });

    renderers.renderAppRows([{ app_id: "app_1", name: "Finance App", params_schema: {} }]);
    const runBtn = els.appRows.children[0].children[2].children[0];
    await runBtn.onclick();

    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].ok, false);
    assert.equal(statuses[0].text, "应用运行结束: pending_review");
  } finally {
    if (typeof prevDocument === "undefined") delete global.document;
    else global.document = prevDocument;
    if (typeof prevWindow === "undefined") delete global.window;
    else global.window = prevWindow;
  }
});
