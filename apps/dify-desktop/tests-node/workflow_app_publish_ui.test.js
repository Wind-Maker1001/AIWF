const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppPublishUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-publish-ui.js")).href;
  return import(file);
}

test("workflow app publish ui adds blank schema row and syncs derived json", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const calls = [];
  const ui = createWorkflowAppPublishUi({}, {
    collectAppSchemaFromForm: () => ({ title: { type: "string", required: true } }),
    appSchemaRowsFromObject: (schema) => {
      calls.push({ kind: "rows", schema });
      return [{ key: "title", type: "string", required: true, defaultText: "", description: "" }];
    },
    renderAppSchemaForm: (rows) => calls.push({ kind: "render", rows }),
    syncAppSchemaJsonFromForm: () => calls.push({ kind: "sync-schema" }),
    syncRunParamsFormFromJson: () => calls.push({ kind: "sync-run" }),
  });

  ui.handleAppSchemaAdd();

  assert.equal(calls[0].kind, "rows");
  assert.deepEqual(calls[1], {
    kind: "render",
    rows: [
      { key: "title", type: "string", required: true, defaultText: "", description: "" },
      { key: "", type: "string", required: false, defaultText: "", description: "" },
    ],
  });
  assert.deepEqual(calls.slice(2), [{ kind: "sync-schema" }, { kind: "sync-run" }]);
});

test("workflow app publish ui blocks publish when preflight is required and fails", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const statuses = [];
  let publishCalled = false;
  global.window = {
    aiwfDesktop: {
      publishWorkflowApp: async () => {
        publishCalled = true;
        return { ok: true };
      },
    },
  };

  try {
    const ui = createWorkflowAppPublishUi({
      appPublishName: { value: "Finance App" },
      publishRequirePreflight: { checked: true },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({ name: "workflow-a" }),
      runWorkflowPreflight: async () => ({
        ok: false,
        issues: [
          { level: "warning", message: "warn only" },
          { level: "error", message: "missing source" },
        ],
      }),
      collectAppSchemaFromForm: () => ({ title: { type: "string" } }),
    });

    await ui.publishApp();
  } finally {
    delete global.window;
  }

  assert.equal(publishCalled, false);
  assert.deepEqual(statuses, [{ text: "发布阻断：预检未通过 (missing source)", ok: false }]);
});

test("workflow app publish ui publishes app and refreshes list", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const statuses = [];
  const publishCalls = [];
  const renderCalls = [];
  global.window = {
    aiwfDesktop: {
      publishWorkflowApp: async (payload) => {
        publishCalls.push(payload);
        return { ok: true };
      },
      listWorkflowApps: async () => ({
        items: [{ app_id: "app_1", name: "Finance App" }],
      }),
    },
  };

  try {
    const ui = createWorkflowAppPublishUi({
      appPublishName: { value: "" },
      publishRequirePreflight: { checked: false },
      appSchemaJson: { value: "{\"region\":{\"type\":\"string\"}}" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({ name: "Finance App", nodes: [], edges: [] }),
      collectAppSchemaFromForm: () => ({}),
      normalizeAppSchemaObject: (obj) => ({ ...obj, normalized: { type: "boolean" } }),
      currentTemplateGovernance: () => ({ mode: "strict" }),
      parseRunParamsLoose: () => ({ region: "cn" }),
      getLastPreflightReport: () => ({ ok: true }),
      getLastTemplateAcceptanceReport: () => ({ accepted: true }),
      renderAppRows: (items) => renderCalls.push(items),
    });

    await ui.publishApp();
  } finally {
    delete global.window;
  }

  assert.equal(publishCalls.length, 1);
  assert.deepEqual(publishCalls[0], {
    name: "Finance App",
    graph: { name: "Finance App", nodes: [], edges: [] },
    params_schema: {
      region: { type: "string" },
      normalized: { type: "boolean" },
    },
    template_policy: {
      version: 1,
      governance: { mode: "strict" },
      runtime_defaults: { region: "cn" },
      latest_preflight: { ok: true },
      latest_template_acceptance: { accepted: true },
    },
  });
  assert.deepEqual(renderCalls, [[{ app_id: "app_1", name: "Finance App" }]]);
  assert.deepEqual(statuses, [{ text: "流程应用已发布", ok: true }]);
});

test("workflow app publish ui falls back to empty app rows when list fails", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const renderCalls = [];
  global.window = {
    aiwfDesktop: {
      listWorkflowApps: async () => {
        throw new Error("network down");
      },
    },
  };

  try {
    const ui = createWorkflowAppPublishUi({}, {
      renderAppRows: (items) => renderCalls.push(items),
    });

    await ui.refreshApps();
  } finally {
    delete global.window;
  }

  assert.deepEqual(renderCalls, [[]]);
});

test("workflow app publish ui formats structured remote publish failure", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      publishWorkflowApp: async () => ({
        ok: false,
        error: "workflow app graph node config invalid: workflow.nodes[0].config.manifest.command is required when workflow.nodes[0].config.op is register",
        error_items: [{ path: "workflow.nodes[0].config.manifest.command", code: "conditional_required", message: "workflow.nodes[0].config.manifest.command is required when workflow.nodes[0].config.op is register" }],
      }),
      listWorkflowApps: async () => ({ items: [] }),
    },
  };

  try {
    const ui = createWorkflowAppPublishUi({
      appPublishName: { value: "Finance App" },
      publishRequirePreflight: { checked: false },
      appSchemaJson: { value: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({ name: "Finance App", nodes: [], edges: [] }),
      collectAppSchemaFromForm: () => ({}),
      renderAppRows: () => {},
    });

    await ui.publishApp();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[conditional_required\] workflow\.nodes\[0\]\.config\.manifest\.command/);
});

test("workflow app publish ui formats structured local workflow contract failure", async () => {
  const { createWorkflowAppPublishUi } = await loadAppPublishUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      publishWorkflowApp: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_code: "workflow_contract_invalid",
        graph_contract: "contracts/workflow/workflow.schema.json",
        error_item_contract: "contracts/desktop/node_config_validation_errors.v1.json",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
      listWorkflowApps: async () => ({ items: [] }),
    },
  };

  try {
    const ui = createWorkflowAppPublishUi({
      appPublishName: { value: "Finance App" },
      publishRequirePreflight: { checked: false },
      appSchemaJson: { value: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({ name: "Finance App", version: "1.0.0", nodes: [], edges: [] }),
      collectAppSchemaFromForm: () => ({}),
      renderAppRows: () => {},
    });

    await ui.publishApp();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[required\] workflow\.version/);
});
