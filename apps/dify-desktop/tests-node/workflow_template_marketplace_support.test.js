const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateMarketplaceModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-ui-marketplace.js")).href;
  return import(file);
}

async function loadTemplateStorageContractModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-storage-contract.js")).href;
  return import(file);
}

function templateGraph() {
  return {
    workflow_id: "wf_local_template",
    version: "1.0.0",
    name: "Local Template",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

test("workflow template marketplace support migrates legacy local template storage on load", async () => {
  const { createWorkflowTemplateMarketplaceSupport } = await loadTemplateMarketplaceModule();
  const {
    LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
  } = await loadTemplateStorageContractModule();

  const localStorageState = {
    "aiwf.workflow.templates.v1": JSON.stringify([{
      id: "custom_1",
      name: "Legacy Local Template",
      graph: templateGraph(),
    }]),
  };

  global.window = {
    localStorage: {
      getItem: (key) => localStorageState[key] || null,
      setItem: (key, value) => {
        localStorageState[key] = value;
      },
    },
  };

  try {
    const support = createWorkflowTemplateMarketplaceSupport({}, {});
    const items = support.loadLocalTemplates();

    assert.equal(items.length, 1);
    assert.equal(items[0].schema_version, LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
    const stored = JSON.parse(localStorageState["aiwf.workflow.templates.v1"]);
    assert.equal(stored.schema_version, LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION);
    assert.equal(stored.items[0].schema_version, LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
    assert.deepEqual(stored.items[0].workflow_definition, templateGraph());
    assert.equal(Object.prototype.hasOwnProperty.call(stored.items[0], "graph"), false);
  } finally {
    delete global.window;
  }
});

test("workflow template marketplace support saves current template using versioned local storage envelope", async () => {
  const { createWorkflowTemplateMarketplaceSupport } = await loadTemplateMarketplaceModule();
  const {
    LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION,
  } = await loadTemplateStorageContractModule();

  const localStorageState = {};
  global.window = {
    localStorage: {
      getItem: (key) => localStorageState[key] || null,
      setItem: (key, value) => {
        localStorageState[key] = value;
      },
    },
  };
  global.prompt = () => "My Local Template";

  try {
    const statuses = [];
    const support = createWorkflowTemplateMarketplaceSupport({
      workflowName: { value: "My Flow" },
      templateSelect: { value: "" },
    }, {
      graphPayload: () => templateGraph(),
      currentTemplateGovernance: () => ({ preflight_gate_required: true }),
      parseRunParamsLoose: () => ({ region: "cn" }),
      renderTemplateSelect: () => {},
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });

    support.saveCurrentAsTemplate();

    const stored = JSON.parse(localStorageState["aiwf.workflow.templates.v1"]);
    assert.equal(stored.schema_version, LOCAL_TEMPLATE_STORAGE_SCHEMA_VERSION);
    assert.equal(stored.items.length, 1);
    assert.equal(stored.items[0].schema_version, LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
    assert.equal(stored.items[0].name, "My Local Template");
    assert.deepEqual(stored.items[0].workflow_definition, templateGraph());
    assert.equal(Object.prototype.hasOwnProperty.call(stored.items[0], "graph"), false);
    assert.deepEqual(stored.items[0].runtime_defaults, { region: "cn" });
    assert.deepEqual(stored.items[0].governance, { preflight_gate_required: true });
    assert.equal(statuses.length, 1);
    assert.equal(statuses[0].ok, true);
  } finally {
    delete global.window;
    delete global.prompt;
  }
});
