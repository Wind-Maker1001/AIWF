const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateParamSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-ui-params.js")).href;
  return import(file);
}

function templateGraph() {
  return {
    workflow_id: "wf_template_apply",
    version: "1.0.0",
    name: "Template Apply",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

test("workflow template param support validates template workflow through authoritative IPC before import", async () => {
  const { createWorkflowTemplateParamSupport } = await loadTemplateParamSupportModule();
  const statuses = [];
  const imported = [];

  const support = createWorkflowTemplateParamSupport({
    templateSelect: { value: "tpl_1" },
    templateParams: { value: "{}" },
    templateParamsForm: null,
    workflowName: { value: "" },
    publishRequirePreflight: { checked: true },
    appRunParams: { value: "" },
  }, {
    allTemplates: () => [{
      id: "tpl_1",
      name: "Template One",
      workflow_definition: templateGraph(),
      params_schema: {},
      governance: { preflight_gate_required: true },
      runtime_defaults: { region: "cn" },
    }],
    setStatus: (text, ok) => statuses.push({ text, ok }),
    renderAll: () => {},
    renderMigrationReport: () => {},
    store: {
      state: { graph: { name: "Imported Template" } },
      importGraph: (graph) => {
        imported.push(graph);
        return { contract: { migrated: false, notes: [], errors: [] } };
      },
    },
    syncRunParamsFormFromJson: () => {},
    clearSelectedEdge: () => {},
    validateWorkflowDefinition: async (workflowDefinition) => ({
      ok: true,
      workflow_definition: { ...workflowDefinition, version: "1.0.0" },
      notes: ["workflow.version migrated to 1.0.0"],
    }),
  });

  await support.applySelectedTemplate();

  assert.equal(imported.length, 1);
  assert.equal(imported[0].workflow_id, "wf_template_apply");
  assert.equal(imported[0].version, "1.0.0");
  assert.equal(statuses.at(-1).ok, true);
});

test("workflow template param support fails closed on authoritative validation error", async () => {
  const { createWorkflowTemplateParamSupport } = await loadTemplateParamSupportModule();
  const statuses = [];

  const support = createWorkflowTemplateParamSupport({
    templateSelect: { value: "tpl_1" },
    templateParams: { value: "{}" },
    templateParamsForm: null,
    workflowName: { value: "" },
  }, {
    allTemplates: () => [{
      id: "tpl_1",
      name: "Template One",
      workflow_definition: templateGraph(),
      params_schema: {},
    }],
    setStatus: (text, ok) => statuses.push({ text, ok }),
    renderAll: () => {},
    renderMigrationReport: () => {},
    store: {
      state: { graph: { name: "" } },
      importGraph: () => {
        throw new Error("should not import invalid template");
      },
    },
    syncRunParamsFormFromJson: () => {},
    clearSelectedEdge: () => {},
    validateWorkflowDefinition: async () => ({
      ok: false,
      error: "workflow contains unregistered node types: unknown_future_node",
      error_code: "workflow_graph_invalid",
    }),
  });

  await support.applySelectedTemplate();

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /unknown_future_node/i);
});

test("workflow template param support fails closed on authoritative validation exception", async () => {
  const { createWorkflowTemplateParamSupport } = await loadTemplateParamSupportModule();
  const statuses = [];

  const support = createWorkflowTemplateParamSupport({
    templateSelect: { value: "tpl_1" },
    templateParams: { value: "{}" },
    templateParamsForm: null,
    workflowName: { value: "" },
  }, {
    allTemplates: () => [{
      id: "tpl_1",
      name: "Template One",
      workflow_definition: templateGraph(),
      params_schema: {},
    }],
    setStatus: (text, ok) => statuses.push({ text, ok }),
    renderAll: () => {},
    renderMigrationReport: () => {},
    store: {
      state: { graph: { name: "" } },
      importGraph: () => {
        throw new Error("should not import invalid template");
      },
    },
    syncRunParamsFormFromJson: () => {},
    clearSelectedEdge: () => {},
    validateWorkflowDefinition: async () => {
      throw new Error("workflow validation unavailable: connection refused");
    },
  });

  await support.applySelectedTemplate();

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /workflow validation unavailable/i);
});
