const test = require("node:test");
const assert = require("node:assert/strict");

const { registerWorkflowStoreIpc } = require("../workflow_ipc_store");
const {
  LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
  createWorkflowIpcStateSupport,
} = require("../workflow_ipc_state");

function createHarness() {
  const handlers = {};
  const localTemplates = [];
  registerWorkflowStoreIpc({
    ipcMain: {
      handle(name, fn) {
        handlers[name] = fn;
      },
    },
    dialog: {},
    app: {},
    fs: {},
    path: {},
  }, {
    appendAudit: () => {},
    isMockIoAllowed: () => true,
    listLocalTemplates: () => localTemplates.slice(),
    listTemplateMarketplace: () => [],
    nowIso: () => "2026-05-27T00:00:00.000Z",
    qualityRuleSetSupport: {
      listQualityRuleSets: async () => ({ ok: true, sets: [] }),
      saveQualityRuleSet: async () => ({ ok: true }),
      removeQualityRuleSet: async () => ({ ok: true }),
    },
    resolveMockFilePath: (target) => ({ ok: true, path: target }),
    saveLocalTemplates: (items) => {
      localTemplates.splice(0, localTemplates.length, ...items);
    },
    saveTemplateMarketplace: () => {},
    workflowValidationSupport: {
      validateWorkflowDefinitionAuthoritatively: async ({ workflowDefinition }) => ({
        ok: true,
        normalized_workflow_definition: workflowDefinition,
        notes: [],
      }),
    },
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true }),
    },
  });
  return { handlers, localTemplates };
}

test("workflow ipc store lists shared local templates", async () => {
  const { handlers, localTemplates } = createHarness();
  localTemplates.push({
    schema_version: LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION,
    id: "custom_1",
    name: "Local Template",
    workflow_definition: {
      workflow_id: "wf_local",
      version: "1.0.0",
      nodes: [{ id: "n1", type: "ingest_files" }],
      edges: [],
    },
  });

  const out = await handlers["aiwf:listLocalTemplates"](null, { limit: 20 });

  assert.equal(out.ok, true);
  assert.equal(out.items.length, 1);
  assert.equal(out.items[0].id, "custom_1");
});

test("workflow ipc store saves shared local templates with canonical workflow_definition", async () => {
  const { handlers, localTemplates } = createHarness();

  const out = await handlers["aiwf:saveLocalTemplate"](null, {
    template: {
      id: "custom_2",
      name: "Saved Local Template",
      workflow_definition: {
        workflow_id: "wf_saved",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      },
      params_schema: {
        region: { type: "string" },
      },
      governance: {
        preflight_gate_required: true,
      },
      runtime_defaults: {
        region: "cn",
      },
    },
  });

  assert.equal(out.ok, true);
  assert.equal(localTemplates.length, 1);
  assert.equal(localTemplates[0].schema_version, LOCAL_TEMPLATE_ENTRY_SCHEMA_VERSION);
  assert.equal(localTemplates[0].workflow_definition.workflow_id, "wf_saved");
  assert.equal(localTemplates[0].graph, undefined);
});
