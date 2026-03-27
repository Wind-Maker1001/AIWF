const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const { registerWorkflowStoreIpc } = require("../workflow_ipc_store");
const { TEMPLATE_PACK_ENTRY_SCHEMA_VERSION } = require("../workflow_ipc_state");
const { TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION } = require("../workflow_template_pack_contract");

function templateGraph() {
  return {
    workflow_id: "wf_template_pack",
    version: "1.0.0",
    name: "Template Graph",
    nodes: [{ id: "n1", type: "ingest_files", config: {} }],
    edges: [],
  };
}

function createIpcHarness() {
  const handlers = {};
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-workflow-ipc-store-"));
  const marketplace = [];
  const audits = [];
  registerWorkflowStoreIpc({
    ipcMain: {
      handle(name, fn) {
        handlers[name] = fn;
      },
    },
    dialog: {
      async showSaveDialog() {
        return { canceled: true, filePath: "" };
      },
    },
    app: {
      getPath(name) {
        if (name === "documents") return root;
        return root;
      },
    },
    fs,
    path,
  }, {
    appendAudit: (action, detail) => audits.push({ action, detail }),
    isMockIoAllowed: () => true,
    listTemplateMarketplace: () => marketplace.slice(),
    nowIso: () => "2026-03-24T00:00:00.000Z",
    qualityRuleSetSupport: {
      listQualityRuleSets: async () => ({ ok: true, sets: [] }),
      saveQualityRuleSet: async () => ({ ok: true }),
      removeQualityRuleSet: async () => ({ ok: true }),
    },
    resolveMockFilePath: (target) => ({ ok: true, path: target }),
    saveTemplateMarketplace: (items) => {
      marketplace.splice(0, marketplace.length, ...items);
    },
    workflowVersionStore: {
      recordVersion: async () => ({ ok: true }),
    },
  });
  return { handlers, root, marketplace, audits };
}

test("workflow ipc store migrates legacy template pack artifact on install", async () => {
  const { handlers, marketplace, audits } = createIpcHarness();
  const install = handlers["aiwf:installTemplatePack"];
  assert.equal(typeof install, "function");

  const out = await install(null, {
    pack: {
      id: "pack_1",
      name: "Finance Pack",
      templates: [{
        id: "tpl_1",
        name: "Finance Template",
        graph: templateGraph(),
      }],
    },
  });

  assert.equal(out.ok, true);
  assert.equal(out.migrated, true);
  assert.equal(out.item.schema_version, TEMPLATE_PACK_ENTRY_SCHEMA_VERSION);
  assert.equal(marketplace.length, 1);
  assert.equal(marketplace[0].schema_version, TEMPLATE_PACK_ENTRY_SCHEMA_VERSION);
  assert.equal(marketplace[0].templates.length, 1);
  assert.match(audits[0].action, /template_pack_install/);
  assert.equal(audits[0].detail.migrated, true);
});

test("workflow ipc store exports template pack as artifact schema", async () => {
  const { handlers, root, marketplace } = createIpcHarness();
  marketplace.push({
    schema_version: TEMPLATE_PACK_ENTRY_SCHEMA_VERSION,
    id: "pack_1",
    name: "Finance Pack",
    version: "v1",
    source: "inline",
    created_at: "2026-03-24T00:00:00.000Z",
    templates: [{
      id: "tpl_1",
      name: "Finance Template",
      graph: templateGraph(),
      template_spec_version: 1,
    }],
  });
  const exportPack = handlers["aiwf:exportTemplatePack"];
  assert.equal(typeof exportPack, "function");

  const exportPath = path.join(root, "template_pack_export.json");
  const out = await exportPack(null, {
    id: "pack_1",
    mock: true,
    path: exportPath,
  });

  assert.equal(out.ok, true);
  const payload = JSON.parse(fs.readFileSync(exportPath, "utf8"));
  assert.equal(payload.schema_version, TEMPLATE_PACK_ARTIFACT_SCHEMA_VERSION);
  assert.equal(payload.id, "pack_1");
  assert.equal(payload.templates.length, 1);
  assert.equal(payload.templates[0].id, "tpl_1");
});

test("workflow ipc store saveWorkflow returns structured workflow contract failure", async () => {
  const { handlers, root } = createIpcHarness();
  const saveWorkflow = handlers["aiwf:saveWorkflow"];
  assert.equal(typeof saveWorkflow, "function");

  const out = await saveWorkflow(null, {
    workflow_id: "wf_invalid",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
  }, "Invalid Workflow", {
    mock: true,
    path: path.join(root, "invalid_workflow.json"),
  });

  assert.equal(out.ok, false);
  assert.equal(out.canceled, false);
  assert.equal(out.error_code, "workflow_contract_invalid");
  assert.equal(out.graph_contract, "contracts/workflow/workflow.schema.json");
  assert.equal(out.error_item_contract, "contracts/desktop/node_config_validation_errors.v1.json");
  assert.ok(Array.isArray(out.error_items));
  assert.ok(out.error_items.some((item) => item.path === "workflow.version" && item.code === "required"));
});

test("workflow ipc store saveWorkflow rejects unregistered node types", async () => {
  const { handlers, root } = createIpcHarness();
  const saveWorkflow = handlers["aiwf:saveWorkflow"];
  assert.equal(typeof saveWorkflow, "function");

  const out = await saveWorkflow(null, {
    workflow_id: "wf_unknown_type",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "unknown_future_node" }],
    edges: [],
  }, "Unknown Node Workflow", {
    mock: true,
    path: path.join(root, "unknown_type_workflow.json"),
  });

  assert.equal(out.ok, false);
  assert.equal(out.error_code, "workflow_contract_invalid");
  assert.ok(Array.isArray(out.error_items));
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes" && item.code === "unknown_node_type"));
});

test("workflow ipc store loadWorkflow returns structured invalid json failure", async () => {
  const { handlers, root } = createIpcHarness();
  const loadWorkflow = handlers["aiwf:loadWorkflow"];
  assert.equal(typeof loadWorkflow, "function");

  const badJsonPath = path.join(root, "broken_workflow.json");
  fs.writeFileSync(badJsonPath, "{ invalid", "utf8");

  const out = await loadWorkflow(null, {
    mock: true,
    path: badJsonPath,
  });

  assert.equal(out.ok, false);
  assert.equal(out.canceled, false);
  assert.equal(out.error_code, "workflow_load_invalid_json");
  assert.match(String(out.error || ""), /json|position|unexpected/i);
});

test("workflow ipc store loadWorkflow rejects unregistered node types with structured contract failure", async () => {
  const { handlers, root, audits } = createIpcHarness();
  const loadWorkflow = handlers["aiwf:loadWorkflow"];
  assert.equal(typeof loadWorkflow, "function");

  const invalidWorkflowPath = path.join(root, "unknown_type_workflow.json");
  fs.writeFileSync(invalidWorkflowPath, `${JSON.stringify({
    workflow_id: "wf_unknown_type",
    version: "1.0.0",
    nodes: [{ id: "n1", type: "unknown_future_node" }],
    edges: [],
  }, null, 2)}\n`, "utf8");

  const out = await loadWorkflow(null, {
    mock: true,
    path: invalidWorkflowPath,
  });

  assert.equal(out.ok, false);
  assert.equal(out.canceled, false);
  assert.equal(out.error_code, "workflow_contract_invalid");
  assert.ok(Array.isArray(out.error_items));
  assert.ok(out.error_items.some((item) => item.path === "workflow.nodes" && item.code === "unknown_node_type"));
  assert.equal(audits.some((entry) => entry.action === "workflow_load"), false);
});

test("workflow ipc store loadWorkflow still accepts legacy graphs missing version", async () => {
  const { handlers, root } = createIpcHarness();
  const loadWorkflow = handlers["aiwf:loadWorkflow"];
  assert.equal(typeof loadWorkflow, "function");

  const legacyWorkflowPath = path.join(root, "legacy_missing_version.json");
  fs.writeFileSync(legacyWorkflowPath, `${JSON.stringify({
    workflow_id: "wf_legacy",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
  }, null, 2)}\n`, "utf8");

  const out = await loadWorkflow(null, {
    mock: true,
    path: legacyWorkflowPath,
  });

  assert.equal(out.ok, true);
  assert.equal(out.graph.workflow_id, "wf_legacy");
  assert.equal("version" in out.graph, false);
});

test("workflow ipc store loadWorkflow can skip graph contract validation for non-workflow json pickers", async () => {
  const { handlers, root } = createIpcHarness();
  const loadWorkflow = handlers["aiwf:loadWorkflow"];
  assert.equal(typeof loadWorkflow, "function");

  const templatePackPath = path.join(root, "template_pack.json");
  fs.writeFileSync(templatePackPath, `${JSON.stringify({
    schema_version: "template_pack_artifact.v1",
    id: "pack_1",
    name: "Pack One",
    templates: [],
  }, null, 2)}\n`, "utf8");

  const out = await loadWorkflow(null, {
    mock: true,
    path: templatePackPath,
    validateGraphContract: false,
  });

  assert.equal(out.ok, true);
  assert.equal(out.path, templatePackPath);
  assert.equal(out.graph.schema_version, "template_pack_artifact.v1");
});
