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
