const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadFlowIoUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/flow-io-ui.js")).href;
  return import(file);
}

test("workflow flow io ui exports json and saves flow", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  let refreshCount = 0;
  const els = {
    log: { textContent: "" },
    workflowName: { value: "Flow Alpha" },
  };
  global.window = {
    aiwfDesktop: {
      saveWorkflow: async (graph, name) => {
        assert.deepEqual(graph, {
          workflow_id: "wf_1",
          version: "1.0.0",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
        });
        assert.equal(name, "Flow Alpha");
        return { ok: true, path: "D:/flows/flow-alpha.json" };
      },
    },
  };

  try {
    const ui = createWorkflowFlowIoUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({
        workflow_id: "wf_1",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      }),
      refreshVersions: async () => { refreshCount += 1; },
    });

    ui.exportJson();
    await ui.saveFlow();
  } finally {
    delete global.window;
  }

  assert.match(els.log.textContent, /"workflow_id": "wf_1"/);
  assert.equal(refreshCount, 1);
  assert.equal(statuses.length, 2);
  assert.equal(statuses[0].ok, true);
  assert.equal(statuses[1].ok, true);
  assert.match(statuses[1].text, /flow-alpha\.json/);
});

test("workflow flow io ui loads migrated flow and updates ui state", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  const reports = [];
  const applied = [];
  const els = {
    workflowName: { value: "" },
  };
  global.window = {
    aiwfDesktop: {
      loadWorkflow: async () => ({
        ok: true,
        path: "D:/flows/imported.json",
        graph: {
          workflow_id: "wf_imported",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
          name: "Imported",
        },
      }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      migrateLoadedWorkflowGraph: (graph) => ({
        migrated: true,
        notes: ["v2", "sanitized"],
        graph: { ...graph, version: "1.0.0", name: "Imported Flow" },
      }),
      applyLoadedWorkflowGraph: (graph) => {
        applied.push(graph);
        return { contract: { migrated: false, notes: [], errors: [] } };
      },
      getLoadedWorkflowName: () => "Imported Flow",
      renderMigrationReport: (report) => reports.push(report),
    });

    await ui.loadFlow();
  } finally {
    delete global.window;
  }

  assert.deepEqual(applied, [{
    workflow_id: "wf_imported",
    nodes: [{ id: "n1", type: "ingest_files" }],
    edges: [],
    name: "Imported Flow",
    version: "1.0.0",
  }]);
  assert.equal(els.workflowName.value, "Imported Flow");
  assert.equal(reports.length, 1);
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, true);
  assert.match(statuses[0].text, /imported\.json/);
  assert.match(statuses[0].text, /v2/);
});

test("workflow flow io ui handles save/load failures and cancellations", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveWorkflow: async () => {
        throw new Error("disk full");
      },
      loadWorkflow: async () => ({ ok: false, canceled: false, error: "denied" }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi({
      workflowName: { value: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({
        workflow_id: "wf_err",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      }),
    });

    await ui.saveFlow();
    await ui.loadFlow();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 2);
  assert.equal(statuses[0].ok, false);
  assert.equal(statuses[1].ok, false);
  assert.match(statuses[0].text, /disk full/);
  assert.match(statuses[1].text, /denied/);
});
