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
  assert.match(statuses[0].text, /JSON/);
  assert.match(statuses[1].text, /flow-alpha\.json/);
});

test("workflow flow io ui treats saved_local workflow saves as partial success", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  let refreshCount = 0;
  global.window = {
    aiwfDesktop: {
      saveWorkflow: async () => ({
        ok: false,
        canceled: false,
        saved_local: true,
        path: "D:/flows/local-only.json",
        error: "workflow version record failed: backend down",
      }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi({
      workflowName: { value: "Local Flow" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({
        workflow_id: "wf_partial",
        version: "1.0.0",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      }),
      refreshVersions: async () => { refreshCount += 1; },
    });

    await ui.saveFlow();
  } finally {
    delete global.window;
  }

  assert.equal(refreshCount, 0);
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, true);
  assert.match(statuses[0].text, /local-only\.json/);
  assert.match(statuses[0].text, /backend down/);
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

test("workflow flow io ui formats structured ipc failures", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveWorkflow: async () => ({
        ok: false,
        canceled: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
      loadWorkflow: async () => ({
        ok: false,
        canceled: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
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
  assert.match(statuses[0].text, /\[required\] workflow\.version/);
  assert.match(statuses[1].text, /\[required\] workflow\.version/);
});

test("workflow flow io ui surfaces structured workflow contract error code on save", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveWorkflow: async () => ({
        ok: false,
        canceled: false,
        error: "workflow.version is required",
        error_code: "workflow_graph_invalid",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi({
      workflowName: { value: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({
        workflow_id: "wf_invalid",
        nodes: [{ id: "n1", type: "ingest_files" }],
        edges: [],
      }),
    });

    await ui.saveFlow();
  } finally {
    delete global.window;
  }

  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, false);
  assert.match(statuses[0].text, /\[required\] workflow\.version/);
});

test("workflow flow io ui surfaces structured workflow contract error code on load", async () => {
  const { createWorkflowFlowIoUi } = await loadFlowIoUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      loadWorkflow: async () => ({
        ok: true,
        path: "D:/flows/invalid.json",
        graph: {
          workflow_id: "wf_invalid_load",
          version: "1.0.0",
          nodes: [{ id: "n1", type: "ingest_files" }],
          edges: [],
        },
      }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi({
      workflowName: { value: "" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      migrateLoadedWorkflowGraph: (graph) => ({ migrated: false, graph, notes: [] }),
      applyLoadedWorkflowGraph: () => {
        const error = new Error("workflow contract invalid: workflow.version is required");
        error.code = "workflow_contract_invalid";
        error.details = {
          errors: ["workflow.version is required"],
          error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
        };
        throw error;
      },
    });

    await ui.loadFlow();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{ text: "加载失败: [required] workflow.version | 请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。", ok: false }]);
});
