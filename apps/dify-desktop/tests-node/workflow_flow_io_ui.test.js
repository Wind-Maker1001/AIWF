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
        assert.deepEqual(graph, { workflow_id: "wf_1", nodes: [], edges: [] });
        assert.equal(name, "Flow Alpha");
        return { ok: true, path: "D:/flows/flow-alpha.json" };
      },
    },
  };

  try {
    const ui = createWorkflowFlowIoUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      graphPayload: () => ({ workflow_id: "wf_1", nodes: [], edges: [] }),
      refreshVersions: async () => { refreshCount += 1; },
    });

    ui.exportJson();
    await ui.saveFlow();
  } finally {
    delete global.window;
  }

  assert.match(els.log.textContent, /"workflow_id": "wf_1"/);
  assert.equal(refreshCount, 1);
  assert.deepEqual(statuses, [
    { text: "已导出流程 JSON 到右侧日志区", ok: true },
    { text: "流程已保存: D:/flows/flow-alpha.json", ok: true },
  ]);
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
        graph: { name: "Imported" },
      }),
    },
  };

  try {
    const ui = createWorkflowFlowIoUi(els, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      migrateLoadedWorkflowGraph: (graph) => ({
        migrated: true,
        notes: ["v2", "sanitized"],
        graph: { ...graph, name: "Imported Flow" },
      }),
      applyLoadedWorkflowGraph: (graph) => applied.push(graph),
      getLoadedWorkflowName: () => "Imported Flow",
      renderMigrationReport: (report) => reports.push(report),
    });

    await ui.loadFlow();
  } finally {
    delete global.window;
  }

  assert.deepEqual(applied, [{ name: "Imported Flow" }]);
  assert.equal(els.workflowName.value, "Imported Flow");
  assert.equal(reports.length, 1);
  assert.deepEqual(statuses, [
    { text: "流程已加载并迁移: D:/flows/imported.json (v2, sanitized)", ok: true },
  ]);
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
      graphPayload: () => ({ nodes: [], edges: [] }),
    });

    await ui.saveFlow();
    await ui.loadFlow();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [
    { text: "保存失败: Error: disk full", ok: false },
    { text: "加载失败: denied", ok: false },
  ]);
});
