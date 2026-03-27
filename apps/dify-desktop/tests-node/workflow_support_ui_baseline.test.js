const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadSupportUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/support-ui.js")).href;
  return import(file);
}

test("workflow support ui saves current run as baseline", async () => {
  const { createWorkflowSupportUi } = await loadSupportUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveRunBaseline: async (payload) => {
        assert.deepEqual(payload, {
          run_id: "run_a_12345678",
          name: "baseline_run_a_12",
        });
        return { ok: true, item: { baseline_id: "base_1" } };
      },
    },
  };

  try {
    const ui = createWorkflowSupportUi({
      compareRunA: { value: "run_a_12345678" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.saveCurrentRunAsBaseline();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{ text: "基线已保存: base_1", ok: true }]);
});

test("workflow support ui compares latest baseline and loads lineage for run A", async () => {
  const { createWorkflowSupportUi } = await loadSupportUiModule();
  const statuses = [];
  const log = { textContent: "" };
  global.window = {
    aiwfDesktop: {
      listRunBaselines: async () => ({ items: [{ baseline_id: "base_2" }] }),
      compareRunWithBaseline: async (payload) => {
        assert.deepEqual(payload, { run_id: "run_b_123", baseline_id: "base_2" });
        return {
          ok: true,
          regression: { changed_nodes: 3, status_flip_nodes: 1, perf_hot_nodes: 2 },
        };
      },
      getWorkflowLineage: async (payload) => {
        assert.deepEqual(payload, { run_id: "run_a_123" });
        return {
          ok: true,
          lineage: {
            nodes: [{ id: "n1" }, { id: "n2" }],
            edges: [{ from: "n1", to: "n2" }],
          },
        };
      },
    },
  };

  try {
    const ui = createWorkflowSupportUi({
      compareRunA: { value: "run_a_123" },
      compareRunB: { value: "run_b_123" },
      log,
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.compareWithLatestBaseline();
    await ui.loadLineageForRunA();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [
    { text: "基线对比完成: changed=3, status_flip=1, perf_hot=2", ok: true },
    { text: "血缘已加载: nodes=2, edges=1", ok: true },
  ]);
  assert.match(log.textContent, /"run_id": "run_a_123"/);
});

test("workflow support ui guards when baseline or lineage inputs are missing", async () => {
  const { createWorkflowSupportUi } = await loadSupportUiModule();
  const statuses = [];
  const ui = createWorkflowSupportUi({
    compareRunA: { value: "" },
    compareRunB: { value: "" },
    log: { textContent: "" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  await ui.saveCurrentRunAsBaseline();
  await ui.compareWithLatestBaseline();
  await ui.loadLineageForRunA();

  assert.deepEqual(statuses, [
    { text: "请先在“运行对比”里选择 Run A 作为基线", ok: false },
    { text: "请先在“运行对比”里选择 Run B", ok: false },
    { text: "请先在“运行对比”里选择 Run A", ok: false },
  ]);
});

test("workflow support ui formats structured baseline failure", async () => {
  const { createWorkflowSupportUi } = await loadSupportUiModule();
  const statuses = [];
  global.window = {
    aiwfDesktop: {
      saveRunBaseline: async () => ({
        ok: false,
        error: "workflow contract invalid: workflow.version is required",
        error_items: [{ path: "workflow.version", code: "required", message: "workflow.version is required" }],
      }),
    },
  };

  try {
    const ui = createWorkflowSupportUi({
      compareRunA: { value: "run_a_12345678" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
    });
    await ui.saveCurrentRunAsBaseline();
  } finally {
    delete global.window;
  }

  assert.deepEqual(statuses, [{
    text: "保存基线失败: [required] workflow.version | 请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。",
    ok: false,
  }]);
});
