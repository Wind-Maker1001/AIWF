const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadVersionCacheUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/version-cache-ui.js")).href;
  return import(file);
}

test("workflow version cache ui blocks compare when version ids are missing", async () => {
  const { createWorkflowVersionCacheUi } = await loadVersionCacheUiModule();
  const statuses = [];
  const ui = createWorkflowVersionCacheUi({
    versionCompareA: { value: "" },
    versionCompareB: { value: "ver_b" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  await ui.compareVersions();

  assert.deepEqual(statuses, [{ text: "请填写版本 A/B", ok: false }]);
});

test("workflow version cache ui refreshes versions and compares selected versions", async () => {
  const { createWorkflowVersionCacheUi } = await loadVersionCacheUiModule();
  const statuses = [];
  const renderCalls = [];
  global.window = {
    aiwfDesktop: {
      listWorkflowVersions: async (payload) => {
        assert.deepEqual(payload, { limit: 120 });
        return { items: [{ version_id: "ver_a", workflow_name: "wf-a" }] };
      },
      compareWorkflowVersions: async (payload) => {
        assert.deepEqual(payload, { version_a: "ver_a", version_b: "ver_b" });
        return { ok: true, summary: { version_a: "ver_a", version_b: "ver_b" }, node_diff: [] };
      },
    },
  };

  try {
    const ui = createWorkflowVersionCacheUi({
      versionCompareA: { value: " ver_a " },
      versionCompareB: { value: "ver_b" },
    }, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      renderVersionRows: (items) => renderCalls.push({ kind: "rows", items }),
      renderVersionCompare: (out) => renderCalls.push({ kind: "compare", out }),
    });

    await ui.refreshVersions();
    await ui.compareVersions();
  } finally {
    delete global.window;
  }

  assert.deepEqual(renderCalls[0], {
    kind: "rows",
    items: [{ version_id: "ver_a", workflow_name: "wf-a" }],
  });
  assert.equal(renderCalls[1].kind, "compare");
  assert.equal(renderCalls[1].out?.ok, true);
  assert.deepEqual(statuses, [{ text: "版本对比完成", ok: true }]);
});

test("workflow version cache ui refreshes and clears cache with fallback behavior", async () => {
  const { createWorkflowVersionCacheUi } = await loadVersionCacheUiModule();
  const statuses = [];
  const cacheCalls = [];
  global.window = {
    aiwfDesktop: {
      getWorkflowNodeCacheStats: async () => {
        throw new Error("cache stats unavailable");
      },
      clearWorkflowNodeCache: async () => ({
        ok: false,
        error: "locked",
        stats: { entries: 3, hits: 8, misses: 2, hit_rate: 0.8 },
      }),
    },
  };

  try {
    const ui = createWorkflowVersionCacheUi({}, {
      setStatus: (text, ok) => statuses.push({ text, ok }),
      renderCacheStats: (stats) => cacheCalls.push(stats),
    });

    await ui.refreshCacheStats();
    await ui.clearCache();
  } finally {
    delete global.window;
  }

  assert.deepEqual(cacheCalls, [
    {},
    { entries: 3, hits: 8, misses: 2, hit_rate: 0.8 },
  ]);
  assert.deepEqual(statuses, [{ text: "清空缓存失败: locked", ok: false }]);
});
