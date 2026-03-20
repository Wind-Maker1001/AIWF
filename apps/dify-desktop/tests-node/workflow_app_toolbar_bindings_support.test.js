const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadToolbarSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-toolbar-bindings-support.js")).href;
  return import(file);
}

function createEventTarget() {
  const handlers = {};
  return {
    handlers,
    addEventListener(eventName, handler) {
      handlers[eventName] = handler;
    },
  };
}

test("workflow toolbar support rerenders compare result filters with fallback", async () => {
  const {
    bindWorkflowTemplateToolbarActions,
    compareFallback,
  } = await loadToolbarSupportModule();

  assert.deepEqual(compareFallback(() => null), { ok: false, error: "请先执行对比" });

  const calls = [];
  const compareOnlyChanged = createEventTarget();
  const compareOnlyStatusChanged = createEventTarget();
  const compareMinDelta = createEventTarget();

  bindWorkflowTemplateToolbarActions({
    els: {
      compareOnlyChanged,
      compareOnlyStatusChanged,
      compareMinDelta,
    },
    renderCompareResult: (out) => calls.push(out),
    getLastCompareResult: () => ({ ok: true, summary: { changed_nodes: 2 } }),
  });

  compareOnlyChanged.handlers.change();
  compareOnlyStatusChanged.handlers.change();
  compareMinDelta.handlers.change();

  assert.equal(calls.length, 3);
  assert.deepEqual(calls[0], { ok: true, summary: { changed_nodes: 2 } });
});
