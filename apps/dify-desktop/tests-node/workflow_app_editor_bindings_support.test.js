const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadEditorSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-editor-bindings-support.js")).href;
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

test("workflow editor support syncs edge hints back into builder fields", async () => {
  const { bindEdgeConfigEditorActions } = await loadEditorSupportModule();
  const statuses = [];
  let syncCalls = 0;
  const els = {
    edgeFieldHintSelect: createEventTarget(),
    edgePathHintSelect: createEventTarget(),
    edgeWhenField: { value: "" },
    edgeWhenPath: { value: "" },
    btnBuildEdgeWhen: createEventTarget(),
  };
  els.edgeFieldHintSelect.value = "detail.ok";
  els.edgePathHintSelect.value = "output.status";

  bindEdgeConfigEditorActions({
    els,
    syncEdgeTextFromBuilder: () => { syncCalls += 1; },
    setStatus: (text, ok) => statuses.push({ text, ok }),
  });

  els.edgeFieldHintSelect.handlers.change();
  els.edgePathHintSelect.handlers.change();
  els.btnBuildEdgeWhen.handlers.click();

  assert.equal(els.edgeWhenField.value, "detail.ok");
  assert.equal(els.edgeWhenPath.value, "output.status");
  assert.equal(syncCalls, 3);
  assert.deepEqual(statuses, [{ text: "已从可视化构造器生成 JSON 条件", ok: true }]);
});

test("workflow editor support handles ctrl-wheel zoom and resize sync", async () => {
  const { bindCanvasEditorInteractions } = await loadEditorSupportModule();
  const zoomCalls = [];
  const windowTarget = createEventTarget();
  const canvasWrap = createEventTarget();

  bindCanvasEditorInteractions({
    els: { canvasWrap },
    canvas: { getZoom: () => 1 },
    setZoom: (zoom, point) => zoomCalls.push({ zoom, point }),
    syncCanvasPanels: () => zoomCalls.push({ resize: true }),
    handleCanvasDragOver: () => {},
    handleCanvasDrop: () => {},
    windowTarget,
  });

  let prevented = false;
  canvasWrap.handlers.wheel({
    ctrlKey: true,
    deltaY: -1,
    clientX: 12,
    clientY: 34,
    preventDefault() { prevented = true; },
  });
  windowTarget.handlers.resize();

  assert.equal(prevented, true);
  assert.deepEqual(zoomCalls[0], { zoom: 1.08, point: { clientX: 12, clientY: 34 } });
  assert.deepEqual(zoomCalls[1], { resize: true });
});
