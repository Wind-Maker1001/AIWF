const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPaletteUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/palette-ui.js")).href;
  return import(file);
}

function createFakeElement(tag) {
  return {
    tag,
    className: "",
    draggable: false,
    dataset: {},
    style: {},
    textContent: "",
    children: [],
    listeners: {},
    appendChild(child) {
      this.children.push(child);
    },
    addEventListener(name, handler) {
      this.listeners[name] = handler;
    },
  };
}

function createPaletteStub() {
  return {
    innerHTML: "",
    children: [],
    appendChild(child) {
      this.children.push(child);
    },
    querySelectorAll(selector) {
      if (selector !== ".palette-item") return [];
      return this.children.filter((child) => child.className === "palette-item");
    },
  };
}

test("workflow palette ui adds ds_refine using deepseek defaults", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const statuses = [];
  const createCalls = [];
  const selected = [];
  const ui = createWorkflowPaletteUi({
    nodeType: { value: "ds_refine" },
    aiEndpoint: { value: "" },
    aiKey: { value: "sk-demo" },
    aiModel: { value: "" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    defaultNodeConfigFn: () => ({ base: true }),
    createNode: (type, x, y, config) => {
      createCalls.push({ type, x, y, config });
      return "node_1";
    },
    selectNodeIds: (ids) => selected.push(ids),
    renderAll: () => statuses.push({ text: "rendered", ok: true }),
  });

  ui.handleAddNode();

  assert.deepEqual(createCalls, [{
    type: "ai_refine",
    x: 60,
    y: 60,
    config: {
      base: true,
      reuse_existing: false,
      provider_name: "DeepSeek",
      ai_endpoint: "https://api.deepseek.com/v1/chat/completions",
      ai_api_key: "sk-demo",
      ai_model: "deepseek-chat",
    },
  }]);
  assert.deepEqual(selected, [["node_1"]]);
  assert.deepEqual(statuses.slice(-1), [{ text: "已添加节点: DS提炼", ok: true }]);
});

test("workflow palette ui renders filtered palette and click selects node type", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const palette = createPaletteStub();
  const nodeType = { value: "" };
  const ui = createWorkflowPaletteUi({
    palette,
    paletteMode: { value: "simple" },
    paletteSearch: { value: "sql" },
    nodeType,
  }, {
    nodeCatalog: [
      { type: "sql_chart_v1", name: "SQL Chart", desc: "chart" },
      { type: "vector_index_v2_search", name: "Vector Search", desc: "search" },
    ],
    createElement: createFakeElement,
  });

  ui.renderPalette();

  assert.equal(palette.children.length, 1);
  const item = palette.children[0];
  assert.equal(item.dataset.nodeType, "sql_chart_v1");
  item.listeners.click();
  assert.equal(nodeType.value, "sql_chart_v1");
});

test("workflow palette ui handles canvas drop with snapped coordinates", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const createCalls = [];
  const statuses = [];
  const ui = createWorkflowPaletteUi({
    aiEndpoint: { value: "https://example.com" },
    aiKey: { value: "token" },
    aiModel: { value: "deepseek-chat" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    defaultNodeConfigFn: () => ({}),
    createNode: (type, x, y, config) => {
      createCalls.push({ type, x, y, config });
      return "node_drop";
    },
    selectNodeIds: () => {},
    renderAll: () => {},
    computeDropPosition: () => ({ x: 120, y: 144 }),
  });

  ui.handleCanvasDrop({
    preventDefault() {},
    dataTransfer: {
      getData: () => "ds_refine",
    },
  });

  assert.deepEqual(createCalls, [{
    type: "ai_refine",
    x: 120,
    y: 144,
    config: {
      reuse_existing: false,
      provider_name: "DeepSeek",
      ai_endpoint: "https://example.com",
      ai_api_key: "token",
      ai_model: "deepseek-chat",
    },
  }]);
  assert.deepEqual(statuses, [{ text: "已拖入节点: DS提炼", ok: true }]);
});
