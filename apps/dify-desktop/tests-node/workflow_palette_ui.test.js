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
    querySelectorAll(selector) {
      const matches = [];
      const visit = (node) => {
        if (!node || !Array.isArray(node.children)) return;
        node.children.forEach((child) => {
          if (selector === ".palette-item" && child.className === "palette-item") {
            matches.push(child);
          }
          if (selector === ".palette-group-title" && child.className === "palette-group-title") {
            matches.push(child);
          }
          visit(child);
        });
      };
      visit(this);
      return matches;
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
      const matches = [];
      const visit = (node) => {
        if (!node || !Array.isArray(node.children)) return;
        node.children.forEach((child) => {
          if (selector === ".palette-item" && child.className === "palette-item") {
            matches.push(child);
          }
          if (selector === ".palette-group-title" && child.className === "palette-group-title") {
            matches.push(child);
          }
          visit(child);
        });
      };
      visit(this);
      return matches;
    },
  };
}

function createInputStub(value = "") {
  return {
    value,
    dataset: {},
    title: "",
    ariaInvalid: "",
    setAttribute(name, nextValue) {
      if (name === "aria-invalid") this.ariaInvalid = nextValue;
      else this[name] = nextValue;
    },
  };
}

test("workflow palette ui adds ds_refine using deepseek defaults", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const statuses = [];
  const createCalls = [];
  const selected = [];
  const ui = createWorkflowPaletteUi({
    nodeType: createInputStub("ds_refine"),
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
    aiEndpoint: { value: "" },
    aiKey: { value: "sk-demo" },
    aiModel: { value: "" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    nodeCatalog: [
      { type: "ds_refine", name: "DS提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
      { type: "ai_refine", name: "AI 提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    ],
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
  assert.equal(statuses.at(-1)?.ok, true);
  assert.match(statuses.at(-1)?.text || "", /已添加节点:/);
  assert.match(statuses.at(-1)?.text || "", /DS提炼/);
  assert.match(statuses.at(-1)?.text || "", /local_ai/);
  assert.match(statuses.at(-1)?.text || "", /local_policy/);
});

test("workflow palette ui renders filtered palette and click selects node type", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const palette = createPaletteStub();
  const nodeType = createInputStub("");
  const statuses = [];
  const btnAdd = { disabled: false };
  const ui = createWorkflowPaletteUi({
    palette,
    paletteMode: { value: "simple" },
    paletteSearch: { value: "sql" },
    nodeType,
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
    btnAdd,
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    nodeCatalog: [
      { type: "sql_chart_v1", name: "SQL Chart", desc: "chart", group: "未分组", policy_section: "" },
      { type: "vector_index_v2_search", name: "Vector Search", desc: "search" },
    ],
    createElement: createFakeElement,
  });

  ui.renderPalette();

  assert.equal(palette.children.length, 1);
  const titles = palette.querySelectorAll(".palette-group-title");
  assert.deepEqual(titles.map((item) => item.textContent), ["未分组"]);
  const items = palette.querySelectorAll(".palette-item");
  assert.equal(items.length, 1);
  const item = items[0];
  assert.equal(item.dataset.nodeType, "sql_chart_v1");
  assert.equal(item.dataset.paletteGroup, "未分组");
  assert.equal(item.children[2].textContent, "未分组");
  item.listeners.click();
  assert.equal(nodeType.value, "sql_chart_v1");
  assert.equal(btnAdd.disabled, false);
  assert.deepEqual(statuses.slice(-1), [{ text: "已选择节点: SQL Chart", ok: true }]);
});

test("workflow palette ui groups items by group label while preserving policy order", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const palette = createPaletteStub();
  const ui = createWorkflowPaletteUi({
    palette,
    paletteMode: { value: "all" },
    paletteSearch: { value: "" },
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
  }, {
    nodeCatalog: [
      { type: "load_rows_v2", name: "Load", desc: "demo", group: "数据接入", policy_section: "data_access", policy_source: "rust_manifest" },
      { type: "transform_rows_v3", name: "Transform", desc: "demo", group: "数据处理", policy_section: "data_processing", policy_source: "rust_manifest" },
      { type: "manual_review", name: "Review", desc: "demo", group: "治理与审核", policy_section: "local_governance", policy_source: "local_policy" },
    ],
    createElement: createFakeElement,
  });

  ui.renderPalette();

  const titles = palette.querySelectorAll(".palette-group-title");
  assert.deepEqual(titles.map((item) => item.textContent), ["数据接入", "数据处理", "治理与审核"]);
  const items = palette.querySelectorAll(".palette-item");
  assert.deepEqual(items.map((item) => item.dataset.paletteGroup), ["数据接入", "数据处理", "治理与审核"]);
  assert.deepEqual(items.map((item) => item.dataset.nodeType), ["load_rows_v2", "transform_rows_v3", "manual_review"]);
  assert.deepEqual(items.map((item) => item.children[2].textContent), [
    "data_access 路 rust_manifest",
    "data_processing 路 rust_manifest",
    "local_governance 路 local_policy",
  ]);
});

test("workflow palette ui simple mode follows local node policy truth", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const palette = createPaletteStub();
  const ui = createWorkflowPaletteUi({
    palette,
    paletteMode: { value: "simple" },
    paletteSearch: { value: "" },
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
  }, {
    nodeCatalog: [
      { type: "ai_strategy_v1", name: "AI Strategy", desc: "local" },
      { type: "load_rows_v2", name: "Load Rows", desc: "rust" },
      { type: "manual_review", name: "Review", desc: "local" },
    ],
    createElement: createFakeElement,
  });

  ui.renderPalette();

  const items = palette.querySelectorAll(".palette-item");
  assert.deepEqual(items.map((item) => item.dataset.nodeType), ["ai_strategy_v1", "manual_review"]);
});

test("workflow palette ui handles canvas drop with snapped coordinates", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const createCalls = [];
  const statuses = [];
  const ui = createWorkflowPaletteUi({
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
    aiEndpoint: { value: "https://example.com" },
    aiKey: { value: "token" },
    aiModel: { value: "deepseek-chat" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    nodeCatalog: [
      { type: "ds_refine", name: "DS提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
      { type: "ai_refine", name: "AI 提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    ],
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
  assert.equal(statuses.length, 1);
  assert.equal(statuses[0].ok, true);
  assert.match(statuses[0].text, /已拖入节点:/);
  assert.match(statuses[0].text, /DS提炼/);
  assert.match(statuses[0].text, /local_ai/);
  assert.match(statuses[0].text, /local_policy/);
});

test("workflow palette ui reports unregistered node type before node creation proceeds", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const statuses = [];
  const ui = createWorkflowPaletteUi({
    nodeType: createInputStub("unknown_future_node"),
    nodeTypePolicyHint: { textContent: "", innerHTML: "" },
  }, {
    setStatus: (text, ok) => statuses.push({ text, ok }),
    createNode: () => {
      const error = new Error("workflow contains unregistered node types in add_node: unknown_future_node");
      error.code = "workflow_node_type_unregistered";
      throw error;
    },
  });

  ui.handleAddNode();

  assert.deepEqual(statuses, [{ text: "节点类型未注册: unknown_future_node", ok: false }]);
});

test("workflow palette ui renders node type policy hint for registered unknown and empty types", async () => {
  const { createWorkflowPaletteUi } = await loadPaletteUiModule();
  const nodeType = createInputStub("ai_refine");
  const btnAdd = { disabled: false };
  const nodeTypePolicyHint = { textContent: "", innerHTML: "" };
  const ui = createWorkflowPaletteUi({
    nodeType,
    nodeTypePolicyHint,
    btnAdd,
  }, {
    nodeCatalog: [
      { type: "ai_refine", name: "AI 提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    ],
  });

  ui.renderNodeTypePolicyHint();
  assert.match(nodeTypePolicyHint.textContent, /AI 提炼/);
  assert.match(nodeTypePolicyHint.textContent, /local_policy/);
  assert.match(nodeTypePolicyHint.textContent, /Identity/);
  assert.equal(btnAdd.disabled, false);
  assert.equal(nodeType.dataset.policyState, "registered");
  assert.equal(nodeType.ariaInvalid, "false");
  assert.match(nodeType.title, /registered node type/);

  nodeType.value = "unknown_future_node";
  ui.renderNodeTypePolicyHint();
  assert.match(nodeTypePolicyHint.textContent, /未注册/);
  assert.match(nodeTypePolicyHint.innerHTML, /已禁止添加/);
  assert.equal(btnAdd.disabled, true);
  assert.equal(nodeType.dataset.policyState, "unknown");
  assert.equal(nodeType.ariaInvalid, "true");
  assert.match(nodeType.title, /unregistered node type/);

  nodeType.value = "";
  ui.renderNodeTypePolicyHint();
  assert.match(nodeTypePolicyHint.textContent, /待输入/);
  assert.equal(btnAdd.disabled, true);
  assert.equal(nodeType.dataset.policyState, "empty");
  assert.equal(nodeType.ariaInvalid, "false");
  assert.match(nodeType.title, /enter node type/);
});
