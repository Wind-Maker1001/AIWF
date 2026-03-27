const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadPreflightUiModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/preflight-ui.js")).href;
  return import(file);
}

function createFakeElement(tag) {
  return {
    tag,
    className: "",
    textContent: "",
    innerHTML: "",
    style: {},
    children: [],
    append(...nodes) {
      this.children.push(...nodes);
    },
    appendChild(node) {
      this.children.push(node);
    },
  };
}

test("workflow preflight ui renders unknown node type issues with contract guidance", async () => {
  const { createWorkflowPreflightUi } = await loadPreflightUiModule();
  const focusCalls = [];
  const preflightSummary = { textContent: "", style: {} };
  const preflightRisk = { textContent: "", style: {} };
  const preflightRows = {
    innerHTML: "",
    children: [],
    appendChild(node) {
      this.children.push(node);
    },
  };
  const ui = createWorkflowPreflightUi({
    preflightSummary,
    preflightRisk,
    preflightRows,
  }, {
    focusNodeInCanvas: (nodeId) => focusCalls.push(nodeId),
    createElement: createFakeElement,
  });

  ui.renderPreflightReport({
    ok: false,
    issues: [
      {
        level: "error",
        kind: "unknown_node_type",
        node_id: "n_unknown",
        message: "workflow contains unregistered node types: unknown_future_node",
        contract_boundary: "node_catalog_truth",
        resolution_hint: "replace node type or sync Rust manifest / local node policy",
        action_text: "定位节点",
      },
    ],
  });

  assert.match(preflightSummary.textContent, /未通过/);
  assert.match(preflightSummary.textContent, /错误 1/);
  assert.match(preflightRisk.textContent, /风险等级:/);
  assert.equal(preflightRows.children.length, 1);

  const row = preflightRows.children[0];
  assert.equal(row.children.length, 4);
  assert.equal(row.children[0].textContent, "错误");
  assert.equal(row.children[1].textContent, "unknown_node_type");
  assert.match(row.children[2].textContent, /未注册节点类型/);
  assert.match(row.children[2].textContent, /主路径已禁止导入、添加和运行/);
  assert.match(row.children[2].textContent, /node_catalog_truth/);
  assert.match(row.children[2].textContent, /Rust manifest \/ local node policy/);
  assert.equal(row.children[3].children.length, 1);
  assert.equal(row.children[3].children[0].textContent, "定位节点");

  row.children[3].children[0].onclick();
  assert.deepEqual(focusCalls, ["n_unknown"]);
});

test("workflow preflight ui renders graph contract issues with error code", async () => {
  const { createWorkflowPreflightUi } = await loadPreflightUiModule();
  const preflightRows = {
    innerHTML: "",
    children: [],
    appendChild(node) {
      this.children.push(node);
    },
  };
  const ui = createWorkflowPreflightUi({
    preflightSummary: { textContent: "", style: {} },
    preflightRisk: { textContent: "", style: {} },
    preflightRows,
  }, {
    createElement: createFakeElement,
  });

  ui.renderPreflightReport({
    ok: false,
    issues: [
      {
        level: "error",
        kind: "graph_contract",
        message: "workflow.version is required",
        error_code: "required",
        error_path: "workflow.version",
        error_contract: "contracts/desktop/node_config_validation_errors.v1.json",
        resolution_hint: "请先把流程迁移到带顶层 version 的格式后再保存、运行或发布。",
      },
    ],
  });

  assert.equal(preflightRows.children.length, 1);
  const row = preflightRows.children[0];
  assert.equal(row.children[1].textContent, "required");
  assert.match(row.children[2].textContent, /workflow\.version is required/);
  assert.match(row.children[2].textContent, /workflow\.version/);
  assert.match(row.children[2].textContent, /node_config_validation_errors\.v1\.json/);
  assert.match(row.children[2].textContent, /请先把流程迁移到带顶层 version 的格式/);
});
