const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadNodeEditorModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/config-ui-node-editor.js")).href;
  return import(file);
}

test("workflow config node editor shows selected node policy hint", async () => {
  const { createWorkflowConfigNodeEditorSupport } = await loadNodeEditorModule();
  const els = {
    selectedNodeInfo: { textContent: "" },
    selectedNodePolicyInfo: { textContent: "", innerHTML: "" },
    nodeConfig: { value: "", disabled: false },
    btnApplyNodeCfg: { disabled: false },
    btnResetNodeCfg: { disabled: false },
    btnFormatNodeCfg: { disabled: false },
    nodeConfigForm: { innerHTML: "" },
  };
  const renderCalls = [];
  const ioCalls = [];
  const support = createWorkflowConfigNodeEditorSupport(els, {
    canvas: { getSelectedIds: () => ["n1"] },
    nodeCatalog: [
      { type: "ai_refine", name: "AI 提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    ],
    singleSelectedNode: () => ({ id: "n1", type: "ai_refine", config: { reuse_existing: true } }),
    renderNodeConfigForm: (node, cfg) => renderCalls.push({ node, cfg }),
    renderIoMapEditor: (node, cfg) => ioCalls.push({ node, cfg }),
    defaultNodeConfig: () => ({}),
  });

  support.renderNodeConfigEditor();

  assert.equal(els.selectedNodeInfo.textContent, "当前节点: n1 (ai_refine)");
  assert.match(els.selectedNodePolicyInfo.textContent, /节点策略:/);
  assert.match(els.selectedNodePolicyInfo.textContent, /分组 AI 编排/);
  assert.match(els.selectedNodePolicyInfo.textContent, /策略段 local_ai/);
  assert.match(els.selectedNodePolicyInfo.textContent, /来源 local_policy/);
  assert.match(els.selectedNodePolicyInfo.textContent, /所有权 前端 authoring/);
  assert.match(els.selectedNodePolicyInfo.textContent, /Identity 节点 type 由前端 policy 管理/);
  assert.match(els.selectedNodePolicyInfo.textContent, /编辑边界/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /节点策略/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /分组/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /策略段/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /来源/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /所有权/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /Identity/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /编辑边界/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /前端 authoring/);
  assert.match(els.selectedNodePolicyInfo.innerHTML, /Rust 平台能力节点/);
  assert.match(els.nodeConfig.value, /reuse_existing/);
  assert.equal(els.nodeConfig.disabled, false);
  assert.equal(renderCalls.length, 1);
  assert.equal(ioCalls.length, 1);
});
