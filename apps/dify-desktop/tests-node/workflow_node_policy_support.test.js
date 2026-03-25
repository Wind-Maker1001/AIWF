const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadNodePolicySupport() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/node-policy-support.js")).href;
  return import(file);
}

test("workflow node policy support normalizes labels and formats ownership hints", async () => {
  const support = await loadNodePolicySupport();
  const nodeCatalog = [
    { type: "ai_refine", name: "AI 提炼", desc: "demo", group: "AI 编排", policy_section: "local_ai", policy_source: "local_policy" },
    { type: "load_rows_v2", name: "加载数据 v2", desc: "demo", group: "数据接入", policy_section: "data_access", policy_source: "rust_manifest" },
  ];

  assert.equal(support.normalizeGroupLabel(""), "未分组");
  assert.equal(support.findNodeCatalogEntry(nodeCatalog, "AI_REFINE").type, "ai_refine");

  const localHint = support.formatNodePolicyHint(nodeCatalog, "ai_refine", "Fallback");
  assert.equal(localHint.statusLabel, "AI 提炼 · AI 编排 [local_ai] · local_policy");
  assert.equal(localHint.policySource, "local_policy");

  const rustInfo = support.getNodePolicyInfo(nodeCatalog, { type: "load_rows_v2" });
  assert.deepEqual(rustInfo, {
    group: "数据接入",
    policySection: "data_access",
    policySource: "rust_manifest",
  });

  const localOwnership = support.describePolicyOwnership(localHint);
  assert.equal(localOwnership.owner, "前端 authoring");
  assert.match(localOwnership.identityRule, /节点 type 由前端 policy 管理/);
  assert.match(localOwnership.boundary, /前端可直接调整本地节点展示与交互/);

  const rustOwnership = support.describePolicyOwnership(rustInfo);
  assert.equal(rustOwnership.owner, "Rust 平台");
  assert.match(rustOwnership.identityRule, /identity 不可在前端改写/);
  assert.match(rustOwnership.boundary, /不拥有 operator identity/);
});
