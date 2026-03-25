import {
  describePolicyOwnership,
  formatNodePolicyHint,
  getNodePolicyInfo,
} from "./node-policy-support.js";

function createWorkflowConfigNodeEditorSupport(els, deps = {}) {
  const {
    canvas,
    nodeCatalog = [],
    singleSelectedNode = () => null,
    prettyJson = (value) => JSON.stringify(value || {}, null, 2),
    renderNodeConfigForm = () => {},
    renderIoMapEditor = () => {},
    defaultNodeConfig = () => ({}),
  } = deps;

  function setNodeConfigEditorDisabled(disabled) {
    if (els.nodeConfig) els.nodeConfig.disabled = !!disabled;
    if (els.btnApplyNodeCfg) els.btnApplyNodeCfg.disabled = !!disabled;
    if (els.btnResetNodeCfg) els.btnResetNodeCfg.disabled = !!disabled;
    if (els.btnFormatNodeCfg) els.btnFormatNodeCfg.disabled = !!disabled;
  }

  function renderSelectedNodePolicyInfo(node = null, options = {}) {
    if (!els.selectedNodePolicyInfo) return;
    const fallbackText = String(options.fallbackText || "节点策略: -");
    const info = node ? getNodePolicyInfo(nodeCatalog, node) : null;
    if (!info) {
      els.selectedNodePolicyInfo.textContent = fallbackText;
      els.selectedNodePolicyInfo.innerHTML = `<div class="cfg-help">${fallbackText}</div>`;
      return;
    }
    const ownership = describePolicyOwnership(info);
    const rows = [
      { label: "分组", value: info.group || "-" },
      { label: "策略段", value: info.policySection || "-" },
      { label: "来源", value: info.policySource || "-" },
      { label: "所有权", value: ownership.owner },
      { label: "Identity", value: ownership.identityRule },
      { label: "编辑边界", value: ownership.boundary },
    ];
    els.selectedNodePolicyInfo.textContent = `节点策略: ${rows.map((row) => `${row.label} ${row.value}`).join(" · ")}`;
    els.selectedNodePolicyInfo.innerHTML = [
      '<div class="node-policy-card">',
      '<div class="cfg-help">节点策略</div>',
      ...rows.map((row) => (
        `<div class="node-policy-row"><strong>${row.label}</strong><span>${row.value}</span></div>`
      )),
      "</div>",
    ].join("");
  }

  function clearNodeConfigEditor(ids = []) {
    const many = Array.isArray(ids) && ids.length > 1;
    if (els.selectedNodeInfo) {
      els.selectedNodeInfo.textContent = many
        ? `当前选中 ${ids.length} 个节点。请只保留 1 个节点后编辑配置。`
        : "未选中节点";
    }
    renderSelectedNodePolicyInfo(null, {
      fallbackText: many ? "节点策略: 多选模式下不显示" : "节点策略: -",
    });
    if (els.nodeConfig) els.nodeConfig.value = "";
    setNodeConfigEditorDisabled(true);
    if (els.nodeConfigForm) els.nodeConfigForm.innerHTML = '<div class="cfg-help">请先选中 1 个节点。</div>';
    renderIoMapEditor(null, null);
  }

  function renderNodeConfigEditor() {
    const ids = canvas.getSelectedIds();
    const node = singleSelectedNode();
    if (!node) {
      clearNodeConfigEditor(ids);
      return;
    }
    const cfg = node.config && typeof node.config === "object" ? node.config : defaultNodeConfig(node.type);
    const policyHint = formatNodePolicyHint(nodeCatalog, node?.type, node?.type);
    if (els.selectedNodeInfo) {
      els.selectedNodeInfo.textContent = `当前节点: ${node.id} (${node.type})`;
    }
    renderSelectedNodePolicyInfo(node, {
      fallbackText: policyHint ? `节点策略: ${policyHint}` : "节点策略: 未声明",
    });
    if (els.nodeConfig) els.nodeConfig.value = prettyJson(cfg);
    renderNodeConfigForm(node, cfg);
    setNodeConfigEditorDisabled(false);
    renderIoMapEditor(node, cfg);
  }

  return {
    renderNodeConfigEditor,
  };
}

export { createWorkflowConfigNodeEditorSupport };
