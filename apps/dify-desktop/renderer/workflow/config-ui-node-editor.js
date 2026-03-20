function createWorkflowConfigNodeEditorSupport(els, deps = {}) {
  const {
    canvas,
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

  function clearNodeConfigEditor(ids = []) {
    const many = Array.isArray(ids) && ids.length > 1;
    if (els.selectedNodeInfo) {
      els.selectedNodeInfo.textContent = many
        ? `当前选中 ${ids.length} 个节点。请只保留 1 个节点后编辑配置。`
        : "未选中节点";
    }
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
    if (els.selectedNodeInfo) els.selectedNodeInfo.textContent = `当前节点: ${node.id} (${node.type})`;
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
