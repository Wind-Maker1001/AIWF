function nodeBaseConfig(node) {
  return node?.config && typeof node.config === "object" ? node.config : {};
}

function bindNodeConfigEditorActions(ctx = {}) {
  const {
    els,
    store,
    defaultNodeConfig,
    setStatus = () => {},
    singleSelectedNode = () => null,
    parseNodeConfigText = () => ({}),
    parseNodeConfigForm = () => ({}),
    prettyJson = (value) => String(value ?? ""),
    renderNodeConfigForm = () => {},
    renderNodeConfigEditor = () => {},
    setCfgMode = () => {},
    getCfgViewMode = () => "form",
  } = ctx;

  els.btnApplyNodeCfg.addEventListener("click", () => {
    const node = singleSelectedNode();
    if (!node) {
      setStatus("请先选中 1 个节点", false);
      return;
    }
    try {
      const parsed =
        getCfgViewMode() === "json"
          ? parseNodeConfigText()
          : parseNodeConfigForm(node, nodeBaseConfig(node));
      const ok = store.updateNodeConfig(node.id, parsed);
      if (!ok) {
        setStatus("配置应用失败：节点不存在", false);
        return;
      }
      setStatus(`配置已应用: ${node.id}`, true);
      renderNodeConfigEditor();
    } catch (error) {
      setStatus(String(error?.message || error || "配置应用失败"), false);
    }
  });

  els.btnResetNodeCfg.addEventListener("click", () => {
    const node = singleSelectedNode();
    if (!node) {
      setStatus("请先选中 1 个节点", false);
      return;
    }
    const cfg = defaultNodeConfig(node.type);
    store.updateNodeConfig(node.id, cfg);
    renderNodeConfigEditor();
    setStatus(`已重置为默认配置: ${node.type}`, true);
  });

  els.btnFormatNodeCfg.addEventListener("click", () => {
    if (els.nodeConfig.disabled) return;
    try {
      const parsed = parseNodeConfigText();
      els.nodeConfig.value = prettyJson(parsed);
      const node = singleSelectedNode();
      if (node) renderNodeConfigForm(node, parsed);
    } catch (error) {
      setStatus(String(error?.message || error || "配置格式化失败"), false);
    }
  });

  els.btnCfgForm.addEventListener("click", () => setCfgMode("form"));
  els.btnCfgJson.addEventListener("click", () => setCfgMode("json"));

  els.nodeConfigForm.addEventListener("input", () => {
    const node = singleSelectedNode();
    if (!node) return;
    try {
      const parsed = parseNodeConfigForm(node, nodeBaseConfig(node));
      els.nodeConfig.value = prettyJson(parsed);
    } catch {}
  });
}

export {
  bindNodeConfigEditorActions,
  nodeBaseConfig,
};
