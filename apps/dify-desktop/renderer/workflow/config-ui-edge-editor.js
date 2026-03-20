function createWorkflowConfigEdgeEditorSupport(els, deps = {}) {
  const {
    store,
    edgeHintsByNodeType = {},
    getSelectedEdge = () => null,
    setSelectedEdge = () => {},
    applyEdgeWhenToBuilder = () => {},
  } = deps;

  function setEdgeEditorDisabled(disabled) {
    if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = !!disabled;
    if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = !!disabled;
    if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = !!disabled;
    if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = !!disabled;
    if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = !!disabled;
    if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = !!disabled;
  }

  function renderEdgeHintOptions(selectEl, placeholder, values, labelFor) {
    if (!selectEl) return;
    selectEl.innerHTML = placeholder;
    values.forEach((value) => {
      const option = document.createElement("option");
      option.value = value;
      option.textContent = labelFor(value);
      selectEl.appendChild(option);
    });
  }

  function rebuildEdgeHints(edge) {
    const fieldMap = new Map();
    const addHint = (value, source) => {
      const nextValue = String(value || "").trim();
      if (!nextValue) return;
      if (!fieldMap.has(nextValue)) fieldMap.set(nextValue, new Set());
      fieldMap.get(nextValue).add(String(source || "system"));
    };

    [
      "ok",
      "status",
      "detail",
      "detail.ok",
      "detail.status",
      "output.ok",
      "output.status",
    ].forEach((key) => addHint(key, "system"));

    if (edge && edge.from) {
      const fromNode = store.getNode(edge.from);
      if (fromNode) {
        const byType = edgeHintsByNodeType[String(fromNode.type || "")] || [];
        byType.forEach((key) => addHint(key, `type:${fromNode.type}`));
        const cfg = fromNode.config && typeof fromNode.config === "object" ? fromNode.config : {};
        Object.keys(cfg).forEach((key) => addHint(key, "config"));
      }
    }

    const values = Array.from(fieldMap.keys()).filter(Boolean).sort((a, b) => a.localeCompare(b));
    const labelFor = (value) => {
      const src = Array.from(fieldMap.get(value) || []).sort().join("|");
      return `${value}  [from:${src}]`;
    };

    if (els.edgeFieldHints) {
      els.edgeFieldHints.innerHTML = "";
      values.forEach((value) => {
        const option = document.createElement("option");
        option.value = value;
        els.edgeFieldHints.appendChild(option);
      });
    }

    renderEdgeHintOptions(els.edgeFieldHintSelect, '<option value="">选择字段提示...</option>', values, labelFor);
    renderEdgeHintOptions(els.edgePathHintSelect, '<option value="">选择路径提示...</option>', values, labelFor);
  }

  function clearEdgeConfigEditor() {
    if (els.selectedEdgeInfo) els.selectedEdgeInfo.textContent = "未选中连线";
    if (els.edgeWhenText) els.edgeWhenText.value = "";
    rebuildEdgeHints(null);
    applyEdgeWhenToBuilder(null);
    setEdgeEditorDisabled(true);
  }

  function renderEdgeConfigEditor() {
    if (!els.selectedEdgeInfo || !els.edgeWhenText) return;
    const selectedEdge = getSelectedEdge();
    if (!selectedEdge || !selectedEdge.from || !selectedEdge.to) {
      clearEdgeConfigEditor();
      return;
    }

    const alive = store.getEdge(selectedEdge.from, selectedEdge.to);
    if (!alive) {
      setSelectedEdge(null);
      clearEdgeConfigEditor();
      return;
    }

    const nextEdge = { ...alive };
    setSelectedEdge(nextEdge);
    rebuildEdgeHints(nextEdge);
    els.selectedEdgeInfo.textContent = `当前连线: ${nextEdge.from} -> ${nextEdge.to}`;
    els.edgeWhenText.value = nextEdge.when === null || typeof nextEdge.when === "undefined"
      ? ""
      : JSON.stringify(nextEdge.when, null, 2);
    applyEdgeWhenToBuilder(nextEdge.when);
    setEdgeEditorDisabled(false);
  }

  return {
    rebuildEdgeHints,
    renderEdgeConfigEditor,
  };
}

export { createWorkflowConfigEdgeEditorSupport };
