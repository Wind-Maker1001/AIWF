function bindIfPresent(target, eventName, handler, options) {
  if (target?.addEventListener) target.addEventListener(eventName, handler, options);
}

function bindEdgeConfigEditorActions(ctx = {}) {
  const {
    els,
    store,
    setStatus = () => {},
    selectedEdgeRef = { get: () => null, set: () => {} },
    parseEdgeWhenText = () => null,
    applyEdgeWhenToBuilder = () => {},
    renderEdgeConfigEditor = () => {},
    renderAll = () => {},
    setEdgeWhenBuilderVisibility = () => {},
    syncEdgeTextFromBuilder = () => {},
  } = ctx;

  bindIfPresent(els.btnApplyEdgeCfg, "click", () => {
    const selectedEdge = selectedEdgeRef.get();
    if (!selectedEdge) {
      setStatus("请先点击一条连线", false);
      return;
    }
    try {
      const when = parseEdgeWhenText();
      const ok = store.updateEdgeWhen(selectedEdge.from, selectedEdge.to, when);
      if (!ok) {
        setStatus("连线不存在，可能已被删除", false);
        selectedEdgeRef.set(null);
        renderEdgeConfigEditor();
        return;
      }
      const nextEdge = store.getEdge(selectedEdge.from, selectedEdge.to);
      selectedEdgeRef.set(nextEdge);
      applyEdgeWhenToBuilder(nextEdge?.when);
      renderAll();
      setStatus(`已更新连线条件: ${selectedEdge.from} -> ${selectedEdge.to}`, true);
    } catch (error) {
      setStatus(String(error?.message || error || "连线条件更新失败"), false);
    }
  });

  bindIfPresent(els.btnClearEdgeCfg, "click", () => {
    const selectedEdge = selectedEdgeRef.get();
    if (!selectedEdge) {
      setStatus("请先点击一条连线", false);
      return;
    }
    const ok = store.updateEdgeWhen(selectedEdge.from, selectedEdge.to, null);
    if (!ok) {
      setStatus("连线不存在，可能已被删除", false);
      selectedEdgeRef.set(null);
      renderEdgeConfigEditor();
      return;
    }
    const nextEdge = store.getEdge(selectedEdge.from, selectedEdge.to);
    selectedEdgeRef.set(nextEdge);
    applyEdgeWhenToBuilder(nextEdge?.when);
    renderAll();
    setStatus(`已清空连线条件: ${selectedEdge.from} -> ${selectedEdge.to}`, true);
  });

  bindIfPresent(els.edgeWhenKind, "change", () => {
    setEdgeWhenBuilderVisibility(els.edgeWhenKind.value);
    syncEdgeTextFromBuilder();
  });
  bindIfPresent(els.edgeWhenBool, "change", syncEdgeTextFromBuilder);
  bindIfPresent(els.edgeWhenPath, "input", syncEdgeTextFromBuilder);
  bindIfPresent(els.edgeWhenField, "input", syncEdgeTextFromBuilder);
  bindIfPresent(els.edgeWhenOp, "change", syncEdgeTextFromBuilder);
  bindIfPresent(els.edgeWhenValue, "input", syncEdgeTextFromBuilder);

  bindIfPresent(els.edgeFieldHintSelect, "change", () => {
    const value = String(els.edgeFieldHintSelect.value || "").trim();
    if (!value || !els.edgeWhenField) return;
    els.edgeWhenField.value = value;
    syncEdgeTextFromBuilder();
  });

  bindIfPresent(els.edgePathHintSelect, "change", () => {
    const value = String(els.edgePathHintSelect.value || "").trim();
    if (!value || !els.edgeWhenPath) return;
    els.edgeWhenPath.value = value;
    syncEdgeTextFromBuilder();
  });

  bindIfPresent(els.btnBuildEdgeWhen, "click", () => {
    try {
      syncEdgeTextFromBuilder();
      setStatus("已从可视化构造器生成 JSON 条件", true);
    } catch (error) {
      setStatus(String(error?.message || error || "生成条件失败"), false);
    }
  });

  bindIfPresent(els.btnParseEdgeWhen, "click", () => {
    try {
      const when = parseEdgeWhenText();
      applyEdgeWhenToBuilder(when);
      setStatus("已将 JSON 条件回填到可视化构造器", true);
    } catch (error) {
      setStatus(String(error?.message || error || "JSON 回填失败"), false);
    }
  });
}

export {
  bindEdgeConfigEditorActions,
  bindIfPresent,
};
