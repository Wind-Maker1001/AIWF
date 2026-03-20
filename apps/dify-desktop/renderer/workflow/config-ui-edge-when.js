function createWorkflowConfigEdgeWhenSupport(els) {
  function parseEdgeWhenText() {
    const text = String(els.edgeWhenText?.value || "").trim();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      throw new Error("连线条件必须是合法 JSON");
    }
  }

  function parseLooseJsonValue(raw) {
    const text = String(raw || "").trim();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }

  function setEdgeWhenBuilderVisibility(kind) {
    const nextKind = String(kind || "none");
    if (els.edgeWhenBoolWrap) els.edgeWhenBoolWrap.style.display = nextKind === "bool" ? "block" : "none";
    if (els.edgeWhenPathWrap) els.edgeWhenPathWrap.style.display = nextKind === "path" ? "block" : "none";
    if (els.edgeWhenRuleWrap) els.edgeWhenRuleWrap.style.display = nextKind === "rule" ? "block" : "none";
  }

  function edgeWhenFromBuilder() {
    const kind = String(els.edgeWhenKind?.value || "none");
    if (kind === "none") return null;
    if (kind === "bool") return String(els.edgeWhenBool?.value || "true") === "true";
    if (kind === "path") return String(els.edgeWhenPath?.value || "").trim();
    const field = String(els.edgeWhenField?.value || "").trim();
    const op = String(els.edgeWhenOp?.value || "eq").trim() || "eq";
    const output = { field, op };
    if (op !== "exists" && op !== "not_exists") output.value = parseLooseJsonValue(els.edgeWhenValue?.value || "");
    return output;
  }

  function applyEdgeWhenToBuilder(when) {
    if (!els.edgeWhenKind) return;
    if (when === null || typeof when === "undefined") {
      els.edgeWhenKind.value = "none";
      if (els.edgeWhenPath) els.edgeWhenPath.value = "";
      if (els.edgeWhenField) els.edgeWhenField.value = "";
      if (els.edgeWhenValue) els.edgeWhenValue.value = "";
      if (els.edgeWhenOp) els.edgeWhenOp.value = "eq";
      if (els.edgeWhenBool) els.edgeWhenBool.value = "true";
      setEdgeWhenBuilderVisibility("none");
      return;
    }
    if (typeof when === "boolean") {
      els.edgeWhenKind.value = "bool";
      if (els.edgeWhenBool) els.edgeWhenBool.value = when ? "true" : "false";
      setEdgeWhenBuilderVisibility("bool");
      return;
    }
    if (typeof when === "string") {
      els.edgeWhenKind.value = "path";
      if (els.edgeWhenPath) els.edgeWhenPath.value = when;
      setEdgeWhenBuilderVisibility("path");
      return;
    }
    els.edgeWhenKind.value = "rule";
    if (els.edgeWhenField) els.edgeWhenField.value = String(when.field || "");
    if (els.edgeWhenOp) els.edgeWhenOp.value = String(when.op || "eq");
    if (els.edgeWhenValue) els.edgeWhenValue.value = typeof when.value === "undefined" ? "" : JSON.stringify(when.value);
    setEdgeWhenBuilderVisibility("rule");
  }

  function syncEdgeTextFromBuilder() {
    if (!els.edgeWhenText) return;
    const when = edgeWhenFromBuilder();
    els.edgeWhenText.value = when === null ? "" : JSON.stringify(when, null, 2);
  }

  return {
    applyEdgeWhenToBuilder,
    edgeWhenFromBuilder,
    parseEdgeWhenText,
    parseLooseJsonValue,
    setEdgeWhenBuilderVisibility,
    syncEdgeTextFromBuilder,
  };
}

export { createWorkflowConfigEdgeWhenSupport };
