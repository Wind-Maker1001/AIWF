function createWorkflowConfigUi(els, deps = {}) {
  const {
    store,
    canvas,
    nodeFormSchemas = {},
    edgeHintsByNodeType = {},
    setStatus = () => {},
    renderAll = () => {},
    refreshOfflineBoundaryHint = () => {},
    getSelectedEdge = () => null,
    setSelectedEdge = () => {},
    getCfgViewMode = () => "form",
    setCfgViewMode = () => {},
  } = deps;

  function singleSelectedNode() {
    const ids = canvas.getSelectedIds();
    if (!Array.isArray(ids) || ids.length !== 1) return null;
    return store.getNode(ids[0]);
  }

  function prettyJson(value) {
    return JSON.stringify(value || {}, null, 2);
  }

  function parseNodeConfigText() {
    const text = String(els.nodeConfig.value || "").trim();
    if (!text) return {};
    let obj = null;
    try {
      obj = JSON.parse(text);
    } catch {
      throw new Error("配置必须是合法 JSON");
    }
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
      throw new Error("配置必须是 JSON 对象");
    }
    return obj;
  }

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
    const k = String(kind || "none");
    if (els.edgeWhenBoolWrap) els.edgeWhenBoolWrap.style.display = k === "bool" ? "block" : "none";
    if (els.edgeWhenPathWrap) els.edgeWhenPathWrap.style.display = k === "path" ? "block" : "none";
    if (els.edgeWhenRuleWrap) els.edgeWhenRuleWrap.style.display = k === "rule" ? "block" : "none";
  }

  function edgeWhenFromBuilder() {
    const kind = String(els.edgeWhenKind?.value || "none");
    if (kind === "none") return null;
    if (kind === "bool") return String(els.edgeWhenBool?.value || "true") === "true";
    if (kind === "path") return String(els.edgeWhenPath?.value || "").trim();
    const field = String(els.edgeWhenField?.value || "").trim();
    const op = String(els.edgeWhenOp?.value || "eq").trim() || "eq";
    const out = { field, op };
    if (op !== "exists" && op !== "not_exists") out.value = parseLooseJsonValue(els.edgeWhenValue?.value || "");
    return out;
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
    if (els.edgeWhenValue) {
      const v = typeof when.value === "undefined" ? "" : JSON.stringify(when.value);
      els.edgeWhenValue.value = v;
    }
    setEdgeWhenBuilderVisibility("rule");
  }

  function syncEdgeTextFromBuilder() {
    if (!els.edgeWhenText) return;
    const when = edgeWhenFromBuilder();
    els.edgeWhenText.value = when === null ? "" : JSON.stringify(when, null, 2);
  }

  function rebuildEdgeHints(edge) {
    const fieldMap = new Map();
    function addHint(value, source) {
      const v = String(value || "").trim();
      if (!v) return;
      if (!fieldMap.has(v)) fieldMap.set(v, new Set());
      fieldMap.get(v).add(String(source || "system"));
    }
    [
      "ok",
      "status",
      "detail",
      "detail.ok",
      "detail.status",
      "output.ok",
      "output.status",
    ].forEach((k) => addHint(k, "system"));
    if (edge && edge.from) {
      const fromNode = store.getNode(edge.from);
      if (fromNode) {
        const byType = edgeHintsByNodeType[String(fromNode.type || "")] || [];
        byType.forEach((k) => addHint(k, `type:${fromNode.type}`));
        const cfg = fromNode.config && typeof fromNode.config === "object" ? fromNode.config : {};
        Object.keys(cfg).forEach((k) => addHint(k, "config"));
      }
    }
    const arr = Array.from(fieldMap.keys()).filter(Boolean).sort((a, b) => a.localeCompare(b));
    const labelFor = (value) => {
      const src = Array.from(fieldMap.get(value) || []).sort().join("|");
      return `${value}  [from:${src}]`;
    };
    if (els.edgeFieldHints) {
      els.edgeFieldHints.innerHTML = "";
      arr.forEach((v) => {
        const op = document.createElement("option");
        op.value = v;
        els.edgeFieldHints.appendChild(op);
      });
    }
    if (els.edgePathHints) {
      els.edgePathHints.innerHTML = "";
      arr
        .map((v) => (v.includes(".") ? v : `detail.${v}`))
        .concat(arr)
        .filter((v, i, xs) => xs.indexOf(v) === i)
        .forEach((v) => {
          const op = document.createElement("option");
          op.value = v;
          els.edgePathHints.appendChild(op);
        });
    }
    if (els.edgeFieldHintSelect) {
      els.edgeFieldHintSelect.innerHTML = '<option value="">选择建议...</option>';
      arr.forEach((v) => {
        const op = document.createElement("option");
        op.value = v;
        op.textContent = labelFor(v);
        els.edgeFieldHintSelect.appendChild(op);
      });
    }
    if (els.edgePathHintSelect) {
      const pathArr = arr
        .map((v) => (v.includes(".") ? v : `detail.${v}`))
        .concat(arr)
        .filter((v, i, xs) => xs.indexOf(v) === i);
      els.edgePathHintSelect.innerHTML = '<option value="">选择建议...</option>';
      pathArr.forEach((v) => {
        const base = v.startsWith("detail.") ? v.slice("detail.".length) : v;
        const op = document.createElement("option");
        op.value = v;
        op.textContent = labelFor(base).replace(`${base}  `, `${v}  `);
        els.edgePathHintSelect.appendChild(op);
      });
    }
  }

  function renderEdgeConfigEditor() {
    if (!els.selectedEdgeInfo || !els.edgeWhenText) return;
    const selectedEdge = getSelectedEdge();
    if (!selectedEdge || !selectedEdge.from || !selectedEdge.to) {
      els.selectedEdgeInfo.textContent = "未选中连线";
      els.edgeWhenText.value = "";
      rebuildEdgeHints(null);
      applyEdgeWhenToBuilder(null);
      if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = true;
      if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = true;
      if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = true;
      if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = true;
      if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = true;
      if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = true;
      return;
    }
    const alive = store.getEdge(selectedEdge.from, selectedEdge.to);
    if (!alive) {
      setSelectedEdge(null);
      els.selectedEdgeInfo.textContent = "未选中连线";
      els.edgeWhenText.value = "";
      rebuildEdgeHints(null);
      applyEdgeWhenToBuilder(null);
      if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = true;
      if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = true;
      if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = true;
      if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = true;
      if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = true;
      if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = true;
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
    if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = false;
    if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = false;
    if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = false;
    if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = false;
    if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = false;
    if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = false;
  }

  function formSchemaForNodeType(type) {
    return nodeFormSchemas[String(type || "")] || [];
  }

  function setCfgMode(mode) {
    const nextMode = mode === "json" ? "json" : "form";
    setCfgViewMode(nextMode);
    const isJson = getCfgViewMode() === "json";
    els.nodeConfig.style.display = isJson ? "block" : "none";
    els.nodeConfigForm.style.display = isJson ? "none" : "block";
    els.btnCfgJson.className = `btn ${isJson ? "" : "secondary"}`.trim();
    els.btnCfgForm.className = `btn ${isJson ? "secondary" : ""}`.trim();
  }

  function toFieldDisplayValue(type, v) {
    if (type === "json") return prettyJson(v ?? (Array.isArray(v) ? [] : {}));
    if (type === "csv") return Array.isArray(v) ? v.join(",") : "";
    if (type === "bool") return v === true ? "true" : "false";
    if (v === null || v === undefined) return "";
    return String(v);
  }

  function parseFieldValue(type, text) {
    const raw = String(text || "");
    if (type === "number") {
      const n = Number(raw);
      if (!Number.isFinite(n)) throw new Error(`数字字段格式错误: ${raw}`);
      return n;
    }
    if (type === "csv") {
      return raw
        .split(",")
        .map((s) => s.trim())
        .filter((s) => !!s);
    }
    if (type === "bool") {
      const s = raw.trim().toLowerCase();
      if (s === "true" || s === "1" || s === "yes") return true;
      if (s === "false" || s === "0" || s === "no" || s === "") return false;
      throw new Error(`布尔字段格式错误: ${raw}`);
    }
    if (type === "json") {
      const t = raw.trim();
      if (!t) return {};
      return JSON.parse(t);
    }
    return raw;
  }

  function renderNodeConfigForm(node, cfg) {
    const schema = formSchemaForNodeType(node?.type);
    const generic = [
      { key: "isolation_level", label: "隔离等级(none/process/sandbox)", type: "text" },
      { key: "sandbox_limits", label: "Sandbox限制(JSON对象)", type: "json" },
      { key: "input_map", label: "输入映射(JSON对象: 目标字段->来源路径)", type: "json" },
      { key: "output_map", label: "输出映射(JSON对象: 别名->输出路径)", type: "json" },
    ];
    const fullSchema = [...schema, ...generic];
    els.nodeConfigForm.innerHTML = "";
    if (!node || !fullSchema.length) {
      els.nodeConfigForm.innerHTML = '<div class="cfg-help">该节点暂无预置表单，请切到 JSON 模式编辑。</div>';
      return;
    }
    fullSchema.forEach((f) => {
      const wrap = document.createElement("div");
      wrap.className = "cfg-field";
      const label = document.createElement("label");
      label.textContent = f.label;
      const control = f.type === "json" ? document.createElement("textarea") : document.createElement("input");
      control.dataset.key = f.key;
      control.dataset.kind = f.type;
      if (f.type !== "json") control.type = "text";
      if (f.type === "number") control.inputMode = "numeric";
      control.value = toFieldDisplayValue(f.type, cfg?.[f.key]);
      wrap.append(label, control);
      els.nodeConfigForm.appendChild(wrap);
    });
  }

  function parseNodeConfigForm(node, baseCfg = {}) {
    const schema = [
      ...formSchemaForNodeType(node?.type),
      { key: "isolation_level", type: "text" },
      { key: "sandbox_limits", type: "json" },
      { key: "input_map", type: "json" },
      { key: "output_map", type: "json" },
    ];
    if (!schema.length) return { ...baseCfg };
    const out = { ...(baseCfg || {}) };
    const controls = els.nodeConfigForm.querySelectorAll("[data-key]");
    controls.forEach((el) => {
      const key = String(el.dataset.key || "");
      const kind = String(el.dataset.kind || "text");
      if (!key) return;
      const val = parseFieldValue(kind, el.value);
      out[key] = val;
    });
    return out;
  }

  function readMapTableRows(tbody) {
    const out = {};
    if (!tbody) return out;
    const rows = tbody.querySelectorAll("tr[data-map-row='1']");
    rows.forEach((tr) => {
      const k = String(tr.querySelector("input[data-role='key']")?.value || "").trim();
      const v = String(tr.querySelector("input[data-role='value']")?.value || "").trim();
      if (!k || !v) return;
      out[k] = v;
    });
    return out;
  }

  function renderMapTableRows(tbody, mapObj, onChange) {
    if (!tbody) return;
    const obj = mapObj && typeof mapObj === "object" && !Array.isArray(mapObj) ? mapObj : {};
    const items = Object.entries(obj);
    tbody.innerHTML = "";
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
      return;
    }
    items.forEach(([k, v]) => {
      const tr = document.createElement("tr");
      tr.dataset.mapRow = "1";
      const tdK = document.createElement("td");
      const tdV = document.createElement("td");
      const tdOp = document.createElement("td");
      const inK = document.createElement("input");
      inK.dataset.role = "key";
      inK.value = String(k || "");
      const inV = document.createElement("input");
      inV.dataset.role = "value";
      inV.value = String(v || "");
      inK.oninput = onChange;
      inV.oninput = onChange;
      const del = document.createElement("button");
      del.className = "mini del";
      del.textContent = "删";
      del.onclick = () => {
        tr.remove();
        onChange();
      };
      tdK.appendChild(inK);
      tdV.appendChild(inV);
      tdOp.appendChild(del);
      tr.append(tdK, tdV, tdOp);
      tbody.appendChild(tr);
    });
  }

  function syncIoMapFromTables() {
    const node = singleSelectedNode();
    if (!node) return;
    const current = node.config && typeof node.config === "object" ? { ...node.config } : {};
    current.input_map = readMapTableRows(els.inputMapRows);
    current.output_map = readMapTableRows(els.outputMapRows);
    store.updateNodeConfig(node.id, current);
    els.nodeConfig.value = prettyJson(current);
  }

  function renderIoMapEditor(node, cfg) {
    if (!els.inputMapRows || !els.outputMapRows) return;
    if (!node) {
      els.inputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
      els.outputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
      return;
    }
    const onChange = () => syncIoMapFromTables();
    renderMapTableRows(els.inputMapRows, cfg?.input_map, onChange);
    renderMapTableRows(els.outputMapRows, cfg?.output_map, onChange);
  }

  function renderNodeConfigEditor() {
    const ids = canvas.getSelectedIds();
    const many = Array.isArray(ids) && ids.length > 1;
    const node = singleSelectedNode();
    if (!node) {
      els.selectedNodeInfo.textContent = many
        ? `当前选中 ${ids.length} 个节点。请只保留 1 个节点后编辑配置。`
        : "未选中节点";
      els.nodeConfig.value = "";
      els.nodeConfig.disabled = true;
      els.btnApplyNodeCfg.disabled = true;
      els.btnResetNodeCfg.disabled = true;
      els.btnFormatNodeCfg.disabled = true;
      els.nodeConfigForm.innerHTML = '<div class="cfg-help">请先选中 1 个节点。</div>';
      renderIoMapEditor(null, null);
      return;
    }
    const cfg = node.config && typeof node.config === "object" ? node.config : defaultNodeConfig(node.type);
    els.selectedNodeInfo.textContent = `当前节点: ${node.id} (${node.type})`;
    els.nodeConfig.value = prettyJson(cfg);
    renderNodeConfigForm(node, cfg);
    els.nodeConfig.disabled = false;
    els.btnApplyNodeCfg.disabled = false;
    els.btnResetNodeCfg.disabled = false;
    els.btnFormatNodeCfg.disabled = false;
    renderIoMapEditor(node, cfg);
  }

  return {
    singleSelectedNode,
    prettyJson,
    parseNodeConfigText,
    parseEdgeWhenText,
    parseLooseJsonValue,
    setEdgeWhenBuilderVisibility,
    edgeWhenFromBuilder,
    applyEdgeWhenToBuilder,
    syncEdgeTextFromBuilder,
    rebuildEdgeHints,
    renderEdgeConfigEditor,
    formSchemaForNodeType,
    setCfgMode,
    toFieldDisplayValue,
    parseFieldValue,
    renderNodeConfigForm,
    parseNodeConfigForm,
    readMapTableRows,
    renderMapTableRows,
    syncIoMapFromTables,
    renderIoMapEditor,
    renderNodeConfigEditor,
  };
}

export { createWorkflowConfigUi };
