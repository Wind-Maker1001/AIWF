function createWorkflowConfigMapSupport(els, deps = {}) {
  const {
    store,
    singleSelectedNode = () => null,
    prettyJson = (value) => JSON.stringify(value ?? {}, null, 2),
  } = deps;

  function readMapTableRows(tbody) {
    const out = {};
    if (!tbody) return out;
    const rows = tbody.querySelectorAll("tr[data-map-row='1']");
    rows.forEach((tr) => {
      const key = String(tr.querySelector("input[data-role='key']")?.value || "").trim();
      const value = String(tr.querySelector("input[data-role='value']")?.value || "").trim();
      if (!key || !value) return;
      out[key] = value;
    });
    return out;
  }

  function renderMapTableRows(tbody, mapObj, onChange) {
    if (!tbody) return;
    const obj = mapObj && typeof mapObj === "object" && !Array.isArray(mapObj) ? mapObj : {};
    const items = Object.entries(obj);
    tbody.innerHTML = "";
    if (!items.length) {
      tbody.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    items.forEach(([key, value]) => {
      const tr = document.createElement("tr");
      tr.dataset.mapRow = "1";
      const tdKey = document.createElement("td");
      const tdValue = document.createElement("td");
      const tdOp = document.createElement("td");
      const inputKey = document.createElement("input");
      inputKey.dataset.role = "key";
      inputKey.value = String(key || "");
      const inputValue = document.createElement("input");
      inputValue.dataset.role = "value";
      inputValue.value = String(value || "");
      inputKey.oninput = onChange;
      inputValue.oninput = onChange;
      const del = document.createElement("button");
      del.className = "mini del";
      del.textContent = "删";
      del.onclick = () => {
        tr.remove();
        onChange();
      };
      tdKey.appendChild(inputKey);
      tdValue.appendChild(inputValue);
      tdOp.appendChild(del);
      tr.append(tdKey, tdValue, tdOp);
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
      els.inputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      els.outputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const onChange = () => syncIoMapFromTables();
    renderMapTableRows(els.inputMapRows, cfg?.input_map, onChange);
    renderMapTableRows(els.outputMapRows, cfg?.output_map, onChange);
  }

  return {
    readMapTableRows,
    renderMapTableRows,
    renderIoMapEditor,
    syncIoMapFromTables,
  };
}

export { createWorkflowConfigMapSupport };
