function escapeAttr(value) {
  return String(value || "").replace(/"/g, "&quot;");
}

function renderAppSchemaFormInto(formEl, rows = [], hooks = {}) {
  const {
    onSchemaChanged = () => {},
    onEmptyRequested = () => {},
  } = hooks;

  if (!formEl) return;
  const list = Array.isArray(rows) ? rows : [];
  formEl.innerHTML = "";
  if (!list.length) {
    formEl.innerHTML = '<div class="cfg-help">暂无参数，可点击“新增参数”。</div>';
    return;
  }

  list.forEach((rowData) => {
    const row = document.createElement("div");
    row.dataset.appSchemaRow = "1";
    row.style.border = "1px solid #dfe8f3";
    row.style.borderRadius = "8px";
    row.style.padding = "6px";
    row.style.marginBottom = "6px";
    row.innerHTML = `
      <div style="display:grid;grid-template-columns:1.2fr 0.9fr 0.7fr auto;gap:6px">
        <input data-app-schema="key" placeholder="参数名" value="${escapeAttr(rowData.key)}" />
        <select data-app-schema="type">
          <option value="string">string</option>
          <option value="number">number</option>
          <option value="boolean">boolean</option>
          <option value="object">object</option>
          <option value="array">array</option>
        </select>
        <label style="margin:0;display:flex;align-items:center;font-size:12px"><input data-app-schema="required" type="checkbox" style="width:auto;margin-right:6px" />必填</label>
        <button data-app-schema="del" class="mini del" type="button">删除</button>
      </div>
      <input data-app-schema="default" placeholder="默认值(JSON或纯文本)" value="${escapeAttr(rowData.defaultText)}" style="margin-top:6px" />
      <input data-app-schema="description" placeholder="说明" value="${escapeAttr(rowData.description)}" style="margin-top:6px" />
    `;
    const typeSel = row.querySelector("[data-app-schema='type']");
    if (typeSel) typeSel.value = String(rowData.type || "string");
    const req = row.querySelector("[data-app-schema='required']");
    if (req) req.checked = !!rowData.required;
    const delBtn = row.querySelector("[data-app-schema='del']");
    if (delBtn) {
      delBtn.addEventListener("click", () => {
        row.remove();
        onSchemaChanged();
        if (!formEl.querySelector("div[data-app-schema-row='1']")) onEmptyRequested();
      });
    }
    row.querySelectorAll("[data-app-schema]").forEach((ctrl) => {
      if (String(ctrl.getAttribute("data-app-schema")) === "del") return;
      ctrl.addEventListener("input", onSchemaChanged);
      ctrl.addEventListener("change", onSchemaChanged);
    });
    formEl.appendChild(row);
  });
}

export { renderAppSchemaFormInto };
