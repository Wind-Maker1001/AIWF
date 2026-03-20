function renderRunParamsFormInto(formEl, schema = {}, params = {}, onSync = () => {}) {
  if (!formEl) return;
  const entries = Object.entries(schema || {});
  if (!entries.length) {
    formEl.innerHTML = '<div class="cfg-help">当前应用无参数定义，可直接编辑 JSON。</div>';
    return;
  }

  formEl.innerHTML = "";
  entries.forEach(([key, rule]) => {
    const wrap = document.createElement("div");
    wrap.className = "cfg-field";
    const label = document.createElement("label");
    label.textContent = `${key}${rule.required ? " *" : ""}`;
    wrap.appendChild(label);
    if (rule.description) {
      const tip = document.createElement("div");
      tip.className = "cfg-help";
      tip.textContent = String(rule.description);
      wrap.appendChild(tip);
    }
    const type = String(rule.type || "string");
    let ctrl;
    if (type === "boolean") {
      ctrl = document.createElement("select");
      ctrl.innerHTML = '<option value="true">true</option><option value="false">false</option>';
      ctrl.value = params[key] ? "true" : "false";
    } else if (type === "object" || type === "array") {
      ctrl = document.createElement("textarea");
      ctrl.style.minHeight = "66px";
      ctrl.value = JSON.stringify(params[key], null, 2);
    } else {
      ctrl = document.createElement("input");
      ctrl.type = type === "number" ? "number" : "text";
      ctrl.value = params[key] == null ? "" : String(params[key]);
    }
    ctrl.dataset.appRunParam = key;
    ctrl.dataset.appRunType = type;
    ctrl.addEventListener("input", onSync);
    ctrl.addEventListener("change", onSync);
    wrap.appendChild(ctrl);
    formEl.appendChild(wrap);
  });
}

export { renderRunParamsFormInto };
