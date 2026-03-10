function createWorkflowAppFormUi(els, deps = {}) {
  const { setStatus = () => {} } = deps;

  function normalizeAppSchemaObject(obj) {
    const src = obj && typeof obj === "object" && !Array.isArray(obj) ? obj : {};
    const out = {};
    Object.entries(src).forEach(([key, raw]) => {
      const k = String(key || "").trim();
      if (!k) return;
      const rule = raw && typeof raw === "object" && !Array.isArray(raw) ? { ...raw } : {};
      const type = String(rule.type || "string").trim() || "string";
      const item = { type };
      if (rule.required === true) item.required = true;
      if (Object.prototype.hasOwnProperty.call(rule, "default")) item.default = rule.default;
      if (rule.description != null && String(rule.description).trim()) item.description = String(rule.description);
      out[k] = item;
    });
    return out;
  }

  function appSchemaRowsFromObject(schemaObj) {
    return Object.entries(normalizeAppSchemaObject(schemaObj)).map(([key, rule]) => ({
      key,
      type: String(rule.type || "string"),
      required: rule.required === true,
      defaultText: Object.prototype.hasOwnProperty.call(rule, "default") ? JSON.stringify(rule.default) : "",
      description: String(rule.description || ""),
    }));
  }

  function collectAppSchemaFromForm() {
    if (!els.appSchemaForm) return {};
    const rows = els.appSchemaForm.querySelectorAll("div[data-app-schema-row='1']");
    const out = {};
    rows.forEach((row) => {
      const key = String(row.querySelector("[data-app-schema='key']")?.value || "").trim();
      const type = String(row.querySelector("[data-app-schema='type']")?.value || "string").trim() || "string";
      const required = !!row.querySelector("[data-app-schema='required']")?.checked;
      const defaultText = String(row.querySelector("[data-app-schema='default']")?.value || "").trim();
      const description = String(row.querySelector("[data-app-schema='description']")?.value || "").trim();
      if (!key) return;
      const item = { type };
      if (required) item.required = true;
      if (defaultText) {
        try {
          item.default = JSON.parse(defaultText);
        } catch {
          item.default = defaultText;
        }
      }
      if (description) item.description = description;
      out[key] = item;
    });
    return out;
  }

  function renderAppSchemaForm(rows) {
    if (!els.appSchemaForm) return;
    const list = Array.isArray(rows) ? rows : [];
    els.appSchemaForm.innerHTML = "";
    if (!list.length) {
      els.appSchemaForm.innerHTML = '<div class="cfg-help">暂无参数，可点击“新增参数”。</div>';
      return;
    }
    list.forEach((r) => {
      const row = document.createElement("div");
      row.dataset.appSchemaRow = "1";
      row.style.border = "1px solid #dfe8f3";
      row.style.borderRadius = "8px";
      row.style.padding = "6px";
      row.style.marginBottom = "6px";
      row.innerHTML = `
        <div style="display:grid;grid-template-columns:1.2fr 0.9fr 0.7fr auto;gap:6px">
          <input data-app-schema="key" placeholder="参数名" value="${String(r.key || "").replace(/"/g, "&quot;")}" />
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
        <input data-app-schema="default" placeholder="默认值(JSON或纯文本)" value="${String(r.defaultText || "").replace(/"/g, "&quot;")}" style="margin-top:6px" />
        <input data-app-schema="description" placeholder="说明" value="${String(r.description || "").replace(/"/g, "&quot;")}" style="margin-top:6px" />
      `;
      const typeSel = row.querySelector("[data-app-schema='type']");
      if (typeSel) typeSel.value = String(r.type || "string");
      const req = row.querySelector("[data-app-schema='required']");
      if (req) req.checked = !!r.required;
      const delBtn = row.querySelector("[data-app-schema='del']");
      if (delBtn) {
        delBtn.addEventListener("click", () => {
          row.remove();
          syncAppSchemaJsonFromForm();
          syncRunParamsFormFromJson();
          if (!els.appSchemaForm.querySelector("div[data-app-schema-row='1']")) renderAppSchemaForm([]);
        });
      }
      row.querySelectorAll("[data-app-schema]").forEach((ctrl) => {
        if (String(ctrl.getAttribute("data-app-schema")) === "del") return;
        ctrl.addEventListener("input", () => {
          syncAppSchemaJsonFromForm();
          syncRunParamsFormFromJson();
        });
        ctrl.addEventListener("change", () => {
          syncAppSchemaJsonFromForm();
          syncRunParamsFormFromJson();
        });
      });
      els.appSchemaForm.appendChild(row);
    });
  }

  function syncAppSchemaJsonFromForm() {
    if (!els.appSchemaJson) return;
    const schema = collectAppSchemaFromForm();
    els.appSchemaJson.value = JSON.stringify(schema, null, 2);
  }

  function syncAppSchemaFormFromJson() {
    const text = String(els.appSchemaJson?.value || "").trim();
    if (!text) {
      renderAppSchemaForm([]);
      return;
    }
    let parsed = {};
    try {
      parsed = JSON.parse(text);
    } catch (e) {
      setStatus(`Schema JSON 解析失败: ${e}`, false);
      return;
    }
    renderAppSchemaForm(appSchemaRowsFromObject(parsed));
    syncAppSchemaJsonFromForm();
  }

  function defaultRunParamValue(rule) {
    if (rule && Object.prototype.hasOwnProperty.call(rule, "default")) return rule.default;
    const t = String(rule?.type || "string");
    if (t === "number") return 0;
    if (t === "boolean") return false;
    if (t === "object") return {};
    if (t === "array") return [];
    return "";
  }

  function collectRunParamsForm() {
    if (!els.appRunParamsForm) return {};
    const controls = els.appRunParamsForm.querySelectorAll("[data-app-run-param]");
    const out = {};
    controls.forEach((ctrl) => {
      const key = String(ctrl.dataset.appRunParam || "").trim();
      const type = String(ctrl.dataset.appRunType || "string");
      if (!key) return;
      if (type === "boolean") {
        out[key] = String(ctrl.value || "false") === "true";
        return;
      }
      if (type === "number") {
        const n = Number(ctrl.value || 0);
        out[key] = Number.isFinite(n) ? n : 0;
        return;
      }
      if (type === "object" || type === "array") {
        const raw = String(ctrl.value || "").trim();
        if (!raw) {
          out[key] = type === "array" ? [] : {};
          return;
        }
        try {
          out[key] = JSON.parse(raw);
        } catch {
          out[key] = raw;
        }
        return;
      }
      out[key] = String(ctrl.value || "");
    });
    return out;
  }

  function syncRunParamsJsonFromForm() {
    if (!els.appRunParams) return;
    els.appRunParams.value = JSON.stringify(collectRunParamsForm(), null, 2);
  }

  function renderRunParamsFormBySchema(schemaObj, preferredParams) {
    if (!els.appRunParamsForm) return;
    const schema = normalizeAppSchemaObject(schemaObj);
    const entries = Object.entries(schema);
    const preferred = preferredParams && typeof preferredParams === "object" && !Array.isArray(preferredParams) ? preferredParams : {};
    if (!entries.length) {
      els.appRunParamsForm.innerHTML = '<div class="cfg-help">当前应用无参数定义，可直接编辑 JSON。</div>';
      return;
    }
    const params = {};
    entries.forEach(([k, rule]) => {
      if (Object.prototype.hasOwnProperty.call(preferred, k)) params[k] = preferred[k];
      else params[k] = defaultRunParamValue(rule);
    });
    els.appRunParamsForm.innerHTML = "";
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
      ctrl.addEventListener("input", syncRunParamsJsonFromForm);
      ctrl.addEventListener("change", syncRunParamsJsonFromForm);
      wrap.appendChild(ctrl);
      els.appRunParamsForm.appendChild(wrap);
    });
    syncRunParamsJsonFromForm();
  }

  function syncRunParamsFormFromJson() {
    const schema = normalizeAppSchemaObject((() => {
      try { return JSON.parse(String(els.appSchemaJson?.value || "{}")); } catch { return {}; }
    })());
    let params = {};
    try {
      const raw = String(els.appRunParams?.value || "{}").trim();
      params = raw ? JSON.parse(raw) : {};
    } catch (e) {
      setStatus(`运行参数 JSON 解析失败: ${e}`, false);
      return;
    }
    renderRunParamsFormBySchema(schema, params);
  }

  return {
    normalizeAppSchemaObject,
    appSchemaRowsFromObject,
    collectAppSchemaFromForm,
    renderAppSchemaForm,
    syncAppSchemaJsonFromForm,
    syncAppSchemaFormFromJson,
    defaultRunParamValue,
    collectRunParamsForm,
    syncRunParamsJsonFromForm,
    renderRunParamsFormBySchema,
    syncRunParamsFormFromJson,
  };
}

export { createWorkflowAppFormUi };
