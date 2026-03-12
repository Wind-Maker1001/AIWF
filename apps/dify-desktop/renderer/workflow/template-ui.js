import { defaultTemplateParamValue, resolveTemplateRule, validateTemplateParams, applyTemplateVars } from "./template-utils.js";

function createWorkflowTemplateUi(els, deps = {}) {
  const {
    templateStorageKey = "aiwf.workflow.templates.v1",
    builtinTemplates = [],
    store,
    setStatus = () => {},
    renderAll = () => {},
    renderMigrationReport = () => {},
    graphPayload = () => ({}),
    syncRunParamsFormFromJson = () => {},
  } = deps;

  let marketplaceTemplates = [];

  function loadLocalTemplates() {
    try {
      const raw = window.localStorage.getItem(templateStorageKey);
      if (!raw) return [];
      const arr = JSON.parse(raw);
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  }

  function saveLocalTemplates(items) {
    try {
      window.localStorage.setItem(templateStorageKey, JSON.stringify(Array.isArray(items) ? items : []));
    } catch {}
  }

  async function refreshTemplateMarketplace() {
    try {
      const out = await window.aiwfDesktop.listTemplateMarketplace({ limit: 500 });
      const packs = Array.isArray(out?.items) ? out.items : [];
      const merged = [];
      packs.forEach((p) => {
        const arr = Array.isArray(p?.templates) ? p.templates : [];
        arr.forEach((t) => {
          if (!t || typeof t !== "object") return;
          merged.push({ ...t, __pack_id: String(p?.id || ""), __pack_name: String(p?.name || "") });
        });
      });
      marketplaceTemplates = merged;
    } catch {
      marketplaceTemplates = [];
    }
  }

  function parseRunParamsLoose() {
    try {
      const raw = String(els.appRunParams?.value || "").trim();
      if (!raw) return {};
      const obj = JSON.parse(raw);
      return obj && typeof obj === "object" && !Array.isArray(obj) ? obj : {};
    } catch {
      return {};
    }
  }

  function currentTemplateGovernance() {
    return {
      preflight_gate_required: els.publishRequirePreflight ? !!els.publishRequirePreflight.checked : true,
      auto_fix_enabled: true,
      acceptance_mode: "preflight_autofix_recheck_v1",
    };
  }

  function allTemplates() {
    return [...builtinTemplates, ...loadLocalTemplates(), ...marketplaceTemplates];
  }

  function renderTemplateSelect() {
    if (!els.templateSelect) return;
    const items = allTemplates();
    els.templateSelect.innerHTML = '<option value="">选择模板...</option>';
    items.forEach((t) => {
      const op = document.createElement("option");
      op.value = String(t.id || "");
      op.textContent = String(t.name || t.id || "template");
      els.templateSelect.appendChild(op);
    });
    renderTemplateParamsForm();
  }



  function readTemplateParamsLoose() {
    const text = String(els.templateParams?.value || "").trim();
    if (!text) return {};
    try {
      const obj = JSON.parse(text);
      if (obj && typeof obj === "object" && !Array.isArray(obj)) return obj;
    } catch {}
    return {};
  }

  function renderTemplateParamsForm() {
    if (!els.templateParamsForm) return;
    const id = String(els.templateSelect?.value || "").trim();
    const tpl = allTemplates().find((x) => String(x.id || "") === id);
    const schema = tpl?.params_schema;
    if (!schema || typeof schema !== "object" || !Object.keys(schema).length) {
      els.templateParamsForm.innerHTML = '<div class="cfg-help">当前模板无参数定义，可直接编辑 JSON。</div>';
      return;
    }
    const existing = readTemplateParamsLoose();
    const merged = {};
    Object.entries(schema).forEach(([key, rule]) => {
      if (Object.prototype.hasOwnProperty.call(existing, key)) merged[key] = existing[key];
      else merged[key] = defaultTemplateParamValue(rule);
    });
    if (els.templateParams) els.templateParams.value = JSON.stringify(merged, null, 2);
    els.templateParamsForm.innerHTML = "";
    const grouped = {};
    Object.entries(schema).forEach(([key, baseRule]) => {
      const rule = resolveTemplateRule(baseRule, merged);
      const g = String(rule?.group || baseRule?.group || "未分组");
      if (!grouped[g]) grouped[g] = [];
      grouped[g].push([key, baseRule, rule]);
    });
    Object.entries(grouped).forEach(([groupName, entries]) => {
      const box = document.createElement("div");
      box.style.border = "1px solid #dfe8f3";
      box.style.borderRadius = "8px";
      box.style.padding = "8px";
      box.style.marginBottom = "8px";
      const title = document.createElement("div");
      title.className = "cfg-help";
      title.style.fontWeight = "700";
      title.style.color = "#41566b";
      title.textContent = groupName;
      box.appendChild(title);
      entries.forEach(([key, baseRule, rule]) => {
        const wrap = document.createElement("div");
        wrap.className = "cfg-field";
        const label = document.createElement("label");
        const req = rule?.required ? " *" : "";
        label.textContent = `${key}${req}`;
        wrap.appendChild(label);
        if (rule?.description || baseRule?.description) {
          const desc = document.createElement("div");
          desc.className = "cfg-help";
          desc.style.marginTop = "-2px";
          desc.style.marginBottom = "5px";
          desc.dataset.tparamTip = key;
          desc.textContent = String(rule?.description || baseRule?.description || "");
          wrap.appendChild(desc);
        }
        const type = String(rule?.type || "string");
        let ctrl = null;
        if (Array.isArray(rule?.enum) && rule.enum.length) {
          ctrl = document.createElement("select");
          rule.enum.forEach((v) => {
            const op = document.createElement("option");
            op.value = String(v);
            op.textContent = String(v);
            ctrl.appendChild(op);
          });
          const current = merged[key] == null ? "" : String(merged[key]);
          if (current) ctrl.value = current;
        } else if (type === "boolean") {
          ctrl = document.createElement("select");
          ctrl.innerHTML = '<option value="true">true</option><option value="false">false</option>';
          ctrl.value = merged[key] ? "true" : "false";
        } else if (type === "object" || type === "array") {
          ctrl = document.createElement("textarea");
          ctrl.value = JSON.stringify(merged[key], null, 2);
          ctrl.style.minHeight = "66px";
        } else {
          ctrl = document.createElement("input");
          ctrl.type = type === "number" ? "number" : "text";
          if (type === "number") {
            if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
            if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
            if (Number.isFinite(rule?.step)) ctrl.step = String(rule.step);
          }
          ctrl.value = merged[key] == null ? "" : String(merged[key]);
        }
        ctrl.dataset.tparam = key;
        ctrl.dataset.ttype = String(baseRule?.type || type);
        ctrl.addEventListener("input", syncTemplateParamsJsonFromForm);
        wrap.appendChild(ctrl);
        box.appendChild(wrap);
      });
      els.templateParamsForm.appendChild(box);
    });
    applyTemplateDependencyState();
  }

  function collectTemplateParamsFromForm() {
    if (!els.templateParamsForm) return null;
    const controls = els.templateParamsForm.querySelectorAll("[data-tparam]");
    if (!controls.length) return null;
    const out = {};
    controls.forEach((ctrl) => {
      if (ctrl.disabled) return;
      const key = String(ctrl.dataset.tparam || "").trim();
      const type = String(ctrl.dataset.ttype || "string");
      if (!key) return;
      const raw = String(ctrl.value || "");
      if (type === "number") {
        const n = Number(raw);
        out[key] = Number.isFinite(n) ? n : raw;
        return;
      }
      if (type === "boolean") {
        out[key] = raw === "true";
        return;
      }
      if (type === "object" || type === "array") {
        try {
          out[key] = raw.trim() ? JSON.parse(raw) : (type === "array" ? [] : {});
        } catch {
          out[key] = raw;
        }
        return;
      }
      out[key] = raw;
    });
    return out;
  }

  function syncTemplateParamsJsonFromForm() {
    const params = collectTemplateParamsFromForm();
    if (!params || !els.templateParams) return;
    els.templateParams.value = JSON.stringify(params, null, 2);
    applyTemplateDependencyState();
  }

  function applyTemplateDependencyState() {
    if (!els.templateParamsForm) return;
    const id = String(els.templateSelect?.value || "").trim();
    if (!id) return;
    const tpl = allTemplates().find((x) => String(x.id || "") === id);
    const schema = tpl?.params_schema;
    if (!schema || typeof schema !== "object") return;
    const params = collectTemplateParamsFromForm() || readTemplateParamsLoose();
    const controls = els.templateParamsForm.querySelectorAll("[data-tparam]");
    controls.forEach((ctrl) => {
      const key = String(ctrl.dataset.tparam || "").trim();
      if (!key) return;
      const baseRule = schema[key];
      const rule = resolveTemplateRule(baseRule, params);
      const active = rule.__active !== false;
      ctrl.disabled = !active;
      if (String(ctrl.tagName || "").toLowerCase() === "input" && String(ctrl.type || "") === "number") {
        if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
        else ctrl.removeAttribute("min");
        if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
        else ctrl.removeAttribute("max");
      }
      const tip = els.templateParamsForm.querySelector(`[data-tparam-tip="${key}"]`);
      if (tip) {
        if (!active) tip.textContent = "该参数当前不生效（受联动条件控制）";
        else tip.textContent = String(rule?.description || baseRule?.description || "");
      }
    });
  }

  function applySelectedTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    if (!id) {
      setStatus("请先选择模板", false);
      return;
    }
    const it = allTemplates().find((x) => String(x.id || "") === id);
    if (!it || !it.graph) {
      setStatus("模板不存在", false);
      return;
    }
    let params = {};
    try {
      params = parseTemplateParams();
      validateTemplateParams(it.params_schema, params);
    } catch (e) {
      setStatus(String(e?.message || e || "模板参数错误"), false);
      return;
    }
    const graph = applyTemplateVars(it.graph, params);
    store.importGraph(graph);
    selectedEdge = null;
    els.workflowName.value = store.state.graph.name || String(it.name || "模板流程");
    if (it.governance && typeof it.governance === "object" && els.publishRequirePreflight) {
      els.publishRequirePreflight.checked = it.governance.preflight_gate_required !== false;
    }
    if (it.runtime_defaults && typeof it.runtime_defaults === "object") {
      els.appRunParams.value = JSON.stringify(it.runtime_defaults, null, 2);
      syncRunParamsFormFromJson();
    }
    renderAll();
    renderMigrationReport({ migrated: false });
    setStatus(`已应用模板: ${it.name || id}`, true);
  }

  function saveCurrentAsTemplate() {
    const name = String(prompt("请输入模板名称", String(els.workflowName?.value || "自定义模板")) || "").trim();
    if (!name) return;
    const custom = loadLocalTemplates();
    const id = `custom_${Date.now()}`;
    const runtimeDefaults = parseRunParamsLoose();
    const governance = currentTemplateGovernance();
    custom.push({
      id,
      name,
      graph: graphPayload(),
      template_spec_version: 1,
      governance,
      runtime_defaults: runtimeDefaults,
      created_at: new Date().toISOString(),
    });
    saveLocalTemplates(custom);
    renderTemplateSelect();
    if (els.templateSelect) els.templateSelect.value = id;
    setStatus(`模板已保存: ${name}`, true);
  }

  async function installTemplatePack() {
    const out = await window.aiwfDesktop.loadWorkflow();
    if (!out?.ok || !out?.path) {
      if (!out?.canceled) setStatus(`读取模板包失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const ret = await window.aiwfDesktop.installTemplatePack({ path: out.path });
    if (!ret?.ok) {
      setStatus(`安装模板包失败: ${ret?.error || "unknown"}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`模板包已安装: ${ret?.item?.name || ret?.item?.id || ""}`, true);
  }

  async function removeTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const tpl = allTemplates().find((x) => String(x?.id || "") === id);
    const packId = String(tpl?.__pack_id || "").trim();
    if (!packId) {
      setStatus("当前模板不是模板包来源，无法移除", false);
      return;
    }
    const out = await window.aiwfDesktop.removeTemplatePack({ id: packId });
    if (!out?.ok) {
      setStatus(`移除模板包失败: ${out?.error || "unknown"}`, false);
      return;
    }
    await refreshTemplateMarketplace();
    renderTemplateSelect();
    setStatus(`模板包已移除: ${packId}`, true);
  }

  async function exportTemplatePackByCurrentTemplate() {
    const id = String(els.templateSelect?.value || "").trim();
    const tpl = allTemplates().find((x) => String(x?.id || "") === id);
    const packId = String(tpl?.__pack_id || "").trim();
    if (!packId) {
      setStatus("当前模板不是模板包来源，无法导出", false);
      return;
    }
    const out = await window.aiwfDesktop.exportTemplatePack({ id: packId });
    if (out?.ok) setStatus(`模板包已导出: ${out.path}`, true);
    else if (!out?.canceled) setStatus(`导出模板包失败: ${out?.error || "unknown"}`, false);
  }

  function parseTemplateParams() {
    const fromForm = collectTemplateParamsFromForm();
    if (fromForm) return fromForm;
    const text = String(els.templateParams?.value || "").trim();
    if (!text) return {};
    const obj = JSON.parse(text);
    if (!obj || typeof obj !== "object" || Array.isArray(obj)) throw new Error("模板参数必须是 JSON 对象");
    return obj;
  }





  return {
    parseRunParamsLoose,
    currentTemplateGovernance,
    allTemplates,
    renderTemplateSelect,
    refreshTemplateMarketplace,
    renderTemplateParamsForm,
    collectTemplateParamsFromForm,
    syncTemplateParamsJsonFromForm,
    applyTemplateDependencyState,
    applySelectedTemplate,
    saveCurrentAsTemplate,
    installTemplatePack,
    removeTemplatePackByCurrentTemplate,
    exportTemplatePackByCurrentTemplate,
    parseTemplateParams,
  };
}

export { createWorkflowTemplateUi };
