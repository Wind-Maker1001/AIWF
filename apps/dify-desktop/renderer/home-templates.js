function applyTemplateOptions(templates, preferred = "default"){
  if(!Array.isArray(templates) || templates.length===0) return;
  const current = String(preferred || $("cleaningTemplate").value || "default");
  const disabled = getDisabledTemplateSet();
  const visible = templates.filter((tpl)=>{
    const id = String(tpl.id || "").toLowerCase();
    if(id === "default") return true;
    if(id === current.toLowerCase()) return true;
    return !disabled.has(id);
  });
  $("cleaningTemplate").innerHTML = "";
  visible.forEach((tpl)=>{
    const opt = document.createElement("option");
    opt.value = String(tpl.id || "");
    opt.textContent = String(tpl.label || tpl.id || "模板");
    if(tpl.description){
      opt.title = String(tpl.description);
    }
    $("cleaningTemplate").appendChild(opt);
  });
  const exists = visible.some(t => String(t.id) === current);
  $("cleaningTemplate").value = exists ? current : "default";
}

function renderTemplateManager(preferred = ""){
  const all = Array.isArray(templateCatalog) ? templateCatalog : [];
  const select = $("tplManageSelect");
  select.innerHTML = "";
  all.forEach((tpl)=>{
    const opt = document.createElement("option");
    const id = String(tpl.id || "");
    const disabled = getDisabledTemplateSet().has(id.toLowerCase());
    opt.value = id;
    opt.textContent = disabled ? `${tpl.label || id}（已停用）` : (tpl.label || id);
    select.appendChild(opt);
  });
  const pick = String(preferred || select.value || "default");
  if (Array.from(select.options).some(o=>o.value===pick)) select.value = pick;
  renderTemplateDetail(select.value);
}

function renderTemplateDetail(templateId){
  const tpl = findTemplateById(templateId);
  if(!tpl){
    tplManageMetaEl.textContent = "未找到模板";
    tplManageRulesEl.textContent = "{}";
    return;
  }
  const id = String(tpl.id || "");
  const disabled = getDisabledTemplateSet().has(id.toLowerCase());
  tplManageMetaEl.textContent = `ID: ${id} | 文件: ${tpl.file || "-"} | 状态: ${disabled ? "已停用" : "启用中"}`;
  tplManageRulesEl.textContent = JSON.stringify(tpl.rules || {}, null, 2);
  $("btnTplToggle").textContent = disabled ? "启用当前模板" : "停用当前模板";
}

async function loadCleaningTemplates(preferred = "default"){
  try{
    const r = await window.aiwfDesktop.listCleaningTemplates();
    if(!r?.ok){
      setStatus("模板列表加载失败，已回退到内置选项。", false);
      return;
    }
    const base = Array.isArray(r.templates) ? r.templates : [];
    const customs = Array.isArray(window.__customTemplates) ? window.__customTemplates : [];
    templateCatalog = [...base];
    customs.forEach((tpl)=>{
      const id = String(tpl?.id || "").trim().toLowerCase();
      if(!id) return;
      if(templateCatalog.some(x=>String(x.id).toLowerCase()===id)) return;
      templateCatalog.push({
        id,
        label: String(tpl.label || id),
        description: String(tpl.description || "导入模板"),
        file: "user_import",
        rules: tpl.rules && typeof tpl.rules === "object" ? tpl.rules : null,
      });
    });
    applyTemplateOptions(templateCatalog, preferred);
    renderTemplateManager(preferred);
  }catch(e){
    setStatus("模板列表加载失败，已回退到内置选项。", false);
  }
}


