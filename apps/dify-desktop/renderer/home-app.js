function renderPrecheck(res){
  const p = res && typeof res === "object" ? res : {};
  precheckEl.textContent = JSON.stringify(p, null, 2);
  precheckIssuesEl.innerHTML = "";
  precheckDetailEl.textContent = "点击上方问题可定位详情";
  const ok = !!p.ok;
  if(ok){
    precheckHintEl.textContent = "预检通过：字段和质量门槛可接受。";
    const passBtn = document.createElement("button");
    passBtn.className = "secondary";
    passBtn.textContent = "查看通过摘要";
    passBtn.onclick = () => {
      precheckDetailEl.textContent = `预检通过\n输入行数: ${p.input_rows || 0}\n字段数: ${Array.isArray(p.headers) ? p.headers.length : 0}\n金额可转换率: ${Number((p.amount_convert_rate || 0) * 100).toFixed(1)}%（下限 ${(Number(p.amount_convert_rate_required || 0.9) * 100).toFixed(1)}%）`;
    };
    precheckIssuesEl.appendChild(passBtn);
  }else{
    const issues = Array.isArray(p.issues) ? p.issues.length : 0;
    precheckHintEl.textContent = `预检未通过：${issues} 个问题`;
    const addIssueBtn = (title, detail) => {
      const b = document.createElement("button");
      b.className = "secondary";
      b.textContent = title;
      b.onclick = () => { precheckDetailEl.textContent = detail; };
      precheckIssuesEl.appendChild(b);
    };
    const missing = Array.isArray(p.missing_required_fields) ? p.missing_required_fields : [];
    if (missing.length > 0) {
      addIssueBtn(
        `缺少字段 (${missing.length})`,
        `缺少必填字段:\n- ${missing.join("\n- ")}\n\n当前检测到字段:\n- ${(Array.isArray(p.headers) ? p.headers : []).join("\n- ")}`
      );
    }
    const rate = Number(p.amount_convert_rate || 0);
    const requiredRate = Number(p.amount_convert_rate_required || 0.9);
    const invalidSamples = Array.isArray(p.amount_invalid_samples) ? p.amount_invalid_samples : [];
    if (rate < requiredRate) {
      const lines = invalidSamples.length > 0
        ? invalidSamples.map((s, i) => `${i + 1}. 行 ${s.row_no || "-"} | ${s.raw_value || ""} | ${s.source_file || ""}`)
        : ["无可用样本"];
      addIssueBtn(
        "金额列异常",
        `金额字段: ${p.amount_field || "-"}\n可转换率: ${(rate * 100).toFixed(1)}%（下限 ${(requiredRate * 100).toFixed(1)}%）\n异常样本:\n${lines.join("\n")}`
      );
      invalidSamples.slice(0, 5).forEach((s, i) => {
        const b = document.createElement("button");
        b.className = "secondary";
        b.textContent = `打开样本 ${i + 1}`;
        b.onclick = async () => {
          const fp = String(s?.source_file || "");
          if(!fp){ setStatus("样本缺少文件路径", false); return; }
          await window.aiwfDesktop.openPath(fp);
          setStatus(`已打开样本文件，可定位到行 ${s?.row_no || "-"}`, true);
        };
        precheckIssuesEl.appendChild(b);
      });
    }
    if (!p.quality_gate_ok) {
      addIssueBtn(
        "质量门槛未通过",
        `质量门槛错误:\n${p.quality_gate_error || "unknown"}\n\n建议:\n- 先修复上述字段或金额问题\n- 再重新执行模板预检`
      );
    }
    if (precheckIssuesEl.children.length === 0) {
      addIssueBtn("查看问题摘要", (Array.isArray(p.issues) ? p.issues : []).join("\n"));
    }
  }
}

async function init(){
  const cfg=await window.aiwfDesktop.getConfig();
  $("mode").value=cfg.mode||"offline_local";
  $("baseUrl").value=cfg.baseUrl||"http://127.0.0.1:18080";
  $("apiKey").value=cfg.apiKey||"";
  applyUiCfg(cfg);
  await loadCleaningTemplates(cfg.cleaningTemplate || "default");
  bindDropzone(dz, addToQueue);
  bindDropzone(sampleDz, addToSamplePool);
  bindFilePicker($("btnPickQueueFiles"), $("pickQueueFiles"), addToQueue);
  bindFilePicker($("btnPickSampleFiles"), $("pickSampleFiles"), addToSamplePool);
  renderQueue();
  await refreshSamplePool(true);
  inspectQueueEncoding(true);
  inspectFonts(true);
  inspectRuntime(true);
  refreshTaskStoreStatus(true);
  refreshStartupSelfCheck(true);
  refreshRouteDiagnostics();
  if (!gateLogUnsub && window.aiwfDesktop.onLocalGateLog) {
    gateLogUnsub = window.aiwfDesktop.onLocalGateLog((evt)=>{
      const sc = String(evt?.script || "");
      const type = String(evt?.type || "");
      const t = String(evt?.text || "").trimEnd();
      if (!t) return;
      appendGateLog(`[${sc}][${type}] ${t}`);
    });
  }
  if (!buildLogUnsub && window.aiwfDesktop.onLocalBuildLog) {
    buildLogUnsub = window.aiwfDesktop.onLocalBuildLog((evt)=>{
      const sc = String(evt?.script || "");
      const type = String(evt?.type || "");
      const t = String(evt?.text || "").trimEnd();
      if (!t) return;
      appendGateLog(`[build:${sc}][${type}] ${t}`);
    });
  }
  try {
    const rt = await window.aiwfDesktop.getLocalGateRuntime();
    gateRunning = !!rt?.running;
    setGateButtonsDisabled(gateRunning);
  } catch {}
  try {
    const br = await window.aiwfDesktop.getLocalBuildRuntime();
    buildRunning = !!br?.running;
    const st = await window.aiwfDesktop.getBuildGuardStatus();
    const missing = Array.isArray(st?.missing) ? st.missing : [];
    if (buildRunning) setBuildButtonsDisabled(true);
    else setBuildButtonsDisabled(missing.length > 0);
  } catch {}
  refreshGateSummary();
  refreshBuildGuardStatus();
  updateShellTabUi();
  window.__aiwfHomeReady = true;
}

function renderDebatePreview(out){
  const p = out && typeof out === "object" ? out : {};
  debatePreviewEl.textContent = JSON.stringify(p, null, 2);
  const total = Number(p?.preview?.total || 0);
  const groups = Array.isArray(p?.preview?.groups) ? p.preview.groups : [];
  if (p?.ok) {
    const top = groups.slice(0, 5).map((g)=>`${g.battlefield}:${g.count}`).join(" | ");
    debatePreviewHintEl.textContent = `预览完成：候选 ${total} 条，战场 ${groups.length} 个${top ? `（${top}）` : ""}`;
  } else {
    debatePreviewHintEl.textContent = `预览失败：${p?.error || "unknown"}`;
  }
}

async function runDebatePreview() {
  try{
    debatePreviewHintEl.textContent = "预览中...";
    const out = await window.aiwfDesktop.previewDebateStyle(payloadFromUi(), cfgFromUi());
    renderDebatePreview(out);
    setStatus(out?.ok ? "辩论样式预览完成" : `辩论样式预览失败: ${out?.error || "unknown"}`, !!out?.ok);
  }catch(e){
    debatePreviewHintEl.textContent = `预览失败: ${e}`;
    setStatus(`辩论样式预览失败: ${e}`, false);
  }
}

function applyDebatePreset() {
  $("cleaningTemplate").value = "debate_evidence_v1";
  $("officeTheme").value = "fluent_ms";
  $("officeVariant").value = "light";
  $("officeLang").value = "zh";
  if (!$("reportTitle").value.trim()) $("reportTitle").value = "辩论资料库";
  if (!$("debateBattlefieldRules").value.trim()) {
    $("debateBattlefieldRules").value = "谣言=内容失真\n成瘾=能力减损\n社交=社交与表达";
  }
  if (!$("debateSourcePriority").value.trim()) {
    $("debateSourcePriority").value = "source_org,publisher,source,author,source_type";
  }
}

async function runDebateQuick() {
  applyDebatePreset();
  await window.aiwfDesktop.saveConfig(saveCfgFromUi());
  await runDebatePreview();
  await runMain();
}

function summarizeCleaningFailure(out){
  const p = out && typeof out === "object" ? out : {};
  const errorCode = String(p.error_code || "").trim();
  const reasonCodes = Array.isArray(p.reason_codes) ? p.reason_codes : [];
  const requestedProfile = String(p.requested_profile || p.quality_summary?.requested_profile || "").trim();
  const recommendedProfile = String(p.recommended_profile || p.quality_summary?.recommended_profile || "").trim();
  const templateId = String(p.template || p.template_id || "").trim();
  if (errorCode === "profile_mismatch_blocked") {
    const pair = requestedProfile && recommendedProfile
      ? `${requestedProfile} -> ${recommendedProfile}`
      : (requestedProfile || recommendedProfile || "profile_mismatch");
    return `清洗被拦截: 模板画像不匹配 (${pair})${templateId ? ` | 模板 ${templateId}` : ""}`;
  }
  if (errorCode === "zero_output_unexpected" || reasonCodes.includes("zero_output_unexpected")) {
    return `清洗被拦截: 输出为空且不允许空结果${templateId ? ` | 模板 ${templateId}` : ""}`;
  }
  if (errorCode) return `清洗失败: ${errorCode}`;
  return `生成失败: ${p.error || "unknown"}`;
}

function buildDebateConfigPayload() {
  return {
    version: "debate_config_v1",
    exported_at: new Date().toISOString(),
    cleaningTemplate: "debate_evidence_v1",
    officeTheme: "fluent_ms",
    officeVariant: $("officeVariant").value || "light",
    officeLang: $("officeLang").value || "zh",
    reportTitle: $("reportTitle").value.trim() || "辩论资料库",
    debateBattlefieldRules: $("debateBattlefieldRules").value || "",
    debateSourcePriority: $("debateSourcePriority").value.trim() || "",
  };
}

function applyDebateConfigPayload(cfg = {}) {
  $("cleaningTemplate").value = "debate_evidence_v1";
  if (cfg.officeTheme) $("officeTheme").value = String(cfg.officeTheme);
  if (cfg.officeVariant) $("officeVariant").value = String(cfg.officeVariant);
  if (cfg.officeLang) $("officeLang").value = String(cfg.officeLang);
  if (cfg.reportTitle !== undefined) $("reportTitle").value = String(cfg.reportTitle || "");
  if (cfg.debateBattlefieldRules !== undefined) $("debateBattlefieldRules").value = String(cfg.debateBattlefieldRules || "");
  if (cfg.debateSourcePriority !== undefined) $("debateSourcePriority").value = String(cfg.debateSourcePriority || "");
}

$("btnQueueClear").onclick=()=>{ queuePaths.splice(0, queuePaths.length); renderQueue(); inspectQueueEncoding(true); setStatus("已清空任务队列", true); };
$("btnSampleRefresh").onclick=()=>refreshSamplePool(false);
$("btnSampleOpen").onclick=async()=>{
  const out = await window.aiwfDesktop.getSamplePoolInfo(cfgFromUi());
  if(out?.root){
    await window.aiwfDesktop.openPath(out.root);
    setStatus("已打开样本池目录", true);
  }else{
    setStatus("样本池目录不可用", false);
  }
};
$("btnSampleClear").onclick=async()=>{
  const ok = window.confirm("确定清空样本池吗？该操作会删除样本池目录中的文件。");
  if(!ok) return;
  const out = await window.aiwfDesktop.samplePoolClear(cfgFromUi());
  await refreshSamplePool(true);
  if(out?.ok) setStatus(`样本池已清空（删除 ${Number(out.removed||0)} 个文件）`, true);
  else setStatus(`清空样本池失败: ${out?.error || "unknown"}`, false);
};
$("btnInstallFonts").onclick=async()=>{
  setStatus("正在安装内置字体...");
  try{
    const r = await window.aiwfDesktop.installBundledFonts();
    if(!r || !r.ok){
      setStatus("安装字体失败: "+(r?.reason||"unknown"), false);
      return;
    }
    await inspectFonts(true);
    const n = Number(r.installed||0);
    const s = Number(r.skipped||0);
    setStatus(`字体安装完成（新增 ${n}，已存在 ${s}）`, true);
  }catch(e){
    setStatus("安装字体失败: "+e, false);
  }
};
$("btnSave").onclick=async()=>{await window.aiwfDesktop.saveConfig(saveCfgFromUi());setStatus("配置已保存",true);};
$("btnHealth").onclick=async()=>{
  setStatus("检查中...");
  try{const j=await window.aiwfDesktop.health(cfgFromUi());show(j);setStatus(j.ok?"可用":"不可用",!!j.ok);}catch(e){setStatus("检查失败: "+e,false)}
  await refreshTaskStoreStatus(true);
};
$("btnDifyWizard").onclick=async()=>{
  try{
    setStatus("向导执行中：先检查连通性...");
    const h = await window.aiwfDesktop.health(cfgFromUi());
    if(!h?.ok){
      setStatus("后端连通失败。可继续使用离线模式，或检查 Base URL / API Key。", false);
      show(h || {});
      return;
    }
    setStatus("连通通过，正在做一键联调回收...");
    const payload = payloadFromUi();
    const out = await window.aiwfDesktop.runCleaning(payload,cfgFromUi());
    show(out); renderMetrics(out);
    if(out?.ok){
      setStatus("Dify 连接向导完成：联调通过。", true);
    }else{
      setStatus("Dify 连接向导完成：请求已发出但结果失败。", false);
    }
  }catch(e){
    setStatus("Dify 连接向导失败: "+e,false);
  }
};
if ($("btnWorkflow")) {
  $("btnWorkflow").onclick=async()=>{
    try{
      switchShellTab("workflow", { focusWorkflow: true });
      setStatus("已切换到 Legacy Workflow Studio", true);
    }catch(e){
      setStatus("打开 Legacy Workflow Studio 失败: "+e, false);
    }
  };
}
$("btnRouteDiag").onclick=async()=>{ await refreshRouteDiagnostics(); };
$("btnSelfCheck").onclick=async()=>{ await refreshStartupSelfCheck(false); };
$("btnGateUnit").onclick=async()=>{ await runGateScript("test:unit"); };
$("btnGateSmoke").onclick=async()=>{ await runGateScript("smoke"); };
$("btnGateReg").onclick=async()=>{ await runGateScript("test:regression"); };
$("btnGateRegDirty").onclick=async()=>{ await runGateScript("test:regression:dirty"); };
$("btnGateOffice").onclick=async()=>{ await runGateScript("test:office-gate"); };
$("btnGateAcceptance").onclick=async()=>{ await runGateScript("acceptance:real"); };
$("btnGateRefresh").onclick=async()=>{ await refreshGateSummary(); await refreshSamplePool(true); };
$("btnGateAll").onclick=async()=>{ await runGateAll(); };
$("btnGateCancel").onclick=async()=>{ await cancelGateRun(); };
$("btnGatePackGuard").onclick=async()=>{ await ensureReleaseGatePassedBeforePack(); };
$("btnBuildPortable").onclick=async()=>{ await runLocalBuildScript("build:win:release:gated"); };
$("btnBuildInstaller").onclick=async()=>{ await runLocalBuildScript("build:win:installer:release:gated"); };
$("btnBuildCancel").onclick=async()=>{ await cancelBuildRun(); };
$("btnOpenBuildDir").onclick=async()=>{ await openDesktopBuildDir(); };
$("btnOpenLatestArtifacts").onclick=async()=>{
  try{
    const out = await window.aiwfDesktop.getLatestArtifactsDir();
    if(!out?.ok || !out?.artifacts_dir){
      setStatus(`暂无最新成品目录: ${out?.error || "unknown"}`, false);
      return;
    }
    await window.aiwfDesktop.openPath(String(out.artifacts_dir));
    setStatus(`已打开最新成品目录: ${out.artifacts_dir}`, true);
  }catch(e){
    setStatus(`打开最新成品目录失败: ${e}`, false);
  }
};
$("btnExportReleaseMd").onclick=async()=>{ await exportReleaseReport("md"); };
$("btnExportReleaseJson").onclick=async()=>{ await exportReleaseReport("json"); };
$("btnOpenRouteLog").onclick=async()=>{
  try{
    if(!latestRouteSummaryMeta) latestRouteSummaryMeta = await window.aiwfDesktop.getRouteMetricsSummary();
    const dir = latestRouteSummaryMeta?.log_dir || "";
    if(!dir){ setStatus("未找到路由日志目录", false); return; }
    await window.aiwfDesktop.openPath(dir);
    setStatus("已打开路由日志目录", true);
  }catch(e){
    setStatus("打开路由日志目录失败: "+e, false);
  }
};
if (btnOpenNoiseEl) {
  btnOpenNoiseEl.onclick = async () => {
    try {
      const noise = (latestArtifacts || []).find((x) => String(x?.artifact_id || "") === "md_filtered_noise_001");
      const p = String(noise?.path || "");
      if (!p) { setStatus("当前任务没有问号噪声清单", false); return; }
      await window.aiwfDesktop.openPath(p);
      setStatus("已打开问号噪声清单", true);
    } catch (e) {
      setStatus("打开问号噪声清单失败: " + e, false);
    }
  };
}
async function runMain(){
  setStatus("正在生成...");
  try{
    const enc = await inspectQueueEncoding(false);
    if($("strictEncodingMode").checked && (enc?.uncertainCount||0) > 0){
      setStatus("严格编码模式已开启：存在不确定编码文件，已阻止运行。请先转换为 UTF-8。", false);
      return;
    }
    const payload = payloadFromUi();
    const files = combinedInputFiles();
    if ($("autoNormalizeEncoding").checked && files.length > 0) {
      const norm = await window.aiwfDesktop.normalizeEncoding(files);
      if (norm?.ok) {
        const mapped = (norm.items || []).map(x => String(x.output || x.source || "")).filter(Boolean);
        payload.params.input_files = mapped.join("\n");
        if ((norm.convertedCount || 0) > 0) {
          setStatus(`已自动转换 ${norm.convertedCount} 个文件编码为 UTF-8，继续生成...`, true);
        }
      }
    }
    const j=await window.aiwfDesktop.runCleaning(payload,cfgFromUi());
    show(j);renderMetrics(j);
    setStatus(j.ok ? "生成完成" : summarizeCleaningFailure(j), !!j.ok);
  }catch(e){setStatus("运行失败: "+e,false)}
}

$("btnRun").onclick=async()=>{ await runMain(); };

$("btnPrecheck").onclick=async()=>{
  setStatus("正在执行模板预检...");
  try{
    const enc = await inspectQueueEncoding(false);
    if($("strictEncodingMode").checked && (enc?.uncertainCount||0) > 0){
      setStatus("严格编码模式已开启：存在不确定编码文件，已阻止预检。请先转换为 UTF-8。", false);
      return;
    }
    const payload = payloadFromUi();
    const files = combinedInputFiles();
    if ($("autoNormalizeEncoding").checked && files.length > 0) {
      const norm = await window.aiwfDesktop.normalizeEncoding(files);
      if (norm?.ok) {
        const mapped = (norm.items || []).map(x => String(x.output || x.source || "")).filter(Boolean);
        payload.params.input_files = mapped.join("\n");
      }
    }
    const j = await window.aiwfDesktop.precheckCleaning(payload, cfgFromUi());
    show(j);
    renderPrecheck(j?.precheck || {});
    setStatus(j?.precheck?.ok ? "预检通过，可以开始生成。" : "预检发现问题，请先修复。", !!j?.precheck?.ok);
  }catch(e){
    precheckHintEl.textContent = "预检失败";
    precheckEl.textContent = JSON.stringify({ ok:false, error:String(e) }, null, 2);
    setStatus("预检失败: "+e, false);
  }
};
$("btnDebatePreview").onclick=async()=>{ await runDebatePreview(); };
$("btnRunDebateQuick").onclick=async()=>{ await runDebateQuick(); };
$("btnDebateCfgExport").onclick=()=>{
  try{
    const obj = buildDebateConfigPayload();
    const blob = new Blob([JSON.stringify(obj, null, 2)], { type: "application/json" });
    const a = document.createElement("a");
    a.href = URL.createObjectURL(blob);
    a.download = `debate_config_${Date.now()}.json`;
    a.click();
    URL.revokeObjectURL(a.href);
    setStatus("辩论配置已导出", true);
  }catch(e){
    setStatus("导出辩论配置失败: " + e, false);
  }
};
$("btnDebateCfgImport").onclick=()=>{ $("debateCfgImportFile").click(); };
$("debateCfgImportFile").onchange=async(e)=>{
  try{
    const f = e?.target?.files?.[0];
    if(!f) return;
    const txt = await f.text();
    const obj = JSON.parse(txt || "{}");
    applyDebateConfigPayload(obj || {});
    await window.aiwfDesktop.saveConfig(saveCfgFromUi());
    setStatus("辩论配置已导入并保存", true);
  }catch(err){
    setStatus("导入辩论配置失败: " + err, false);
  }finally{
    $("debateCfgImportFile").value = "";
  }
};

$("tplManageSelect").onchange=()=>{ renderTemplateDetail($("tplManageSelect").value); };
$("btnTplRefresh").onclick=async()=>{ await loadCleaningTemplates($("cleaningTemplate").value); setStatus("模板列表已刷新", true); };
$("btnTplToggle").onclick=async()=>{
  const id = String($("tplManageSelect").value || "").trim().toLowerCase();
  if(!id || id === "default"){ setStatus("默认模板不可停用", false); return; }
  const set = getDisabledTemplateSet();
  if(set.has(id)) set.delete(id); else set.add(id);
  window.__disabledTemplates = Array.from(set);
  await loadCleaningTemplates($("cleaningTemplate").value);
  await window.aiwfDesktop.saveConfig(saveCfgFromUi());
  setStatus(set.has(id) ? "模板已停用" : "模板已启用", true);
};
$("btnTplEnableAll").onclick=async()=>{
  window.__disabledTemplates = [];
  await loadCleaningTemplates($("cleaningTemplate").value);
  await window.aiwfDesktop.saveConfig(saveCfgFromUi());
  setStatus("已恢复全部模板", true);
};
$("btnTplExport").onclick=()=>{
  const id = String($("tplManageSelect").value || "").trim();
  const tpl = findTemplateById(id);
  if(!tpl){ setStatus("未找到模板，无法导出", false); return; }
  const exportPayload = {
    id: tpl.id,
    label: tpl.label,
    description: tpl.description,
    params_schema: tpl.params_schema && typeof tpl.params_schema === "object" ? tpl.params_schema : {},
  };
  if (tpl.cleaning_spec_v2 && typeof tpl.cleaning_spec_v2 === "object") exportPayload.cleaning_spec_v2 = tpl.cleaning_spec_v2;
  else exportPayload.rules = tpl.rules || {};
  const blob = new Blob([JSON.stringify(exportPayload, null, 2)], { type: "application/json" });
  const a = document.createElement("a");
  a.href = URL.createObjectURL(blob);
  a.download = `${tpl.id || "template"}.json`;
  a.click();
  URL.revokeObjectURL(a.href);
  setStatus("模板 JSON 已导出", true);
};
$("tplImportFile").onchange=async(e)=>{
  try{
    const f = e?.target?.files?.[0];
    if(!f) return;
    const txt = await f.text();
    const obj = JSON.parse(txt);
    const id = String(obj.id || obj.template_id || "").trim().toLowerCase();
    const hasSpec = obj.cleaning_spec_v2 && typeof obj.cleaning_spec_v2 === "object";
    const hasRules = obj.rules && typeof obj.rules === "object";
    if (hasSpec && !hasRules) obj.rules = {};
    if(!id){ setStatus("导入失败：模板缺少 id", false); return; }
    if(!obj.rules || typeof obj.rules !== "object"){ setStatus("导入失败：模板缺少 rules", false); return; }
    const entry = {
      id,
      label: String(obj.label || obj.template_label || id),
      description: String(obj.description || obj.template_description || "导入模板"),
      rules: hasRules ? obj.rules : null,
      cleaning_spec_v2: hasSpec ? obj.cleaning_spec_v2 : null,
      params_schema: obj.params_schema && typeof obj.params_schema === "object" ? obj.params_schema : {},
      template_format: hasSpec ? "cleaning_spec_v2" : "legacy_rules",
    };
    const list = Array.isArray(window.__customTemplates) ? window.__customTemplates : [];
    const next = list.filter(x=>String(x?.id||"").toLowerCase()!==id);
    next.push(entry);
    window.__customTemplates = next;
    await loadCleaningTemplates(id);
    await window.aiwfDesktop.saveConfig(saveCfgFromUi());
    setStatus(`模板已导入：${entry.label}`, true);
  }catch(err){
    setStatus("导入模板失败: "+err, false);
  }finally{
    $("tplImportFile").value = "";
  }
};

$("mode").addEventListener("change", ()=>{ refreshTaskStoreStatus(true); });

init();
