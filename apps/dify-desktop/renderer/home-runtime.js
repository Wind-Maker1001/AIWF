function renderQueue(){
  if(queuePaths.length===0){
    queueRowsEl.innerHTML='<tr><td colspan="4" style="color:#75869a">队列为空</td></tr>';
    return;
  }
  queueRowsEl.innerHTML="";
  queuePaths.forEach((p, i) => {
    const tr = document.createElement("tr");
    const td1 = document.createElement("td");
    const td2 = document.createElement("td");
    const td3 = document.createElement("td");
    const td4 = document.createElement("td");
    const btn = document.createElement("button");
    const name=(p.split(/[/\\]/).pop()||p);
    td1.textContent = String(i + 1);
    td2.textContent = name;
    td3.className = "path";
    td3.title = p;
    td3.textContent = p;
    btn.className = "secondary";
    btn.textContent = "移除";
    btn.onclick = () => {
      if (Number.isInteger(i)) {
        queuePaths.splice(i, 1);
        renderQueue();
      }
    };
    td4.appendChild(btn);
    tr.append(td1, td2, td3, td4);
    queueRowsEl.appendChild(tr);
  });
}

async function inspectQueueEncoding(quiet=false){
  try{
    const files = combinedInputFiles();
    if(files.length===0){
      encodingHintEl.textContent = "编码检测：队列为空";
      latestEncodingSummary = { uncertainCount: 0, gbCount: 0, utfCount: 0 };
      return latestEncodingSummary;
    }
    const r = await window.aiwfDesktop.inspectEncoding(files);
    const reps = Array.isArray(r?.reports)?r.reports:[];
    const textReps = reps.filter(x=>x.kind==="text");
    if(textReps.length===0){
      encodingHintEl.textContent = "编码检测：当前无 txt/csv 文件";
      latestEncodingSummary = { uncertainCount: 0, gbCount: 0, utfCount: 0 };
      return latestEncodingSummary;
    }
    const uncertain = textReps.filter(x=>x.encoding==="uncertain");
    const gb = textReps.filter(x=>x.encoding==="gb18030");
    const utf8 = textReps.filter(x=>x.encoding.startsWith("utf-8") || x.encoding.startsWith("utf-16"));
    encodingHintEl.textContent = `编码检测：UTF系 ${utf8.length} 个，GB18030 ${gb.length} 个，不确定 ${uncertain.length} 个`;
    latestEncodingSummary = { uncertainCount: uncertain.length, gbCount: gb.length, utfCount: utf8.length };
    if(!quiet && uncertain.length>0){
      setStatus("检测到编码不确定文件，建议先另存为 UTF-8 再生成。", false);
    }
    return latestEncodingSummary;
  }catch(e){
    encodingHintEl.textContent = "编码检测：失败";
    latestEncodingSummary = { uncertainCount: 0, gbCount: 0, utfCount: 0 };
    return latestEncodingSummary;
  }
}

async function inspectFonts(quiet=false){
  try{
    const r = await window.aiwfDesktop.checkFonts();
    if(!r){
      fontHintEl.textContent = "字体检查：失败";
      return;
    }
    const missReq = Array.isArray(r.missing_required)
      ? r.missing_required
      : (Array.isArray(r.missing_core) ? r.missing_core : []);
    const missOpt = Array.isArray(r.missing_optional)?r.missing_optional:[];
    if(r.ok){
      const coreInstalled = Array.isArray(r.core_installed) ? r.core_installed.join("/") : "";
      fontHintEl.textContent = coreInstalled
        ? `字体检查：通过（可用核心字体：${coreInstalled}）`
        : "字体检查：通过";
      return;
    }
    const details = `缺少核心字体: ${missReq.join("、")}` + (missOpt.length?`；可选缺失: ${missOpt.join("、")}`:"");
    fontHintEl.textContent = `字体检查：${details}`;
    if(!quiet){
      setStatus("检测到核心中文字体缺失，可能导致 DOCX/PPTX 乱码或排版异常。", false);
    }
  }catch(e){
    fontHintEl.textContent = "字体检查：失败";
  }
}

async function inspectRuntime(quiet=false){
  try{
    const r = await window.aiwfDesktop.checkRuntime();
    const t = r?.tesseract || {};
    const p = r?.pdftoppm || {};
    const langs = Array.isArray(r?.tesseract_langs?.langs) ? r.tesseract_langs.langs : [];
    const hasChi = langs.includes("chi_sim") || langs.includes("chi_tra");
    if(t.ok){
      const pdfState = p.ok ? "PDF扫描件OCR可用" : "PDF扫描件OCR未启用(缺pdftoppm)";
      const langState = hasChi ? "中文OCR语言包已安装" : "缺中文OCR语言包";
      ocrHintEl.textContent = `OCR 运行时：可用（${t.path || t.source || "tesseract"}）| ${pdfState} | ${langState}`;
      if(!quiet && !hasChi){
        setStatus("检测到 Tesseract 但缺少 chi_sim 语言包，中文图片/PDF OCR 质量会下降。", false);
      }
    }else{
      ocrHintEl.textContent = "OCR 运行时：未安装 Tesseract（图片将降级为文件信息）";
      if(!quiet){
        setStatus("未检测到 Tesseract，图片 OCR 会降级。可继续处理文本类文件。", false);
      }
    }
  }catch(e){
    ocrHintEl.textContent = "OCR 运行时：检测失败";
  }
}

function fmtEpoch(epoch){
  const n = Number(epoch || 0);
  if(!Number.isFinite(n) || n <= 0) return "-";
  try{
    const d = new Date(n * 1000);
    return d.toLocaleString("zh-CN", { hour12:false });
  }catch{
    return "-";
  }
}

async function refreshTaskStoreStatus(quiet=true){
  try{
    const cfg = cfgFromUi();
    const r = await window.aiwfDesktop.getTaskStoreStatus(cfg);
    if(!r?.ok){
      taskStoreHintEl.textContent = `远程任务存储：检测失败（${String(r?.error||"unknown")}）`;
      if(!quiet) setStatus("远程任务存储检测失败，请检查 accel-rust 是否运行。", false);
      return;
    }
    if(!r.enabled){
      taskStoreHintEl.textContent = "远程任务存储：未启用（本地模式）";
      return;
    }
    const ts = fmtEpoch(r.lastProbeEpoch);
    if(r.healthy){
      taskStoreHintEl.textContent = `远程任务存储：已启用且健康（失败计数 ${r.probeFailures||0}，最近探测 ${ts}）`;
    }else{
      taskStoreHintEl.textContent = `远程任务存储：已启用但异常（失败计数 ${r.probeFailures||0}，最近探测 ${ts}）`;
      if(!quiet) setStatus("远程任务存储异常：任务共享落库可能不可用。", false);
    }
  }catch(e){
    taskStoreHintEl.textContent = `远程任务存储：检测失败（${String(e)}）`;
    if(!quiet) setStatus("远程任务存储检测失败。", false);
  }
}

async function refreshRouteDiagnostics(){
  try{
    const r = await window.aiwfDesktop.getRouteMetricsSummary();
    if(!r?.ok){
      routeDiagHintEl.textContent = "加载失败";
      routeDiagEl.textContent = JSON.stringify(r||{}, null, 2);
      return;
    }
    latestRouteSummaryMeta = r;
    if(!r.exists){
      routeDiagHintEl.textContent = "暂无路由诊断数据（先打开 Workflow Studio 并操作画布）";
      routeDiagEl.textContent = JSON.stringify({workflows:{}}, null, 2);
      return;
    }
    const wf = r.summary?.workflows || {};
    const cnt = Object.keys(wf).length;
    routeDiagHintEl.textContent = `已加载 ${cnt} 个 workflow 路由统计，更新时间 ${r.summary?.updated_at || "-"}`;
    routeDiagEl.textContent = JSON.stringify(r.summary || {}, null, 2);
  }catch(e){
    routeDiagHintEl.textContent = "加载失败";
    routeDiagEl.textContent = JSON.stringify({error:String(e)}, null, 2);
  }
}

async function refreshStartupSelfCheck(quiet=true){
  try{
    const r = await window.aiwfDesktop.startupSelfCheck(cfgFromUi());
    selfCheckEl.textContent = JSON.stringify(r || {}, null, 2);
    if(r?.ok){
      selfCheckHintEl.textContent = "自检通过";
      if(!quiet) setStatus("自检通过", true);
    }else{
      const issues = Array.isArray(r?.issues) ? r.issues : [];
      selfCheckHintEl.textContent = `自检未通过：${issues.length} 个问题`;
      if(!quiet) setStatus("自检发现问题，请按建议修复后重试。", false);
    }
  }catch(e){
    selfCheckHintEl.textContent = "自检失败";
    selfCheckEl.textContent = JSON.stringify({ ok:false, error:String(e) }, null, 2);
    if(!quiet) setStatus("自检执行失败: "+e, false);
  }
}

function addToQueue(paths){
  const before=queuePaths.length;
  paths.forEach(p=>{
    const s=String(p||"").trim();
    if(s && !queuePaths.includes(s)) queuePaths.push(s);
  });
  renderQueue();
  inspectQueueEncoding(true);
  const added=queuePaths.length-before;
  setStatus(added>0?`已加入 ${added} 个文件到任务队列`:"没有新增文件（可能已在队列中）", true);
}

function getPathFromDroppedFile(file){
  try{
    const p = window.aiwfDesktop.getDroppedFilePath(file);
    if(p) return String(p);
  }catch(e){}
  try{
    if(file && file.path) return String(file.path);
  }catch(e){}
  return "";
}

function extractDropPaths(dt){
  const out=[];
  const files = Array.from(dt?.files || []);
  files.forEach((f)=>{
    const p = getPathFromDroppedFile(f);
    if(p) out.push(p);
  });
  if(out.length===0){
    const items = Array.from(dt?.items || []);
    items.forEach((it)=>{
      try{
        const f = it.getAsFile ? it.getAsFile() : null;
        const p = getPathFromDroppedFile(f);
        if(p) out.push(p);
      }catch(e){}
    });
  }
  return Array.from(new Set(out));
}

function extractFilePickerPaths(fileList){
  const out = [];
  Array.from(fileList || []).forEach((file)=>{
    const p = getPathFromDroppedFile(file);
    if(p) out.push(p);
  });
  return Array.from(new Set(out));
}

function bindFilePicker(buttonEl, inputEl, onPickPaths){
  if(!buttonEl || !inputEl) return;
  buttonEl.addEventListener("click", ()=>{
    try { inputEl.click(); } catch(e){}
  });
  inputEl.addEventListener("change", ()=>{
    const paths = extractFilePickerPaths(inputEl.files);
    inputEl.value = "";
    if(paths.length===0){
      setStatus("未获取到所选文件路径。请确认文件位于本地磁盘后重试。", false);
      return;
    }
    onPickPaths(paths);
  });
}

function bindDropzone(targetDz, onDropPaths){
  if(!targetDz) return;
  const prevent=(e)=>{ e.preventDefault(); e.stopPropagation(); };
  ["dragenter","dragover","dragleave","drop"].forEach(evt=>targetDz.addEventListener(evt, prevent));
  ["dragenter","dragover"].forEach(evt=>targetDz.addEventListener(evt, ()=>targetDz.classList.add("active")));
  ["dragleave","drop"].forEach(evt=>targetDz.addEventListener(evt, ()=>targetDz.classList.remove("active")));
  targetDz.addEventListener("click", ()=>{
    const pickerId = String(targetDz.dataset.picker || "").trim();
    if(!pickerId) return;
    const input = $(pickerId);
    if(input) input.click();
  });
  targetDz.addEventListener("drop", (e)=>{
    const paths=extractDropPaths(e.dataTransfer);
    if(paths.length===0){
      setStatus("未获取到拖拽文件路径。请先尝试点击“主数据文件”输入路径，或把文件放到本地磁盘后再拖拽。", false);
      return;
    }
    onDropPaths(paths);
  });
}

function renderSamplePool(items = [], root = ""){
  const list = Array.isArray(items) ? items : [];
  const hintRoot = root || $("samplePoolDir").value.trim() || (($("outputRoot").value.trim() || "E:\\Desktop_Real\\AIWF") + "\\sample_pool");
  if(sampleHintEl){
    sampleHintEl.textContent = `样本池：${hintRoot}（${list.length} 个样本）`;
  }
  if(!sampleRowsEl) return;
  if(list.length===0){
    sampleRowsEl.innerHTML = '<tr><td colspan="4" style="color:#75869a">暂无样本</td></tr>';
    return;
  }
  sampleRowsEl.innerHTML = "";
  list.forEach((it, i)=>{
    const tr = document.createElement("tr");
    const td1 = document.createElement("td");
    const td2 = document.createElement("td");
    const td3 = document.createElement("td");
    const td4 = document.createElement("td");
    td1.textContent = String(i + 1);
    td2.textContent = String(it?.name || "");
    td3.textContent = (Number(it?.size || 0) / 1024).toFixed(1);
    td4.className = "path";
    td4.textContent = String(it?.path || "");
    td4.title = String(it?.path || "");
    tr.append(td1, td2, td3, td4);
    sampleRowsEl.appendChild(tr);
  });
}

async function refreshSamplePool(quiet=false){
  try{
    const out = await window.aiwfDesktop.getSamplePoolInfo(cfgFromUi());
    if(!out?.ok){
      if(!quiet) setStatus(`样本池刷新失败: ${out?.error || "unknown"}`, false);
      renderSamplePool([], out?.root || "");
      return out;
    }
    renderSamplePool(out.files || [], out.root || "");
    if(!quiet) setStatus("样本池已刷新", true);
    return out;
  }catch(e){
    if(!quiet) setStatus(`样本池刷新失败: ${e}`, false);
    return { ok:false, error:String(e) };
  }
}

async function addToSamplePool(paths){
  try{
    const out = await window.aiwfDesktop.samplePoolAddFiles(paths, cfgFromUi());
    await refreshSamplePool(true);
    if(!out?.ok){
      setStatus(`加入样本池失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const copied = Array.isArray(out.copied) ? out.copied.length : 0;
    const skipped = Array.isArray(out.skipped) ? out.skipped.length : 0;
    setStatus(`样本池更新完成：新增 ${copied}，跳过 ${skipped}`, true);
  }catch(e){
    setStatus(`加入样本池失败: ${e}`, false);
  }
}

function renderArtifacts(list){
  latestArtifacts = Array.isArray(list) ? list : [];
  if(!Array.isArray(list)||list.length===0){
    rowsEl.innerHTML='<tr><td colspan="4" style="color:#75869a">暂无结果</td></tr>';
    if (btnOpenNoiseEl) btnOpenNoiseEl.style.display = "none";
    return;
  }
  rowsEl.innerHTML = "";
  const nameMap = {
    xlsx_fin_001: "财务表格成品",
    docx_audit_001: "文档成品",
    pptx_deck_001: "演示成品",
    md_evidence_001: "证据 Markdown",
    md_paper_index_001: "论文分片索引",
    md_ai_corpus_001: "AI 语料合并",
    md_quality_001: "质量报告",
    md_filtered_noise_001: "问号噪声清单",
  };
  const prio = {
    xlsx_fin_001: 10,
    docx_audit_001: 11,
    pptx_deck_001: 12,
    md_evidence_001: 20,
    md_ai_corpus_001: 21,
    md_paper_index_001: 22,
    md_quality_001: 23,
    md_filtered_noise_001: 24,
  };
  const ordered = [...list].sort((a, b) => {
    const ida = String(a?.artifact_id || "");
    const idb = String(b?.artifact_id || "");
    const pa = Number(prio[ida] ?? 999);
    const pb = Number(prio[idb] ?? 999);
    if (pa !== pb) return pa - pb;
    return ida.localeCompare(idb, "zh-CN");
  });
  const noiseArtifact = ordered.find((x) => String(x?.artifact_id || "") === "md_filtered_noise_001");
  if (btnOpenNoiseEl) btnOpenNoiseEl.style.display = noiseArtifact ? "inline-block" : "none";
  ordered.forEach((a) => {
    const tr = document.createElement("tr");
    const id = String(a?.artifact_id || "-");
    const kind = String(a?.kind || "-");
    const p = String(a?.path || "-");
    const td1 = document.createElement("td");
    const td2 = document.createElement("td");
    const td3 = document.createElement("td");
    const td4 = document.createElement("td");
    const btn = document.createElement("button");
    const label = String(nameMap[id] || "");
    td1.textContent = label ? `${id}（${label}）` : id;
    td2.textContent = kind;
    td3.className = "path";
    td3.title = p;
    td3.textContent = p;
    btn.className = "secondary";
    btn.textContent = "打开";
    btn.onclick = () => window.aiwfDesktop.openPath(p);
    td4.appendChild(btn);
    if (id === "md_filtered_noise_001") {
      tr.style.background = "#fff8e8";
    }
    tr.append(td1, td2, td3, td4);
    rowsEl.appendChild(tr);
  });
}

function renderMetrics(p){
  $("mJob").textContent=p?.job_id||"-";
  const ok=!!(p?.ok||p?.run?.ok);
  $("mOk").textContent=ok?"成功":"失败";
  const mode = p?.fallback_applied ? "offline_fallback" : (p?.mode || (cfgFromUi().mode || "-"));
  $("mMode").textContent=String(mode);
  const q = Number(p?.quality_score?.score ?? p?.quality_score ?? p?.quality?.quality_score ?? 0);
  $("mQScore").textContent=Number.isFinite(q) && q > 0 ? q.toFixed(1) : "-";
  $("mSec").textContent=String(p?.run?.seconds??p?.seconds??"-");
  const c=Array.isArray(p?.artifacts)?p.artifacts.length:0;
  $("mCnt").textContent=String(c);
  renderArtifacts(p?.artifacts||[]);
}


