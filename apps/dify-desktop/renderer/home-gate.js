function appendGateLog(line){
  if(!gateLogEl) return;
  const txt = String(line || "");
  const prev = String(gateLogEl.textContent || "");
  const next = `${prev}${prev ? "\n" : ""}${txt}`;
  gateLogEl.textContent = next.length > 20000 ? next.slice(-20000) : next;
  gateLogEl.scrollTop = gateLogEl.scrollHeight;
}

function renderGateSummary(out){
  const x = out && typeof out === "object" ? out : {};
  $("gateTotal").textContent = String(Number(x.total || 0));
  $("gatePassed").textContent = String(Number(x.passed || 0));
  $("gateFailed").textContent = String(Number(x.failed || 0));
  $("gatePassRate").textContent = `${(Number(x.pass_rate || 0) * 100).toFixed(1)}%`;
  const items = Array.isArray(x.items) ? x.items : [];
  const latest = items[0] || null;
  if (gateReleaseStatusEl) {
    gateReleaseStatusEl.textContent = latest
      ? `最近 release gate 状态：${latest.ok ? "通过" : (latest.canceled ? "已取消" : "失败")} | ${String(latest.script || "")} | ${String(latest.ts || "").replace("T"," ").replace("Z","")}`
      : "最近 release gate 状态：未知";
  }
  if(!gateRowsEl) return;
  if(!items.length){
    gateRowsEl.innerHTML = '<tr><td colspan="5" style="color:#75869a">暂无</td></tr>';
    return;
  }
  gateRowsEl.innerHTML = "";
  items.slice(0, 20).forEach((it)=>{
    const tr=document.createElement("tr");
    const tdTs=document.createElement("td");
    const tdSc=document.createElement("td");
    const tdSt=document.createElement("td");
    const tdSec=document.createElement("td");
    const tdOp=document.createElement("td");
    tdTs.textContent = String(it.ts || "").replace("T"," ").replace("Z","");
    tdSc.textContent = String(it.script || "");
    tdSt.textContent = it.ok ? "通过" : (it.canceled ? "已取消" : "失败");
    tdSt.style.color = it.ok ? "#087443" : (it.canceled ? "#8a6d1f" : "#b42318");
    tdSec.textContent = `${Number(it.seconds || 0).toFixed(2)}s`;
    const reportDir = String(it.report_dir || "");
    const failSummary = `${String(it.script || "")} | exit=${Number(it.exit_code || 0)} | ${String(it.stderr_tail || it.stdout_tail || "").slice(0, 240)}`;
    const btnOpen = document.createElement("button");
    btnOpen.className = "secondary";
    btnOpen.style.padding = "4px 8px";
    btnOpen.style.fontSize = "12px";
    btnOpen.textContent = "打开报告";
    btnOpen.disabled = !reportDir;
    btnOpen.onclick = async () => { if (reportDir) await window.aiwfDesktop.openPath(reportDir); };
    const btnCopy = document.createElement("button");
    btnCopy.className = "secondary";
    btnCopy.style.padding = "4px 8px";
    btnCopy.style.fontSize = "12px";
    btnCopy.style.marginLeft = "4px";
    btnCopy.textContent = "复制摘要";
    btnCopy.onclick = async () => {
      try {
        await navigator.clipboard.writeText(failSummary);
        setStatus("已复制失败摘要", true);
      } catch (e) {
        setStatus(`复制失败摘要失败: ${e}`, false);
      }
    };
    tdOp.append(btnOpen, btnCopy);
    tr.append(tdTs,tdSc,tdSt,tdSec,tdOp);
    gateRowsEl.appendChild(tr);
  });
}

function setBuildButtonsDisabled(disabled){
  ["btnBuildPortable","btnBuildInstaller","btnBuildCancel","btnOpenBuildDir","btnOpenLatestArtifacts","btnExportReleaseMd","btnExportReleaseJson"].forEach((id)=>{
    const el = $(id);
    if (!el) return;
    if (id === "btnBuildCancel") el.disabled = !disabled;
    else el.disabled = !!disabled;
  });
}

async function refreshBuildGuardStatus(){
  try{
    const out = await window.aiwfDesktop.getBuildGuardStatus();
    const missing = Array.isArray(out?.missing) ? out.missing : [];
    if (buildReleaseStatusEl) {
      if (missing.length === 0) buildReleaseStatusEl.textContent = "最近打包门禁状态：通过（可执行 gated 打包）";
      else buildReleaseStatusEl.textContent = `最近打包门禁状态：未通过，缺少 ${missing.join(", ")}`;
    }
    const artifact = out?.artifact_check || {};
    const artifactMissing = Array.isArray(artifact?.missing) ? artifact.missing : [];
    if (buildArtifactStatusEl) {
      if (artifactMissing.length === 0) buildArtifactStatusEl.textContent = "最近产物校验：通过（exe + md/json 报告均为近期生成）";
      else buildArtifactStatusEl.textContent = `最近产物校验：未通过，缺少 ${artifactMissing.join(", ")}`;
    }
    if (!buildRunning) setBuildButtonsDisabled(missing.length > 0);
    const runs = Array.isArray(out?.build_runs) ? out.build_runs : [];
    if (runs.length > 0) {
      const last = runs[0];
      appendGateLog(`[build][history] ${String(last.script || "")} -> ${last.ok ? "通过" : "失败"} (${Number(last.seconds || 0).toFixed(2)}s)`);
    }
    return out;
  }catch(e){
    if (buildReleaseStatusEl) buildReleaseStatusEl.textContent = `最近打包门禁状态：读取失败 ${e}`;
    if (buildArtifactStatusEl) buildArtifactStatusEl.textContent = `最近产物校验：读取失败 ${e}`;
    return { ok:false, error:String(e) };
  }
}

function setGateButtonsDisabled(disabled){
  const ids = ["btnGateAll","btnGateCancel","btnGateUnit","btnGateSmoke","btnGateReg","btnGateRegDirty","btnGateOffice","btnGateAcceptance","btnGateRefresh","btnGatePackGuard"];
  ids.forEach((id)=>{
    const el = $(id);
    if (!el) return;
    if (id === "btnGateCancel") el.disabled = !disabled;
    else el.disabled = !!disabled;
  });
  if (disabled) setBuildButtonsDisabled(true);
  else refreshBuildGuardStatus();
}

async function refreshGateSummary(){
  try{
    const out = await window.aiwfDesktop.getLocalGateSummary({ limit: 80 });
    renderGateSummary(out);
  }catch(e){
    appendGateLog(`刷新看板失败: ${e}`);
  }
}

async function runGateScript(script){
  if (gateRunning) {
    setStatus("已有验收任务在运行，请先取消或等待完成。", false);
    return { ok: false, error: "gate_running" };
  }
  try{
    gateRunning = true;
    setGateButtonsDisabled(true);
    appendGateLog(`>>> 执行 ${script}`);
    const out = await window.aiwfDesktop.runLocalGateCheck({ script });
    appendGateLog(`>>> 结束 ${script} (exit=${out?.exit_code ?? "?"})`);
    await refreshGateSummary();
    setStatus(out?.ok ? `验收通过: ${script}` : `验收失败: ${script}`, !!out?.ok);
    return out;
  }catch(e){
    appendGateLog(`>>> 执行异常 ${script}: ${e}`);
    setStatus(`验收执行失败: ${e}`, false);
    return { ok: false, error: String(e) };
  } finally {
    gateRunning = false;
    setGateButtonsDisabled(false);
  }
}

async function runGateAll() {
  const scripts = ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate", "acceptance:real"];
  let passed = 0;
  appendGateLog(">>> 开始一键全跑");
  for (const s of scripts) {
    // eslint-disable-next-line no-await-in-loop
    const one = await runGateScript(s);
    if (one && one.ok) {
      passed += 1;
      continue;
    }
    appendGateLog(`>>> 一键全跑中止于 ${s}`);
    setStatus(`一键全跑失败（${passed}/${scripts.length}）`, false);
    return;
  }
  appendGateLog(">>> 一键全跑完成");
  setStatus(`一键全跑通过（${passed}/${scripts.length}）`, true);
}

async function cancelGateRun() {
  try {
    const out = await window.aiwfDesktop.cancelLocalGateCheck();
    if (out?.ok) {
      appendGateLog(">>> 已请求取消当前任务");
      setStatus("已请求取消当前验收任务", true);
    } else {
      setStatus(`取消失败: ${out?.error || "unknown"}`, false);
    }
  } catch (e) {
    setStatus(`取消失败: ${e}`, false);
  }
}

async function ensureReleaseGatePassedBeforePack() {
  const out = await window.aiwfDesktop.getLocalGateSummary({ limit: 20 });
  const items = Array.isArray(out?.items) ? out.items : [];
  const required = ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate"];
  const latestByScript = {};
  items.forEach((it) => {
    const s = String(it?.script || "");
    if (!required.includes(s)) return;
    if (!latestByScript[s]) latestByScript[s] = it;
  });
  const missing = required.filter((s) => !latestByScript[s] || !latestByScript[s].ok);
  if (missing.length > 0) {
    setStatus(`打包前检查未通过，缺少通过项 ${missing.join(", ")}`, false);
    appendGateLog(`>>> 打包前硬提示阻断: ${missing.join(", ")}`);
    setBuildButtonsDisabled(true);
    return false;
  }
  setStatus("打包前检查通过，可执行 gated build。", true);
  appendGateLog(">>> 打包前检查通过");
  if (!buildRunning) setBuildButtonsDisabled(false);
  return true;
}

async function runLocalBuildScript(script){
  if (buildRunning || gateRunning) {
    setStatus("当前有任务执行中，请稍后再试。", false);
    return;
  }
  const ok = await ensureReleaseGatePassedBeforePack();
  if (!ok) return;
  try{
    buildRunning = true;
    setBuildButtonsDisabled(true);
    appendGateLog(`>>> 开始打包 ${script}`);
    const out = await window.aiwfDesktop.runLocalBuildScript({ script });
    appendGateLog(`>>> 打包结束 ${script} (exit=${out?.exit_code ?? "?"})`);
    setStatus(out?.ok ? `打包成功: ${script}` : `打包失败: ${script}`, !!out?.ok);
  }catch(e){
    appendGateLog(`>>> 打包异常 ${script}: ${e}`);
    setStatus(`打包异常: ${e}`, false);
  }finally{
    buildRunning = false;
    await refreshBuildGuardStatus();
  }
}

async function cancelBuildRun() {
  try {
    const out = await window.aiwfDesktop.cancelLocalBuildScript();
    if (out?.ok) {
      appendGateLog(">>> 已请求取消当前打包任务");
      setStatus("已请求取消当前打包任务", true);
    } else {
      setStatus(`取消打包失败: ${out?.error || "unknown"}`, false);
    }
  } catch (e) {
    setStatus(`取消打包失败: ${e}`, false);
  }
}

async function openDesktopBuildDir() {
  const st = await window.aiwfDesktop.getBuildGuardStatus();
  const runs = Array.isArray(st?.build_runs) ? st.build_runs : [];
  const dir = String(runs[0]?.desktop_dir || $("outputRoot").value.trim() || "");
  if (!dir) {
    setStatus("暂无桌面打包目录记录", false);
    return;
  }
  await window.aiwfDesktop.openPath(dir);
  setStatus("已打开桌面产物目录", true);
}

async function exportReleaseReport(format) {
  try {
    const out = await window.aiwfDesktop.exportReleaseReport({ format });
    if (!out?.ok) {
      setStatus(`导出发布报告失败: ${out?.error || "unknown"}`, false);
      return;
    }
    appendGateLog(`>>> 发布报告已导出 ${out.path}`);
    setStatus(`发布报告已导出（未自动打开）：${out.path}`, true);
  } catch (e) {
    setStatus(`导出发布报告失败: ${e}`, false);
  }
}
