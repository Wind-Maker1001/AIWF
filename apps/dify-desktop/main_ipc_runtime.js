function registerRuntimeIpc(ctx) {
  const {
    ipcMain,
    inspectFileEncoding,
    toUtf8FileIfNeeded,
    checkChineseOfficeFonts,
    installBundledFontsForCurrentUser,
    checkTesseractRuntime,
    checkTesseractLangs,
    checkPdftoppmRuntime,
    getTaskStoreStatus,
  } = ctx;

  ipcMain.handle("aiwf:inspectEncoding", async (_evt, paths) => {
    const arr = Array.isArray(paths) ? paths : [];
    const reports = arr.map((p) => inspectFileEncoding(String(p || "")));
    const warnings = reports.filter((x) => x.warning).map((x) => ({ path: x.path, warning: x.warning }));
    return { ok: true, reports, warnings };
  });

  ipcMain.handle("aiwf:normalizeEncoding", async (_evt, paths) => {
    const arr = Array.isArray(paths) ? paths : [];
    const items = [];
    for (const p of arr) {
      try {
        items.push(toUtf8FileIfNeeded(String(p || "")));
      } catch (e) {
        items.push({
          source: String(p || ""),
          output: String(p || ""),
          converted: false,
          error: String(e),
        });
      }
    }
    const convertedCount = items.filter((x) => x.converted).length;
    return { ok: true, convertedCount, items };
  });

  ipcMain.handle("aiwf:checkFonts", async () => checkChineseOfficeFonts());
  ipcMain.handle("aiwf:installBundledFonts", async () => {
    if (typeof installBundledFontsForCurrentUser !== "function") {
      return { ok: false, reason: "not_supported" };
    }
    return installBundledFontsForCurrentUser();
  });
  ipcMain.handle("aiwf:checkRuntime", async () => ({
    ok: true,
    fonts: checkChineseOfficeFonts(),
    tesseract: checkTesseractRuntime(),
    tesseract_langs: checkTesseractLangs(),
    pdftoppm: checkPdftoppmRuntime(),
  }));

  ipcMain.handle("aiwf:startupSelfCheck", async (_evt, cfg) => {
    const runtime = {
      fonts: checkChineseOfficeFonts(),
      tesseract: checkTesseractRuntime(),
      tesseract_langs: checkTesseractLangs(),
      pdftoppm: checkPdftoppmRuntime(),
    };
    const taskStore = (typeof getTaskStoreStatus === "function")
      ? await getTaskStoreStatus(cfg || {}).catch((e) => ({ ok: false, error: String(e) }))
      : { ok: false, error: "not_supported" };
    const issues = [];
    const suggestions = [];

    const missCore = Array.isArray(runtime.fonts?.missing_required) ? runtime.fonts.missing_required : [];
    if (runtime.fonts && runtime.fonts.ok === false && missCore.length > 0) {
      issues.push(`缺少核心中文字体: ${missCore.join("、")}`);
      suggestions.push("点击“安装内置字体”，再重试产物生成。");
    }
    if (!runtime.tesseract?.ok) {
      issues.push("未检测到 Tesseract，图片/PDF 扫描件 OCR 会降级。");
      suggestions.push("安装 Tesseract 或使用打包版内置工具。");
    } else {
      const langs = Array.isArray(runtime.tesseract_langs?.langs) ? runtime.tesseract_langs.langs : [];
      if (!langs.includes("chi_sim") && !langs.includes("chi_tra")) {
        issues.push("Tesseract 缺少中文语言包。");
        suggestions.push("安装 chi_sim 语言包提升中文 OCR 质量。");
      }
      if (!runtime.pdftoppm?.ok) {
        issues.push("未检测到 pdftoppm，PDF 扫描件 OCR 不可用。");
        suggestions.push("安装 poppler 或使用离线包内置 poppler。");
      }
    }
    if (taskStore?.ok && taskStore.enabled && !taskStore.healthy) {
      issues.push(`远程任务存储异常（失败计数 ${taskStore.probeFailures || 0}）。`);
      suggestions.push("检查 SQL/base_api 连通性，或切换到本地模式。");
    }
    return {
      ok: issues.length === 0,
      runtime,
      task_store: taskStore,
      issues,
      suggestions,
    };
  });

  ipcMain.handle("aiwf:getTaskStoreStatus", async (_evt, cfg) => {
    if (typeof getTaskStoreStatus !== "function") {
      return { ok: false, enabled: false, healthy: false, error: "not_supported" };
    }
    try {
      return await getTaskStoreStatus(cfg || {});
    } catch (e) {
      return { ok: false, enabled: false, healthy: false, error: String(e) };
    }
  });
}

module.exports = {
  registerRuntimeIpc,
};
