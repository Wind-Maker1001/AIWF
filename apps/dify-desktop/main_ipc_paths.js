const os = require("os");

function createMainIpcPathSupport(ctx) {
  const {
    app,
    fs,
    path,
    loadConfig,
  } = ctx;

  function resolveOutputRoot(cfg = null) {
    const c = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromCfg = String(c?.outputRoot || "").trim();
    if (fromCfg) return fromCfg;
    const eDesktop = "E:\\Desktop_Real";
    if (fs.existsSync(eDesktop)) return path.join(eDesktop, "AIWF");
    return path.join(app.getPath("desktop"), "AIWF_Builds");
  }

  function normalizeAbsPath(value) {
    return path.resolve(String(value || "").trim());
  }

  function isPathWithin(childPath, rootPath) {
    try {
      const child = normalizeAbsPath(childPath).toLowerCase();
      const root = normalizeAbsPath(rootPath).toLowerCase();
      return child === root || child.startsWith(`${root}${path.sep.toLowerCase()}`);
    } catch {
      return false;
    }
  }

  function trustedRoots(cfg = null) {
    const merged = cfg && typeof cfg === "object" ? cfg : loadConfig();
    return [
      resolveOutputRoot(merged),
      app.getPath("documents"),
      app.getPath("desktop"),
      app.getPath("userData"),
      os.tmpdir(),
    ];
  }

  function isTrustedPath(candidatePath, cfg = null) {
    const raw = String(candidatePath || "").trim();
    if (!raw) return false;
    if (!path.isAbsolute(raw)) return false;
    const roots = trustedRoots(cfg);
    return roots.some((root) => isPathWithin(raw, root));
  }

  function buildDesktopOutputDir(cfg = null) {
    return resolveOutputRoot(cfg);
  }

  function buildSamplePoolDir(cfg = null) {
    const c = cfg && typeof cfg === "object" ? cfg : loadConfig();
    const fromCfg = String(c?.samplePoolDir || "").trim();
    if (fromCfg) return fromCfg;
    return path.join(resolveOutputRoot(c), "sample_pool");
  }

  function listSamplePoolFiles(cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx", ".csv"]);
    try {
      if (!fs.existsSync(dir)) return { ok: true, root: dir, files: [] };
      const files = fs.readdirSync(dir, { withFileTypes: true })
        .filter((item) => item.isFile() && exts.has(path.extname(String(item.name || "")).toLowerCase()))
        .map((item) => {
          const filePath = path.join(dir, item.name);
          let size = 0;
          let mtime = 0;
          try {
            const stats = fs.statSync(filePath);
            size = Number(stats.size || 0);
            mtime = Number(stats.mtimeMs || 0);
          } catch {}
          return { name: String(item.name || ""), path: filePath, size, mtime };
        })
        .sort((a, b) => b.mtime - a.mtime);
      return { ok: true, root: dir, files };
    } catch (error) {
      return { ok: false, error: String(error), root: dir, files: [] };
    }
  }

  function addSamplePoolFiles(paths = [], cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    fs.mkdirSync(dir, { recursive: true });
    const copied = [];
    const skipped = [];
    const bad = [];
    const arr = Array.isArray(paths) ? paths : [];
    const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx", ".csv"]);
    arr.forEach((inputPath) => {
      try {
        const sourcePath = String(inputPath || "").trim();
        if (!sourcePath || !fs.existsSync(sourcePath) || !fs.statSync(sourcePath).isFile()) {
          bad.push(sourcePath);
          return;
        }
        const ext = path.extname(sourcePath).toLowerCase();
        if (!exts.has(ext)) {
          skipped.push(sourcePath);
          return;
        }
        const base = path.basename(sourcePath);
        let targetPath = path.join(dir, base);
        if (fs.existsSync(targetPath)) {
          const srcSize = Number(fs.statSync(sourcePath).size || 0);
          const dstSize = Number(fs.statSync(targetPath).size || -1);
          if (srcSize === dstSize) {
            skipped.push(sourcePath);
            return;
          }
          const stem = path.basename(base, ext);
          targetPath = path.join(dir, `${stem}_${Date.now()}${ext}`);
        }
        fs.copyFileSync(sourcePath, targetPath);
        copied.push(targetPath);
      } catch {
        bad.push(String(inputPath || ""));
      }
    });
    return { ok: true, root: dir, copied, skipped, bad };
  }

  function clearSamplePool(cfg = null) {
    const dir = buildSamplePoolDir(cfg);
    let removed = 0;
    try {
      if (!fs.existsSync(dir)) return { ok: true, root: dir, removed };
      fs.readdirSync(dir, { withFileTypes: true }).forEach((item) => {
        if (!item.isFile()) return;
        const filePath = path.join(dir, item.name);
        try {
          fs.unlinkSync(filePath);
          removed += 1;
        } catch {}
      });
      return { ok: true, root: dir, removed };
    } catch (error) {
      return { ok: false, error: String(error), root: dir, removed };
    }
  }

  function getLatestArtifactsDir(cfg = null) {
    const root = buildDesktopOutputDir(cfg);
    try {
      if (!fs.existsSync(root)) return { ok: false, error: "desktop_build_dir_not_found", root };
      const dirs = fs.readdirSync(root, { withFileTypes: true })
        .filter((item) => item.isDirectory() && /^[a-f0-9]{32}$/i.test(String(item.name || "")))
        .map((item) => {
          const artifactsDir = path.join(root, item.name, "artifacts");
          let mtime = 0;
          try {
            const stats = fs.statSync(artifactsDir);
            mtime = Number(stats.mtimeMs || 0);
          } catch {}
          return { id: String(item.name || ""), artifacts_dir: artifactsDir, mtime };
        })
        .filter((item) => item.mtime > 0)
        .sort((a, b) => b.mtime - a.mtime);
      if (!dirs.length) return { ok: false, error: "no_artifacts_job_found", root };
      return { ok: true, root, ...dirs[0] };
    } catch (error) {
      return { ok: false, error: String(error), root };
    }
  }

  function collectExeFiles(rootDir, out = []) {
    try {
      if (!fs.existsSync(rootDir)) return out;
      const items = fs.readdirSync(rootDir, { withFileTypes: true });
      items.forEach((item) => {
        const filePath = path.join(rootDir, item.name);
        if (item.isDirectory()) collectExeFiles(filePath, out);
        else if (/\.exe$/i.test(item.name)) out.push(filePath);
      });
    } catch {}
    return out;
  }

  function checkDesktopBuildArtifacts(options = {}, cfg = null) {
    const recentMs = Number(options.recentMs || process.env.AIWF_BUILD_ARTIFACT_RECENT_MS || 7_200_000);
    const now = Date.now();
    const buildDir = buildDesktopOutputDir(cfg);
    const reportsDir = path.join(buildDir, "reports");
    const files = collectExeFiles(buildDir, []);
    const exes = files
      .map((filePath) => ({ path: filePath, mtime: Number(fs.statSync(filePath).mtimeMs || 0) }))
      .sort((a, b) => b.mtime - a.mtime);
    const reportFiles = [];
    try {
      if (fs.existsSync(reportsDir)) {
        fs.readdirSync(reportsDir, { withFileTypes: true }).forEach((item) => {
          if (!item.isFile()) return;
          const filePath = path.join(reportsDir, item.name);
          reportFiles.push({ path: filePath, name: String(item.name || ""), mtime: Number(fs.statSync(filePath).mtimeMs || 0) });
        });
      }
    } catch {}
    const mdReports = reportFiles.filter((item) => /^release_report_.*\.md$/i.test(item.name)).sort((a, b) => b.mtime - a.mtime);
    const jsonReports = reportFiles.filter((item) => /^release_report_.*\.json$/i.test(item.name)).sort((a, b) => b.mtime - a.mtime);
    const recent = (item) => !!item && (now - Number(item.mtime || 0) <= recentMs);
    const missing = [];
    if (!recent(exes[0])) missing.push("desktop_exe_recent");
    if (!recent(mdReports[0])) missing.push("release_report_md_recent");
    if (!recent(jsonReports[0])) missing.push("release_report_json_recent");
    return {
      ok: missing.length === 0,
      missing,
      recent_ms: recentMs,
      desktop_dir: buildDir,
      reports_dir: reportsDir,
      latest_exe: exes[0] ? String(exes[0].path || "") : "",
      latest_md_report: mdReports[0] ? String(mdReports[0].path || "") : "",
      latest_json_report: jsonReports[0] ? String(jsonReports[0].path || "") : "",
    };
  }

  function copyRecentBuildArtifactsToDesktop(sinceMs = 0, cfg = null) {
    const desktopDir = buildDesktopOutputDir(cfg);
    fs.mkdirSync(desktopDir, { recursive: true });
    const distDir = path.join(__dirname, "dist");
    const files = collectExeFiles(distDir, []);
    const picked = files
      .map((filePath) => ({ path: filePath, mtime: Number(fs.statSync(filePath).mtimeMs || 0) }))
      .filter((item) => item.mtime >= Math.max(0, Number(sinceMs || 0) - 10_000))
      .sort((a, b) => b.mtime - a.mtime)
      .slice(0, 8);
    const copied = [];
    picked.forEach((item) => {
      try {
        const basename = path.basename(item.path);
        const targetPath = path.join(desktopDir, basename);
        fs.copyFileSync(item.path, targetPath);
        copied.push(targetPath);
      } catch {}
    });
    return { desktop_dir: desktopDir, copied_files: copied };
  }

  return {
    addSamplePoolFiles,
    buildDesktopOutputDir,
    buildSamplePoolDir,
    checkDesktopBuildArtifacts,
    clearSamplePool,
    copyRecentBuildArtifactsToDesktop,
    getLatestArtifactsDir,
    isPathWithin,
    isTrustedPath,
    listSamplePoolFiles,
    normalizeAbsPath,
    resolveOutputRoot,
    trustedRoots,
  };
}

function registerPathIpc(ctx, helpers) {
  const {
    ipcMain,
    shell,
    loadConfig,
  } = ctx;
  const {
    isTrustedPath,
    getLatestArtifactsDir,
    listSamplePoolFiles,
    addSamplePoolFiles,
    clearSamplePool,
  } = helpers;

  ipcMain.handle("aiwf:openPath", async (_evt, p) => {
    const candidate = String(p || "").trim();
    if (!candidate) return { ok: false, error: "path_required" };
    if (!isTrustedPath(candidate)) return { ok: false, error: "path_not_allowed" };
    await shell.openPath(candidate);
    return { ok: true };
  });
  ipcMain.handle("aiwf:getLatestArtifactsDir", async () => getLatestArtifactsDir());
  ipcMain.handle("aiwf:getSamplePoolInfo", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return listSamplePoolFiles(merged);
  });
  ipcMain.handle("aiwf:samplePoolAddFiles", async (_evt, paths, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return addSamplePoolFiles(paths, merged);
  });
  ipcMain.handle("aiwf:samplePoolClear", async (_evt, cfg) => {
    const merged = { ...loadConfig(), ...(cfg || {}) };
    return clearSamplePool(merged);
  });
}

module.exports = {
  createMainIpcPathSupport,
  registerPathIpc,
};
