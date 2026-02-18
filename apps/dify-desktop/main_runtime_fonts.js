function createFontSupport({ app, fs, path }) {
  function listInstalledFonts() {
    const dirs = [];
    if (process.platform === "win32") {
      dirs.push(path.join(process.env.WINDIR || "C:\\Windows", "Fonts"));
    } else if (process.platform === "darwin") {
      dirs.push("/System/Library/Fonts", "/Library/Fonts", path.join(app.getPath("home"), "Library", "Fonts"));
    } else {
      dirs.push("/usr/share/fonts", "/usr/local/share/fonts", path.join(app.getPath("home"), ".fonts"));
    }
    const out = [];
    for (const d of dirs) {
      if (!fs.existsSync(d)) continue;
      try {
        const names = fs.readdirSync(d).map((x) => String(x || "").toLowerCase());
        out.push(...names);
      } catch {}
    }
    return Array.from(new Set(out));
  }

  function listBundledFonts() {
    const roots = [];
    if (process.resourcesPath) roots.push(process.resourcesPath);
    roots.push(__dirname);
    const out = [];
    for (const root of roots) {
      const d = path.join(root, "tools", "fonts");
      if (!fs.existsSync(d)) continue;
      try {
        const names = fs.readdirSync(d).map((x) => String(x || "").toLowerCase());
        out.push(...names);
      } catch {}
    }
    return Array.from(new Set(out));
  }

  function listBundledFontPaths() {
    const roots = [];
    if (process.resourcesPath) roots.push(process.resourcesPath);
    roots.push(__dirname);
    const out = [];
    for (const root of roots) {
      const d = path.join(root, "tools", "fonts");
      if (!fs.existsSync(d)) continue;
      try {
        fs.readdirSync(d).forEach((name) => {
          const s = String(name || "");
          if (!/\.(ttf|ttc|otf)$/i.test(s)) return;
          out.push(path.join(d, s));
        });
      } catch {}
    }
    return Array.from(new Set(out));
  }

  function hasAnyFontFile(fontFiles, candidates) {
    const lower = candidates.map((x) => String(x || "").toLowerCase());
    return fontFiles.some((f) => lower.some((c) => f === c || f.includes(c)));
  }

  function checkChineseOfficeFonts() {
    const installed = listInstalledFonts();
    const bundled = listBundledFonts();
    const all = Array.from(new Set([...installed, ...bundled]));

    const coreAny = [
      { family: "Microsoft YaHei", files: ["msyh.ttc", "msyh.ttf", "msyhbd.ttc", "msyhbd.ttf"] },
      { family: "SimSun", files: ["simsun.ttc", "simsun.ttf"] },
      { family: "SimHei", files: ["simhei.ttf"] },
      { family: "Noto Sans CJK", files: ["notosanscjk-regular.ttc", "notosanscjk-sc-regular.otf", "notosanscjksc-regular.otf"] },
    ];
    const optional = [
      { family: "DengXian", files: ["deng.ttf", "dengb.ttf"] },
      { family: "KaiTi", files: ["simkai.ttf"] },
      { family: "FangSong", files: ["simfang.ttf"] },
    ];

    const coreInstalled = coreAny.filter((x) => hasAnyFontFile(all, x.files)).map((x) => x.family);
    const missingCore = coreAny.filter((x) => !hasAnyFontFile(all, x.files)).map((x) => x.family);
    const missingOptional = optional.filter((x) => !hasAnyFontFile(all, x.files)).map((x) => x.family);
    const ok = coreInstalled.length > 0;
    return {
      ok,
      installed_count: installed.length,
      bundled_count: bundled.length,
      core_installed: coreInstalled,
      missing_core: missingCore,
      missing_optional: missingOptional,
      suggestion: ok
        ? "Chinese office font check passed."
        : "No core Chinese font detected. Install YaHei/SimSun/SimHei, or place Noto CJK font files under tools/fonts.",
    };
  }

  function installBundledFontsForCurrentUser() {
    if (process.platform !== "win32") {
      return { ok: false, installed: 0, skipped: 0, reason: "not_windows" };
    }
    const srcFonts = listBundledFontPaths();
    if (!srcFonts.length) {
      return { ok: false, installed: 0, skipped: 0, reason: "no_bundled_fonts" };
    }
    const userFontsDir = path.join(process.env.LOCALAPPDATA || path.join(app.getPath("home"), "AppData", "Local"), "Microsoft", "Windows", "Fonts");
    try { fs.mkdirSync(userFontsDir, { recursive: true }); } catch {}

    let installed = 0;
    let skipped = 0;
    const errors = [];
    for (const src of srcFonts) {
      const base = path.basename(src);
      const dst = path.join(userFontsDir, base);
      try {
        if (!fs.existsSync(dst)) {
          fs.copyFileSync(src, dst);
          installed += 1;
        } else {
          skipped += 1;
        }
      } catch (e) {
        errors.push(`copy ${base}: ${String(e)}`);
      }
    }
    const recheck = checkChineseOfficeFonts();
    return {
      ok: errors.length === 0 || recheck.ok,
      installed,
      skipped,
      target: userFontsDir,
      errors: errors.slice(0, 10),
      font_check: recheck,
    };
  }

  return { checkChineseOfficeFonts, installBundledFontsForCurrentUser };
}

module.exports = { createFontSupport };
