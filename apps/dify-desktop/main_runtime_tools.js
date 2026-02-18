function createToolRuntimeSupport({ fs, path, execFileSync }) {
  function resolveBundledBinaryCandidates(relativePaths = []) {
    const roots = [];
    if (process.resourcesPath) roots.push(process.resourcesPath);
    roots.push(path.join(__dirname, ".."));
    const out = [];
    for (const root of roots) {
      for (const rel of relativePaths) {
        const p = path.join(root, rel);
        if (fs.existsSync(p)) out.push(p);
      }
    }
    return Array.from(new Set(out));
  }

  function resolveBinaryByName({ envVar = "", bundled = [], fixed = [], command = "" }) {
    const envPath = String(process.env[envVar] || "").trim();
    if (envPath && fs.existsSync(envPath)) return { ok: true, source: "env", path: envPath };

    const bundledMatches = resolveBundledBinaryCandidates(bundled);
    if (bundledMatches.length > 0) return { ok: true, source: "bundled", path: bundledMatches[0] };

    for (const p of fixed) {
      if (fs.existsSync(p)) return { ok: true, source: "fixed_path", path: p };
    }
    try {
      const finder = process.platform === "win32" ? "where" : "which";
      const out = execFileSync(finder, [command], { encoding: "utf8", windowsHide: true });
      const first = String(out || "").split(/\r?\n/).map((x) => x.trim()).find(Boolean);
      if (first) return { ok: true, source: "path", path: first };
    } catch {}
    return { ok: false, source: "none", path: "" };
  }

  function checkTesseractRuntime() {
    const fixed = process.platform === "win32"
      ? [
        "C:\\Program Files\\Tesseract-OCR\\tesseract.exe",
        "C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe",
      ]
      : process.platform === "darwin"
        ? ["/opt/homebrew/bin/tesseract", "/usr/local/bin/tesseract", "/opt/local/bin/tesseract"]
        : ["/usr/bin/tesseract", "/usr/local/bin/tesseract"];
    return resolveBinaryByName({
      envVar: "TESSERACT_CMD",
      bundled: [path.join("tools", "tesseract", "tesseract.exe"), path.join("tools", "tesseract.exe")],
      fixed,
      command: "tesseract",
    });
  }

  function checkPdftoppmRuntime() {
    const fixed = process.platform === "win32"
      ? [
        "C:\\Program Files\\poppler\\Library\\bin\\pdftoppm.exe",
        "C:\\Program Files\\poppler\\bin\\pdftoppm.exe",
      ]
      : process.platform === "darwin"
        ? ["/opt/homebrew/bin/pdftoppm", "/usr/local/bin/pdftoppm"]
        : ["/usr/bin/pdftoppm", "/usr/local/bin/pdftoppm"];
    return resolveBinaryByName({
      envVar: "PDFTOPPM_CMD",
      bundled: [
        path.join("tools", "poppler", "bin", "pdftoppm.exe"),
        path.join("tools", "poppler", "Library", "bin", "pdftoppm.exe"),
        path.join("tools", "pdftoppm.exe"),
      ],
      fixed,
      command: "pdftoppm",
    });
  }

  function checkTesseractLangs() {
    const t = checkTesseractRuntime();
    if (!t.ok) return { ok: false, langs: [] };
    try {
      const cmd = t.source === "path" ? "tesseract" : t.path;
      const out = execFileSync(cmd, ["--list-langs"], { encoding: "utf8", windowsHide: true });
      const langs = String(out || "")
        .split(/\r?\n/)
        .map((x) => x.trim())
        .filter((x) => x && !x.toLowerCase().includes("list of available languages"));
      return { ok: true, langs };
    } catch {
      return { ok: false, langs: [] };
    }
  }

  return {
    checkTesseractRuntime,
    checkPdftoppmRuntime,
    checkTesseractLangs,
  };
}

module.exports = { createToolRuntimeSupport };
