const fs = require("fs");
const os = require("os");
const path = require("path");
const { execFileSync } = require("child_process");

function listInstalledFonts() {
  const dirs = [];
  if (process.platform === "win32") {
    dirs.push(path.join(process.env.WINDIR || "C:\\Windows", "Fonts"));
  } else if (process.platform === "darwin") {
    dirs.push("/System/Library/Fonts", "/Library/Fonts", path.join(os.homedir(), "Library", "Fonts"));
  } else {
    dirs.push("/usr/share/fonts", "/usr/local/share/fonts", path.join(os.homedir(), ".fonts"));
  }
  const out = [];
  for (const d of dirs) {
    if (!fs.existsSync(d)) continue;
    try {
      out.push(...fs.readdirSync(d).map((x) => String(x || "").toLowerCase()));
    } catch {}
  }
  return Array.from(new Set(out));
}

function hasAnyFontFile(installed, candidates) {
  const lower = candidates.map((x) => String(x || "").toLowerCase());
  return installed.some((f) => lower.some((c) => f === c || f.includes(c)));
}

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

function resolveTesseractCmd() {
  const env = String(process.env.TESSERACT_CMD || "").trim();
  if (env && fs.existsSync(env)) return env;
  const bundled = resolveBundledBinaryCandidates([
    path.join("tools", "tesseract", "tesseract.exe"),
    path.join("tools", "tesseract.exe"),
  ]);
  if (bundled.length > 0) return bundled[0];
  try {
    execFileSync(process.platform === "win32" ? "where" : "which", ["tesseract"], { windowsHide: true, stdio: "ignore" });
    return "tesseract";
  } catch {}
  const candidates = process.platform === "win32"
    ? ["C:\\Program Files\\Tesseract-OCR\\tesseract.exe", "C:\\Program Files (x86)\\Tesseract-OCR\\tesseract.exe"]
    : process.platform === "darwin"
      ? ["/opt/homebrew/bin/tesseract", "/usr/local/bin/tesseract"]
      : ["/usr/bin/tesseract", "/usr/local/bin/tesseract"];
  for (const c of candidates) if (fs.existsSync(c)) return c;
  return null;
}

function resolveTessdataDir() {
  const explicit = String(process.env.TESSDATA_PREFIX || "").trim();
  if (explicit && fs.existsSync(path.join(explicit, "eng.traineddata"))) return explicit;
  const bundled = resolveBundledBinaryCandidates([
    path.join("tools", "tesseract", "tessdata"),
    path.join("tools", "tessdata"),
  ]);
  for (const p of bundled) {
    if (fs.existsSync(path.join(p, "eng.traineddata")) || fs.existsSync(path.join(p, "chi_sim.traineddata"))) return p;
  }
  const userDir = path.join(process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"), "AIWF", "tessdata");
  if (fs.existsSync(path.join(userDir, "eng.traineddata")) || fs.existsSync(path.join(userDir, "chi_sim.traineddata"))) return userDir;
  const candidates = process.platform === "win32"
    ? ["C:\\Program Files\\Tesseract-OCR\\tessdata", "C:\\Program Files (x86)\\Tesseract-OCR\\tessdata"]
    : process.platform === "darwin"
      ? ["/opt/homebrew/share/tessdata", "/usr/local/share/tessdata", "/opt/homebrew/Cellar/tesseract/share/tessdata"]
      : ["/usr/share/tesseract-ocr/4.00/tessdata", "/usr/share/tesseract-ocr/5/tessdata", "/usr/share/tessdata"];
  for (const c of candidates) {
    if (fs.existsSync(path.join(c, "eng.traineddata")) || fs.existsSync(path.join(c, "chi_sim.traineddata"))) return c;
  }
  return "";
}

function runImageOcr(filePath, params = {}) {
  const cmd = resolveTesseractCmd();
  if (!cmd) return { ok: false, reason: "tesseract_missing", text: "" };
  const lang = String(params.ocr_lang || "chi_sim+eng").trim() || "chi_sim+eng";
  const config = String(params.ocr_config || "--oem 1 --psm 6").trim() || "--oem 1 --psm 6";
  const extraArgs = config.split(/\s+/).filter(Boolean);
  const tessdataDir = resolveTessdataDir();
  const tessdataArgs = tessdataDir ? ["--tessdata-dir", tessdataDir] : [];
  try {
    const out = execFileSync(cmd, [filePath, "stdout", "-l", lang, ...tessdataArgs, ...extraArgs], {
      encoding: "utf8",
      windowsHide: true,
      timeout: 45000,
      maxBuffer: 10 * 1024 * 1024,
    });
    return { ok: true, text: String(out || "") };
  } catch (e) {
    return { ok: false, reason: "ocr_failed", text: "", error: String(e) };
  }
}

function resolvePdftoppmCmd() {
  const env = String(process.env.PDFTOPPM_CMD || "").trim();
  if (env && fs.existsSync(env)) return env;
  const bundled = resolveBundledBinaryCandidates([
    path.join("tools", "poppler", "bin", "pdftoppm.exe"),
    path.join("tools", "poppler", "Library", "bin", "pdftoppm.exe"),
    path.join("tools", "pdftoppm.exe"),
  ]);
  if (bundled.length > 0) return bundled[0];
  try {
    execFileSync(process.platform === "win32" ? "where" : "which", ["pdftoppm"], { windowsHide: true, stdio: "ignore" });
    return "pdftoppm";
  } catch {}
  const candidates = process.platform === "win32"
    ? [
      "C:\\Program Files\\poppler\\Library\\bin\\pdftoppm.exe",
      "C:\\Program Files\\poppler\\bin\\pdftoppm.exe",
    ]
    : process.platform === "darwin"
      ? ["/opt/homebrew/bin/pdftoppm", "/usr/local/bin/pdftoppm"]
      : ["/usr/bin/pdftoppm", "/usr/local/bin/pdftoppm"];
  if (process.platform === "win32") {
    const wingetPoppler = path.join(
      process.env.LOCALAPPDATA || path.join(os.homedir(), "AppData", "Local"),
      "Microsoft",
      "WinGet",
      "Packages"
    );
    if (fs.existsSync(wingetPoppler)) {
      try {
        const dirs = fs.readdirSync(wingetPoppler).filter((d) => d.toLowerCase().includes("poppler"));
        for (const d of dirs) {
          candidates.push(path.join(wingetPoppler, d, "poppler-25.07.0", "Library", "bin", "pdftoppm.exe"));
          candidates.push(path.join(wingetPoppler, d, "poppler-24.08.0", "Library", "bin", "pdftoppm.exe"));
        }
      } catch {}
    }
  }
  for (const c of candidates) if (fs.existsSync(c)) return c;
  return null;
}

function runPdfOcr(filePath, params = {}) {
  const pdftoppm = resolvePdftoppmCmd();
  if (!pdftoppm) return { ok: false, reason: "pdftoppm_missing", text: "" };
  if (!resolveTesseractCmd()) return { ok: false, reason: "tesseract_missing", text: "" };
  const dpi = Number(params.ocr_pdf_dpi || 220);
  const maxPages = Number(params.ocr_pdf_max_pages || 120);
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-pdfocr-"));
  try {
    const prefix = path.join(tmpDir, "page");
    execFileSync(
      pdftoppm,
      ["-f", "1", "-l", String(Number.isFinite(maxPages) && maxPages > 0 ? Math.floor(maxPages) : 40), "-png", "-r", String(Number.isFinite(dpi) ? dpi : 220), filePath, prefix],
      {
        windowsHide: true,
        stdio: "ignore",
        timeout: 420000,
      }
    );
    const pages = fs.readdirSync(tmpDir)
      .filter((f) => f.toLowerCase().startsWith("page-") && f.toLowerCase().endsWith(".png"))
      .sort((a, b) => a.localeCompare(b, undefined, { numeric: true }));
    if (pages.length === 0) return { ok: false, reason: "pdf_render_empty", text: "" };
    const joined = [];
    for (const p of pages) {
      const ocr = runImageOcr(path.join(tmpDir, p), params);
      if (ocr.ok && String(ocr.text || "").trim()) joined.push(String(ocr.text || "").trim());
    }
    const text = joined.join("\n");
    if (!text.trim()) return { ok: false, reason: "pdf_ocr_empty", text: "" };
    return { ok: true, text };
  } catch (e) {
    return { ok: false, reason: "pdf_ocr_failed", text: "", error: String(e) };
  } finally {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  }
}

module.exports = {
  listInstalledFonts,
  hasAnyFontFile,
  resolveBundledBinaryCandidates,
  resolveTesseractCmd,
  resolveTessdataDir,
  runImageOcr,
  resolvePdftoppmCmd,
  runPdfOcr,
};
