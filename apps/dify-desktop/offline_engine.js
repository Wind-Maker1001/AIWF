const fs = require("fs");
const os = require("os");
const path = require("path");
const crypto = require("crypto");
const { Readable } = require("stream");
const ExcelJS = require("exceljs");
const mammoth = require("mammoth");
const pdfParseMod = require("pdf-parse");
const imageSize = require("image-size");
const { v4: uuidv4 } = require("uuid");
const { readTextFileSmart } = require("./offline_text");
const {
  listInstalledFonts: listInstalledRuntimeFonts,
  hasAnyFontFile,
  runImageOcr,
  runPdfOcr,
} = require("./offline_runtime");
const {
  splitPdfTextToEvidence,
  scorePdfExtractText,
  isLikelyCorruptedText,
  materializePaperMarkdown,
  writePaperMarkdownIndex,
  writeAiCorpusMarkdown,
  writeQualityReport,
} = require("./offline_paper");
const { createOfflineOutputs } = require("./offline_outputs");
const { createOfflineIngest } = require("./offline_ingest");
const IMG_EXT = new Set([".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"]);
function loadDesktopThemes() {
  const defaults = {
    professional: { title: "专业风", primary: "1F4E78", secondary: "2C6A9D", bg: "EEF5FC" },
    academic: { title: "学术风", primary: "2F3E63", secondary: "445A8D", bg: "F1F3FA" },
    debate: { title: "辩论风", primary: "7A1F1F", secondary: "9C2F2F", bg: "FDF4F4" },
    assignment: { title: "作业风", primary: "245C73", secondary: "2F7C94", bg: "EEF8FB" },
    debate_plus: { title: "辩论增强", primary: "5E1B4F", secondary: "7C2868", bg: "FAF1F8" },
    business: { title: "商务风", primary: "1B4D3E", secondary: "2D7561", bg: "ECF7F2" },
  };
  const p = process.env.AIWF_OFFICE_THEME_FILE_DESKTOP
    || path.join(__dirname, "..", "..", "rules", "templates", "office_themes_desktop.json");
  try {
    if (!fs.existsSync(p)) return defaults;
    const obj = JSON.parse(fs.readFileSync(p, "utf8"));
    if (obj && typeof obj === "object") return { ...defaults, ...obj };
  } catch {}
  return defaults;
}

function loadDesktopLayouts() {
  const defaults = {
    default: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 20,
      pptx_sample_rows: 8,
      pptx_max_findings: 6,
      pptx_max_evidence: 6,
    },
  };
  const p = process.env.AIWF_OFFICE_LAYOUT_FILE_DESKTOP
    || path.join(__dirname, "..", "..", "rules", "templates", "office_layouts_desktop.json");
  try {
    if (!fs.existsSync(p)) return defaults;
    const obj = JSON.parse(fs.readFileSync(p, "utf8"));
    if (obj && typeof obj === "object") return { ...defaults, ...obj };
  } catch {}
  return defaults;
}

const OFFICE_THEMES = loadDesktopThemes();
const OFFICE_LAYOUTS = loadDesktopLayouts();

function resolveOfficeTheme(name) {
  const k = String(name || "assignment").trim().toLowerCase();
  return OFFICE_THEMES[k] || OFFICE_THEMES.assignment;
}

function resolveOfficeLayout(name) {
  const k = String(name || "assignment").trim().toLowerCase();
  const base = OFFICE_LAYOUTS.default || {};
  const ext = OFFICE_LAYOUTS[k] || {};
  return { ...base, ...ext };
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function sha256File(p) {
  const h = crypto.createHash("sha256");
  h.update(fs.readFileSync(p));
  return h.digest("hex");
}

function normalizeCell(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

function normalizeAmount(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[,，\s$¥￥\uFFFD?]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  if (!Number.isFinite(n)) return null;
  return Math.round(n * 100) / 100;
}

function resolveOfficeFont(params = {}, warnings = []) {
  const lang = String(params.office_lang || "zh").toLowerCase();
  const installed = listInstalledRuntimeFonts();
  if (lang === "en") return "Calibri";

  const candidates = [
    { family: "Microsoft YaHei", files: ["msyh.ttc", "msyh.ttf", "msyhbd.ttc", "msyhbd.ttf"] },
    { family: "SimHei", files: ["simhei.ttf"] },
    { family: "SimSun", files: ["simsun.ttc", "simsun.ttf"] },
    { family: "DengXian", files: ["deng.ttf", "dengb.ttf"] },
  ];

  for (const c of candidates) {
    if (hasAnyFontFile(installed, c.files)) return c.family;
  }

  warnings.push("\u672a\u68c0\u6d4b\u5230\u6838\u5fc3\u4e2d\u6587\u5b57\u4f53\uff0c\u5df2\u56de\u9000\u5230 Calibri\uff0c\u53ef\u80fd\u51fa\u73b0\u4e2d\u6587\u663e\u793a\u5f02\u5e38\u3002");
  return "Calibri";
}

const OFFLINE_OUTPUTS = createOfflineOutputs({
  resolveOfficeTheme,
  resolveOfficeFont,
  resolveOfficeLayout,
});
const OFFLINE_INGEST = createOfflineIngest({
  fs,
  path,
  Readable,
  ExcelJS,
  mammoth,
  imageSize,
  readTextFileSmart,
  runImageOcr,
  runPdfOcr,
  splitPdfTextToEvidence,
  scorePdfExtractText,
  isLikelyCorruptedText,
  materializePaperMarkdown,
  extractPdfTextFromBuffer,
  normalizeCell,
  normalizeAmount,
  IMG_EXT,
});

async function extractPdfTextFromBuffer(buf) {
  if (typeof pdfParseMod === "function") {
    const out = await pdfParseMod(buf);
    return String((out && out.text) || "");
  }
  if (pdfParseMod && typeof pdfParseMod.default === "function") {
    const out = await pdfParseMod.default(buf);
    return String((out && out.text) || "");
  }
  const PDFParse = pdfParseMod && pdfParseMod.PDFParse;
  if (typeof PDFParse === "function") {
    let parser = null;
    try {
      parser = new PDFParse({ data: buf });
      const out = await parser.getText();
      return String((out && out.text) || "");
    } finally {
      try {
        if (parser && typeof parser.destroy === "function") parser.destroy();
      } catch {}
    }
  }
  throw new Error("unsupported pdf-parse module shape");
}

async function readInputRows(params, warnings, runtime = {}) {
  return OFFLINE_INGEST.readInputRows(params, warnings, runtime);
}

function cleanRows(rawRows) {
  return OFFLINE_INGEST.cleanRows(rawRows);
}

function applyQualityGates(quality, params = {}) {
  return OFFLINE_INGEST.applyQualityGates(quality, params);
}
function buildDataQualityInsights(rows) {
  return OFFLINE_OUTPUTS.buildDataQualityInsights(rows);
}

function buildEvidenceHighlights(rows, maxItems = 8) {
  return OFFLINE_OUTPUTS.buildEvidenceHighlights(rows, maxItems);
}

function unionColumns(rows) {
  return OFFLINE_OUTPUTS.unionColumns(rows);
}

async function writeXlsx(filePath, rows, quality, warnings, options = {}) {
  return OFFLINE_OUTPUTS.writeXlsx(filePath, rows, quality, warnings, options);
}

async function writeDocx(filePath, jobId, reportTitle, rows, quality, warnings, options = {}) {
  return OFFLINE_OUTPUTS.writeDocx(filePath, jobId, reportTitle, rows, quality, warnings, options);
}

async function writePptx(filePath, reportTitle, rows, quality, warnings, options = {}) {
  return OFFLINE_OUTPUTS.writePptx(filePath, reportTitle, rows, quality, warnings, options);
}

async function writeMarkdown(filePath, jobId, reportTitle, rows, quality, warnings) {
  return OFFLINE_OUTPUTS.writeMarkdown(filePath, jobId, reportTitle, rows, quality, warnings);
}
async function runOfflineCleaning(payload) {
  const t0 = Date.now();
  const params = payload?.params || {};
  const reportTitle = params.report_title || payload?.report_title || "离线作业成品";
  const mdOnly = params.md_only === true || String(params.output_format || "").toLowerCase() === "md";

  const jobId = uuidv4().replace(/-/g, "");
  const outRoot = payload?.output_root || path.join(process.cwd(), "offline-jobs");
  const jobRoot = path.join(outRoot, jobId);
  const artDir = path.join(jobRoot, "artifacts");
  const paperMdDir = path.join(artDir, "paper_markdown");
  ensureDir(artDir);

  const warnings = [];
  const runtime = { paperMdDir, paperMdRecords: [], fileQualityRecords: [] };
  const rawRows = await readInputRows(params, warnings, runtime);
  const cleaned = cleanRows(rawRows);
  const rows = cleaned.rows;
  const quality = cleaned.quality;
  const qualityGate = applyQualityGates(quality, params);

  const xlsxPath = path.join(artDir, "fin.xlsx");
  const docxPath = path.join(artDir, "audit.docx");
  const pptxPath = path.join(artDir, "deck.pptx");
  const mdPath = path.join(artDir, "evidence.md");
  const paperMdIndexPath = path.join(artDir, "paper_markdown_index.md");
  const aiCorpusPath = path.join(artDir, "ai_corpus.md");
  const qualityReportPath = path.join(artDir, "quality_report.md");

  await writeMarkdown(mdPath, jobId, reportTitle, rows, quality, warnings);
  writePaperMarkdownIndex(paperMdIndexPath, runtime.paperMdRecords);
  writeAiCorpusMarkdown(aiCorpusPath, runtime.paperMdRecords);
  writeQualityReport(qualityReportPath, rows, warnings, runtime.fileQualityRecords);
  if (!mdOnly) {
    await writeXlsx(xlsxPath, rows, quality, warnings, params);
    await writeDocx(docxPath, jobId, reportTitle, rows, quality, warnings, params);
    await writePptx(pptxPath, reportTitle, rows, quality, warnings, params);
  }

  const artifacts = [];
  if (!mdOnly) {
    artifacts.push(
      { artifact_id: "xlsx_fin_001", kind: "xlsx", path: xlsxPath, sha256: sha256File(xlsxPath) },
      { artifact_id: "docx_audit_001", kind: "docx", path: docxPath, sha256: sha256File(docxPath) },
      { artifact_id: "pptx_deck_001", kind: "pptx", path: pptxPath, sha256: sha256File(pptxPath) }
    );
  }
  artifacts.push(
    { artifact_id: "md_evidence_001", kind: "md", path: mdPath, sha256: sha256File(mdPath) },
    { artifact_id: "md_paper_index_001", kind: "md", path: paperMdIndexPath, sha256: sha256File(paperMdIndexPath) },
    { artifact_id: "md_ai_corpus_001", kind: "md", path: aiCorpusPath, sha256: sha256File(aiCorpusPath) },
    { artifact_id: "md_quality_001", kind: "md", path: qualityReportPath, sha256: sha256File(qualityReportPath) }
  );

  return {
    ok: true,
    job_id: jobId,
    mode: "offline_local",
    run: {
      ok: true,
      seconds: Math.round(((Date.now() - t0) / 1000) * 1000) / 1000,
    },
    quality,
    quality_gate: qualityGate,
    warnings,
    artifacts,
  };
}

module.exports = { runOfflineCleaning };




