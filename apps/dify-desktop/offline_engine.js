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
  rowTextForQuality,
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
    fluent_ms: { title: "Fluent 宣传风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    fluent_ms_light: { title: "Fluent Light", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    fluent_ms_strong: { title: "Fluent Strong", primary: "005FB8", secondary: "083B7A", bg: "EEF5FD" },
    professional: { title: "专业风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    academic: { title: "学术风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    debate: { title: "辩论风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    assignment: { title: "作业风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    debate_plus: { title: "辩论增强", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
    business: { title: "商务风", primary: "0F6CBD", secondary: "0B3A75", bg: "F7FAFE" },
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
    fluent_ms: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 24,
      pptx_sample_rows: 10,
      pptx_max_findings: 8,
      pptx_max_evidence: 8,
    },
    fluent_ms_light: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 24,
      pptx_sample_rows: 10,
      pptx_max_findings: 8,
      pptx_max_evidence: 8,
    },
    fluent_ms_strong: {
      xlsx_data_sheet_name: "cleaned",
      xlsx_summary_sheet_name: "summary",
      docx_max_table_rows: 22,
      pptx_sample_rows: 8,
      pptx_max_findings: 7,
      pptx_max_evidence: 7,
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
const CLEANING_TEMPLATES = loadCleaningTemplates();

function toTemplateLabel(id) {
  if (!id) return "通用模板";
  if (id === "finance_report_v1") return "财报模板 v1（资产/利润/现金流）";
  const s = String(id).replace(/[_-]+/g, " ").trim();
  if (!s) return "通用模板";
  return s.replace(/\b\w/g, (m) => m.toUpperCase());
}

function normalizeTemplateEntry(entry, templatesDir) {
  if (!entry || typeof entry !== "object") return null;
  const id = String(entry.id || "").trim().toLowerCase();
  if (!id || id === "default") return null;
  const file = String(entry.file || "").trim();
  if (!file) return null;
  const fp = path.join(templatesDir, file);
  if (!fs.existsSync(fp)) return null;
  let rules = null;
  try {
    const raw = String(fs.readFileSync(fp, "utf8") || "").replace(/^\uFEFF/, "");
    const obj = JSON.parse(raw);
    if (!obj || typeof obj !== "object" || typeof obj.rules !== "object") return null;
    rules = obj.rules;
  } catch {
    return null;
  }
  return {
    id,
    file,
    label: String(entry.label || "").trim() || toTemplateLabel(id),
    description: String(entry.description || "").trim(),
    rules,
  };
}

function loadCleaningTemplates() {
  const templatesDir = path.join(__dirname, "..", "..", "rules", "templates");
  const out = [];
  const byId = new Map();
  const add = (entry) => {
    if (!entry || !entry.id || byId.has(entry.id)) return;
    byId.set(entry.id, entry);
    out.push(entry);
  };

  add({
    id: "default",
    file: "",
    label: "通用模板",
    description: "不追加模板规则，沿用通用清洗链路。",
    rules: null,
  });

  const registryPath = path.join(templatesDir, "cleaning_templates_desktop.json");
  try {
    if (fs.existsSync(registryPath)) {
      const raw = String(fs.readFileSync(registryPath, "utf8") || "").replace(/^\uFEFF/, "");
      const reg = JSON.parse(raw);
      const items = Array.isArray(reg?.templates) ? reg.templates : [];
      items.forEach((it) => add(normalizeTemplateEntry(it, templatesDir)));
    }
  } catch {}

  try {
    if (fs.existsSync(templatesDir)) {
      const files = fs.readdirSync(templatesDir).filter((n) => /^generic_.*\.json$/i.test(n));
      files.forEach((file) => {
        const fp = path.join(templatesDir, file);
        let obj = null;
        try {
          const raw = String(fs.readFileSync(fp, "utf8") || "").replace(/^\uFEFF/, "");
          obj = JSON.parse(raw);
        } catch {
          return;
        }
        if (!obj || typeof obj !== "object" || typeof obj.rules !== "object") return;
        const id = String(obj?.meta?.template_id || path.basename(file, ".json")).trim().toLowerCase();
        if (!id || byId.has(id)) return;
        add({
          id,
          file,
          label: String(obj?.meta?.template_label || "").trim() || toTemplateLabel(id),
          description: String(obj?.meta?.template_description || "").trim(),
          rules: obj.rules,
        });
      });
    }
  } catch {}

  return {
    list: out.map((x) => ({
      id: x.id,
      label: x.label,
      description: x.description,
      file: x.file,
      rules: x.rules || null,
    })),
    byId,
  };
}

function resolveOfficeTheme(name) {
  const forceFluent = String(process.env.AIWF_FORCE_FLUENT_STYLE || "1").trim() !== "0";
  if (forceFluent) {
    const nk = String(name || "").trim().toLowerCase();
    if (nk && OFFICE_THEMES[nk] && nk.startsWith("fluent_ms")) return OFFICE_THEMES[nk];
    return OFFICE_THEMES.fluent_ms_light || OFFICE_THEMES.fluent_ms || OFFICE_THEMES.assignment;
  }
  const k = String(name || "assignment").trim().toLowerCase();
  return OFFICE_THEMES[k] || OFFICE_THEMES.assignment;
}

function resolveOfficeLayout(name) {
  const forceFluent = String(process.env.AIWF_FORCE_FLUENT_STYLE || "1").trim() !== "0";
  const nk = String(name || "assignment").trim().toLowerCase();
  const k = forceFluent
    ? ((nk && nk.startsWith("fluent_ms")) ? nk : "fluent_ms_light")
    : nk;
  const base = OFFICE_LAYOUTS.default || {};
  const ext = OFFICE_LAYOUTS[k] || {};
  return { ...base, ...ext };
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function resolveCleaningTemplateParams(params = {}) {
  const p = { ...(params || {}) };
  const t = String(p.cleaning_template || "").trim().toLowerCase();
  if (!t || t === "default") return p;
  const tpl = CLEANING_TEMPLATES.byId.get(t);
  if (!tpl || !tpl.rules) return p;
  // Let caller-provided rules override template defaults.
  p.rules = { ...tpl.rules, ...(p.rules || {}) };
  return p;
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
    { family: "Noto Sans CJK SC", files: ["NotoSansCJKsc-Regular.otf", "NotoSansCJK-Regular.ttc"] },
    { family: "Source Han Sans SC", files: ["SourceHanSansSC-Regular.otf"] },
    { family: "Segoe UI", files: ["segoeui.ttf", "segoeuib.ttf"] },
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

function cleanRows(rawRows, params = {}) {
  return OFFLINE_INGEST.cleanRows(rawRows, params);
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
function filterRowsForOffice(rows) {
  if (typeof OFFLINE_OUTPUTS.filterRowsForOffice === "function") return OFFLINE_OUTPUTS.filterRowsForOffice(rows);
  return { rows: Array.isArray(rows) ? rows : [], filtered: 0, removedRows: [] };
}
function computeOfficeQualityScore(rows, quality, warnings, options = {}) {
  return OFFLINE_OUTPUTS.computeOfficeQualityScore(rows, quality, warnings, options);
}
function assessContentQuality(rows, options = {}) {
  if (typeof OFFLINE_OUTPUTS.assessContentQuality === "function") {
    return OFFLINE_OUTPUTS.assessContentQuality(rows, options);
  }
  return {
    score: 100,
    level: "good",
    pass: true,
    thresholds: { min_score: 60 },
    metrics: {
      title_coverage: 1,
      paragraph_coverage: 1,
      coherent_paragraph_ratio: 1,
      numeric_consistency: 1,
    },
    reasons: [],
  };
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

async function writeFilteredNoiseMarkdown(filePath, rows) {
  if (typeof OFFLINE_OUTPUTS.writeFilteredNoiseMarkdown === "function") {
    return OFFLINE_OUTPUTS.writeFilteredNoiseMarkdown(filePath, rows);
  }
}

async function runOfflinePrecheck(payload) {
  const params = resolveCleaningTemplateParams(payload?.params || {});
  const warnings = [];
  const runtime = {};
  const rawRows = await readInputRows(params, warnings, runtime);
  const precheck = OFFLINE_INGEST.precheckRows(rawRows, params);
  return {
    ok: true,
    mode: "offline_local",
    template: String(params.cleaning_template || "default").trim().toLowerCase() || "default",
    warnings,
    precheck,
  };
}

async function runOfflineDebatePreview(payload) {
  const params = resolveCleaningTemplateParams(payload?.params || {});
  const warnings = [];
  const runtime = {};
  const rawRows = await readInputRows(params, warnings, runtime);
  const cleaned = cleanRows(rawRows, params);
  const rows = Array.isArray(cleaned?.rows) ? cleaned.rows : [];
  const preview = typeof OFFLINE_OUTPUTS.buildDebatePreview === "function"
    ? OFFLINE_OUTPUTS.buildDebatePreview(rows, params, 10)
    : { total: 0, groups: [], rows: [] };
  return {
    ok: true,
    mode: "offline_local",
    template: String(params.cleaning_template || "default").trim().toLowerCase() || "default",
    warnings,
    quality: cleaned?.quality || {},
    preview,
  };
}

function listCleaningTemplates() {
  return { ok: true, templates: CLEANING_TEMPLATES.list };
}

function normalizeReportTitle(rawTitle, fallback = "辩论资料库") {
  const t = String(rawTitle || "").trim();
  if (!t) return fallback;
  const badCount = (t.match(/[?\uFFFD]/g) || []).length;
  const cjkCount = (t.match(/[\u4E00-\u9FFF]/g) || []).length;
  const ratio = badCount / Math.max(1, t.length);
  if (badCount >= 2 && (ratio >= 0.2 || cjkCount === 0)) return fallback;
  return t;
}

async function runOfflineCleaning(payload) {
  const t0 = Date.now();
  const params = resolveCleaningTemplateParams(payload?.params || {});
  const reportTitle = normalizeReportTitle(params.report_title || payload?.report_title || "", "辩论资料库");
  let mdOnly = params.md_only === true || String(params.output_format || "").toLowerCase() === "md";

  const jobId = uuidv4().replace(/-/g, "");
  const outRoot = payload?.output_root || path.join(process.cwd(), "offline-jobs");
  const jobRoot = path.join(outRoot, jobId);
  const artDir = path.join(jobRoot, "artifacts");
  const paperMdDir = path.join(artDir, "paper_markdown");
  ensureDir(artDir);

  const warnings = [];
  const runtime = { paperMdDir, paperMdRecords: [], fileQualityRecords: [] };
  const rawRows = await readInputRows(params, warnings, runtime);
  const cleaned = cleanRows(rawRows, params);
  let rows = cleaned.rows;
  let quality = cleaned.quality;
  let qualityGate = { evaluated: false, passed: false };
  const fidelity = { enabled: false, reasons: [] };

  const evalTextRows = (rows || [])
    .map((r) => rowTextForQuality(r))
    .filter((t) => String(t || "").length >= 4);
  const corruptRows = evalTextRows.filter((t) => isLikelyCorruptedText(t)).length;
  const corruptRatio = evalTextRows.length > 0 ? (corruptRows / evalTextRows.length) : 0;
  const autoFidelity = params.md_fidelity_auto !== false;

  function activateFidelity(reason) {
    if (fidelity.enabled) return;
    fidelity.enabled = true;
    fidelity.reasons.push(String(reason || "unknown"));
    const fallbackRows = OFFLINE_INGEST.buildFidelityRows(rawRows);
    if (fallbackRows.length > 0) {
      rows = fallbackRows;
      quality = {
        input_rows: rawRows.length,
        output_rows: fallbackRows.length,
        filtered_rows: Math.max(0, rawRows.length - fallbackRows.length),
        invalid_rows: 0,
        duplicate_rows_removed: 0,
      };
    }
  }

  try {
    qualityGate = applyQualityGates(quality, params);
  } catch (e) {
    const msg = String(e && e.message ? e.message : e);
    qualityGate = { evaluated: true, passed: false, blocked: true, error: msg };
    warnings.push(`质量门禁未通过，已切换文本保真模式: ${msg}`);
    activateFidelity(`quality_gate:${msg}`);
  }

  if (autoFidelity && !fidelity.enabled && rows.length === 0 && rawRows.length > 0) {
    warnings.push("清洗后结果为空，已切换文本保真模式。");
    activateFidelity("empty_output_after_clean");
  }
  if (!fidelity.enabled && corruptRatio >= 0.2) {
    if (autoFidelity && corruptRatio >= 0.35) {
      warnings.push(`清洗结果疑似乱码率过高(${(corruptRatio * 100).toFixed(1)}%，评估行=${evalTextRows.length})，已切换文本保真模式。`);
      activateFidelity(`gibberish_ratio_high:${corruptRatio.toFixed(4)}`);
    } else {
      warnings.push(`清洗结果疑似乱码率偏高(${(corruptRatio * 100).toFixed(1)}%，评估行=${evalTextRows.length})，建议检查源文件编码。`);
    }
  }
  if (fidelity.enabled) {
    qualityGate = {
      ...(qualityGate || {}),
      evaluated: true,
      passed: true,
      fallback_mode: "text_fidelity",
      fallback_reasons: fidelity.reasons.slice(),
    };
  }

  const strictOutputGate = params.strict_output_gate === true;
  const preflightPassed = params.preflight_passed !== false;
  if (strictOutputGate && !preflightPassed) {
    mdOnly = true;
    warnings.push("预检未通过，严格产物门禁已生效：仅输出 Markdown 熟肉。");
  }

  const xlsxPath = path.join(artDir, "fin.xlsx");
  const docxPath = path.join(artDir, "audit.docx");
  const pptxPath = path.join(artDir, "deck.pptx");
  const mdPath = path.join(artDir, "evidence.md");
  const filteredNoisePath = path.join(artDir, "filtered_noise.md");
  const paperMdIndexPath = path.join(artDir, "paper_markdown_index.md");
  const aiCorpusPath = path.join(artDir, "ai_corpus.md");
  const qualityReportPath = path.join(artDir, "quality_report.md");
  let filteredNoiseWritten = false;

  const qualityScore = computeOfficeQualityScore(rows, quality, warnings, { md_only: mdOnly });
  const contentQuality = assessContentQuality(rows, {
    min_content_score: params.min_content_score,
  });
  const outputGateMeta = {
    source_files: String(params.input_files || "").split(/\r?\n/).map((s) => s.trim()).filter(Boolean).slice(0, 20),
    strict_output_gate: strictOutputGate,
    preflight_passed: preflightPassed,
    preflight_risk_score: Number(params.preflight_risk_score || 0),
    preflight_risk_label: String(params.preflight_risk_label || ""),
    quality_score: Number(qualityScore?.score || 0),
    invalid_ratio: Number(qualityScore?.invalid_ratio || 0),
    gibberish_ratio: Number(qualityScore?.gibberish_ratio || 0),
    content_quality_score: Number(contentQuality?.score || 0),
    content_quality_level: String(contentQuality?.level || ""),
  };
  if (contentQuality && typeof contentQuality === "object") {
    outputGateMeta.content_quality = contentQuality;
  }

  const contentGateEnabled = params.content_quality_gate_enabled !== false;
  if (contentGateEnabled && !mdOnly && !contentQuality.pass) {
    mdOnly = true;
    warnings.push(`内容质量门禁未通过（score=${Number(contentQuality.score || 0).toFixed(1)} < min=${Number(contentQuality.thresholds?.min_score || 60).toFixed(1)}），已自动切换为仅输出 Markdown。`);
  }
  const officeQualityGateEnabled = params.office_quality_gate_enabled !== false;
  const minOfficeQualityScore = Number.isFinite(Number(params.min_office_quality_score))
    ? Math.max(0, Math.min(100, Number(params.min_office_quality_score)))
    : 65;
  if (officeQualityGateEnabled && !mdOnly && Number(qualityScore?.score || 0) < minOfficeQualityScore) {
    mdOnly = true;
    warnings.push(`Office质量门禁未通过（score=${Number(qualityScore?.score || 0).toFixed(1)} < min=${minOfficeQualityScore.toFixed(1)}），已自动切换为仅输出 Markdown。`);
    outputGateMeta.office_quality_gate_blocked = true;
    outputGateMeta.office_quality_gate_min_score = minOfficeQualityScore;
  }

  await writeMarkdown(mdPath, jobId, reportTitle, rows, quality, warnings);
  writePaperMarkdownIndex(paperMdIndexPath, runtime.paperMdRecords);
  writeAiCorpusMarkdown(aiCorpusPath, runtime.paperMdRecords);
  writeQualityReport(qualityReportPath, rows, warnings, runtime.fileQualityRecords, {
    paperRecords: runtime.paperMdRecords,
    fidelity,
    qualityScore,
    contentQuality,
    outputGateMeta: outputGateMeta,
  });
  if (!mdOnly) {
    const officeFiltered = filterRowsForOffice(rows);
    const officeRows = Array.isArray(officeFiltered?.rows) ? officeFiltered.rows : rows;
    const filteredCount = Number(officeFiltered?.filtered || 0);
    const filteredRemovedRows = Array.isArray(officeFiltered?.removedRows) ? officeFiltered.removedRows : [];
    if (filteredCount > 0) warnings.push(`已过滤 ${filteredCount} 行“问号密度过高”文本，避免 Office 成品出现乱码片段。`);
    outputGateMeta.filtered_question_mark_rows = filteredCount;
    if (filteredRemovedRows.length > 0) {
      await writeFilteredNoiseMarkdown(filteredNoisePath, filteredRemovedRows);
      filteredNoiseWritten = true;
    }
    await writeXlsx(xlsxPath, officeRows, quality, warnings, { ...params, output_gate_meta: outputGateMeta });
    await writeDocx(docxPath, jobId, reportTitle, officeRows, quality, warnings, { ...params, output_gate_meta: outputGateMeta });
    await writePptx(pptxPath, reportTitle, officeRows, quality, warnings, { ...params, output_gate_meta: outputGateMeta });
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
  if (filteredNoiseWritten) {
    artifacts.push({ artifact_id: "md_filtered_noise_001", kind: "md", path: filteredNoisePath, sha256: sha256File(filteredNoisePath) });
  }

  return {
    ok: true,
    job_id: jobId,
    mode: "offline_local",
    run: {
      ok: true,
      seconds: Math.round(((Date.now() - t0) / 1000) * 1000) / 1000,
    },
    quality,
    quality_score: qualityScore,
    quality_gate: qualityGate,
    warnings,
    artifacts,
  };
}

module.exports = { runOfflineCleaning, runOfflinePrecheck, runOfflineDebatePreview, listCleaningTemplates };




