const fs = require("fs");
const os = require("os");
const path = require("path");
const { Readable } = require("stream");
const { readTextFileSmart } = require("./offline_text");
const {
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
const { createOfflineEngineConfig } = require("./offline_engine_config");
const {
  ExcelJS,
  IMG_EXT,
  ensureDir,
  extractPdfTextFromBuffer,
  imageSize,
  isMissingNodeModuleError,
  makeJobId,
  mammoth,
  normalizeAmount,
  normalizeCell,
  resolveOfficeFont,
  sha256File,
} = require("./offline_engine_runtime");

const {
  listCleaningTemplates,
  normalizeReportTitle,
  resolveCleaningTemplateParams,
  resolveOfficeLayout,
  resolveOfficeTheme,
} = createOfflineEngineConfig();

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
  fetch: global.fetch,
  glueSidecarUrl: () => String(process.env.AIWF_GLUE_URL || "http://127.0.0.1:18081").trim(),
});

function parseInputFilesList(value) {
  if (Array.isArray(value)) {
    return value.map((item) => String(item || "").trim()).filter(Boolean);
  }
  const text = String(value || "").trim();
  if (!text) return [];
  if (text.startsWith("[") && text.endsWith("]")) {
    try {
      const parsed = JSON.parse(text);
      if (Array.isArray(parsed)) {
        return parsed.map((item) => String(item || "").trim()).filter(Boolean);
      }
    } catch {}
  }
  return text.split(/\r?\n/).map((item) => item.trim()).filter(Boolean);
}

function isCsvPath(filePath) {
  return /\.csv$/i.test(String(filePath || "").trim());
}

async function fetchJsonWithTimeout(url, options = {}, timeoutMs = 10000) {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(url, { ...options, signal: controller.signal });
    const text = await response.text();
    let json = {};
    try { json = JSON.parse(text || "{}"); } catch { json = { ok: false, error: text }; }
    return { ok: response.ok, status: response.status, json };
  } finally {
    clearTimeout(timer);
  }
}

function parseCsvText(text) {
  const rows = [];
  const records = [];
  let field = "";
  let row = [];
  let inQuotes = false;
  const src = String(text || "");
  for (let i = 0; i < src.length; i += 1) {
    const ch = src[i];
    if (inQuotes) {
      if (ch === '"') {
        if (src[i + 1] === '"') {
          field += '"';
          i += 1;
        } else {
          inQuotes = false;
        }
      } else {
        field += ch;
      }
      continue;
    }
    if (ch === '"') {
      inQuotes = true;
    } else if (ch === ",") {
      row.push(field);
      field = "";
    } else if (ch === "\n") {
      row.push(field);
      records.push(row);
      row = [];
      field = "";
    } else if (ch === "\r") {
      continue;
    } else {
      field += ch;
    }
  }
  if (field.length > 0 || row.length > 0) {
    row.push(field);
    records.push(row);
  }
  if (!records.length) return rows;
  const headers = records[0].map((item, idx) => String(item || "").trim() || `col_${idx + 1}`);
  for (let i = 1; i < records.length; i += 1) {
    const record = records[i];
    if (!record.some((item) => String(item || "").trim())) continue;
    const item = {};
    headers.forEach((header, idx) => {
      item[header] = record[idx] ?? "";
    });
    rows.push(item);
  }
  return rows;
}

function getGlueCleaningCompatibilityQuality(glueResult) {
  if (glueResult && glueResult.quality && typeof glueResult.quality === "object") {
    return glueResult.quality;
  }
  if (glueResult && glueResult.profile && typeof glueResult.profile.quality === "object") {
    return glueResult.profile.quality;
  }
  return {};
}

function getGlueCleaningCompatibilityExecution(glueResult) {
  if (glueResult && glueResult.execution && typeof glueResult.execution === "object") {
    return glueResult.execution;
  }
  if (glueResult && glueResult.profile && typeof glueResult.profile.execution === "object") {
    return glueResult.profile.execution;
  }
  return {};
}

function getGlueCleaningCompatibilityQualityGate(glueResult) {
  if (glueResult && glueResult.quality_gate && typeof glueResult.quality_gate === "object") {
    return glueResult.quality_gate;
  }
  if (glueResult && glueResult.profile && typeof glueResult.profile.quality_gate === "object") {
    return glueResult.profile.quality_gate;
  }
  return { evaluated: false, passed: true };
}

async function tryRunGlueFirstCleaning({ payload, params, outRoot, jobId, warnings }) {
  const glueUrl = String(payload?.glue_url || params?.glue_sidecar_url || process.env.AIWF_GLUE_URL || "http://127.0.0.1:18081").trim().replace(/\/$/, "");
  const accelUrl = String(process.env.AIWF_ACCEL_URL || "http://127.0.0.1:18082").trim().replace(/\/$/, "");
  const glueHealth = await fetchJsonWithTimeout(`${glueUrl}/health`, { method: "GET" }, 3000).catch(() => ({ ok: false, json: {} }));
  if (!glueHealth.ok || !glueHealth.json || !glueHealth.json.ok) {
    warnings.push(`glue-first cleaning unavailable, fallback to local cleaner: ${glueUrl}`);
    return { ok: false, reason: "glue_unavailable" };
  }
  const accelHealth = await fetchJsonWithTimeout(`${accelUrl}/health`, { method: "GET" }, 3000).catch(() => ({ ok: false, json: {} }));
  if (!accelHealth.ok || !accelHealth.json || !accelHealth.json.ok) {
    warnings.push(`accel-rust unavailable, fallback to local cleaner: ${accelUrl}`);
    return { ok: false, reason: "accel_unavailable" };
  }

  const inputFiles = parseInputFilesList(params.input_files);
  const directCsvPath = String(
    params.input_csv_path || params.source_csv_path || (inputFiles.length === 1 && isCsvPath(inputFiles[0]) ? inputFiles[0] : "")
  ).trim();

  const glueParams = { ...params, local_standalone: true };
  if (directCsvPath) {
    glueParams.input_csv_path = directCsvPath;
  } else {
    const ingestBody = {
      input_files: inputFiles,
      text_split_by_line: params.text_split_by_line === true,
      ocr_enabled: params.ocr_enabled !== false,
      ocr_lang: params.ocr_lang || null,
      ocr_config: params.ocr_config || null,
      ocr_preprocess: params.ocr_preprocess || null,
      xlsx_all_sheets: params.xlsx_all_sheets !== false,
      include_hidden_sheets: params.include_hidden_sheets === true,
      sheet_allowlist: Array.isArray(params.sheet_allowlist) ? params.sheet_allowlist : [],
      quality_rules: params.quality_rules && typeof params.quality_rules === "object" ? params.quality_rules : {},
      image_rules: params.image_rules && typeof params.image_rules === "object" ? params.image_rules : {},
      xlsx_rules: params.xlsx_rules && typeof params.xlsx_rules === "object" ? params.xlsx_rules : {},
      sheet_profiles: params.sheet_profiles && typeof params.sheet_profiles === "object" ? params.sheet_profiles : {},
      canonical_profile: String(params.canonical_profile || ""),
      on_file_error: "raise",
    };
    const ingest = await fetchJsonWithTimeout(`${glueUrl}/ingest/extract`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(ingestBody),
    }, 300000).catch((error) => ({ ok: false, json: { error: String(error) } }));
    if (!ingest.ok || !ingest.json || ingest.json.ok === false || !Array.isArray(ingest.json.rows) || ingest.json.rows.length === 0) {
      warnings.push(`glue extract failed, fallback to local cleaner: ${String((ingest.json && ingest.json.error) || "no_rows")}`);
      return { ok: false, reason: "glue_extract_failed" };
    }
    glueParams.rows = ingest.json.rows;
  }

  const jobRoot = path.join(outRoot, jobId);
  const jobContext = {
    job_root: jobRoot,
    stage_dir: path.join(jobRoot, "stage"),
    artifacts_dir: path.join(jobRoot, "artifacts"),
    evidence_dir: path.join(jobRoot, "evidence"),
  };
  const body = {
    actor: "desktop",
    ruleset_version: "v1",
    params: glueParams,
    job_context: jobContext,
  };
  const glueRun = await fetchJsonWithTimeout(`${glueUrl}/jobs/${jobId}/run/cleaning`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  }, 300000).catch((error) => ({ ok: false, json: { error: String(error) } }));
  if (!glueRun.ok || !glueRun.json || glueRun.json.ok === false) {
    warnings.push(`glue cleaning failed, fallback to local cleaner: ${String((glueRun.json && glueRun.json.error) || "run_cleaning_failed")}`);
    return { ok: false, reason: "glue_cleaning_failed" };
  }

  const execution = getGlueCleaningCompatibilityExecution(glueRun.json);
  const eligibilityReason = String(execution.eligibility_reason || "");
  if (eligibilityReason === "rust_v2_error" || eligibilityReason === "shadow_compare_mismatch") {
    warnings.push(`glue cleaning fallback triggered: ${eligibilityReason}`);
    return { ok: false, reason: eligibilityReason, glue_result: glueRun.json };
  }
  if (!Array.isArray(glueRun.json.artifacts)) {
    warnings.push("glue cleaning returned invalid artifact shape, fallback to local cleaner");
    return { ok: false, reason: "invalid_glue_response" };
  }

  const quality = getGlueCleaningCompatibilityQuality(glueRun.json);
  const qualityGate = getGlueCleaningCompatibilityQualityGate(glueRun.json);
  const csvArtifact = glueRun.json.artifacts.find((item) => String(item?.artifact_id || "") === "csv_cleaned_001" || String(item?.kind || "").toLowerCase() === "csv");
  let rows = [];
  if (csvArtifact && csvArtifact.path && fs.existsSync(csvArtifact.path)) {
    rows = parseCsvText(readTextFileSmart(String(csvArtifact.path)));
  }
  const qualityScore = rows.length > 0
    ? computeOfficeQualityScore(rows, quality, warnings, { md_only: params.md_only === true })
    : {
        score: String(execution.execution_mode || "") === "rust_v2" ? 100 : 80,
        pass: true,
        level: String(execution.execution_mode || "") === "rust_v2" ? "good" : "fair",
        thresholds: { min_score: 60 },
        metrics: {},
      };
  const artifacts = Array.isArray(glueRun.json.artifacts) ? glueRun.json.artifacts : [];
  const artifactsDir = jobContext.artifacts_dir;
  return {
    ok: true,
    job_id: String(glueRun.json.job_id || jobId),
    mode: "offline_local",
    run: {
      ok: true,
      seconds: Math.round((Number(glueRun.json.seconds || 0) || 0) * 1000) / 1000,
    },
    quality,
    execution,
    quality_score: qualityScore,
    quality_gate: qualityGate,
    warnings,
    artifacts,
    artifacts_dir: artifactsDir,
    profile: glueRun.json.profile && typeof glueRun.json.profile === "object" ? glueRun.json.profile : {},
    raw: glueRun.json,
  };
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
  if (payload?.glue_url && !params.glue_sidecar_url) params.glue_sidecar_url = payload.glue_url;
  const warnings = [];
  const runtime = {};
  let rawRows = [];
  try {
    rawRows = await readInputRows(params, warnings, runtime);
  } catch (e) {
    const error = String(e && e.message ? e.message : e);
    return {
      ok: false,
      mode: "offline_local",
      template: String(params.cleaning_template || "default").trim().toLowerCase() || "default",
      warnings,
      precheck: {
        ok: false,
        input_rows: 0,
        headers: [],
        quality_gate_ok: false,
        quality_gate_error: error,
        issues: [error],
        suggestions: ["请先添加输入文件，再执行模板预检。"],
      },
    };
  }
  const precheck = OFFLINE_INGEST.precheckRows(rawRows, params, runtime);
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
  if (payload?.glue_url && !params.glue_sidecar_url) params.glue_sidecar_url = payload.glue_url;
  const warnings = [];
  const runtime = {};
  let rawRows = [];
  try {
    rawRows = await readInputRows(params, warnings, runtime);
  } catch (e) {
    return {
      ok: false,
      mode: "offline_local",
      template: String(params.cleaning_template || "default").trim().toLowerCase() || "default",
      warnings,
      error: String(e && e.message ? e.message : e),
    };
  }
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

async function runOfflineCleaning(payload) {
  const t0 = Date.now();
  const params = resolveCleaningTemplateParams(payload?.params || {});
  if (payload?.glue_url && !params.glue_sidecar_url) params.glue_sidecar_url = payload.glue_url;
  const reportTitle = normalizeReportTitle(params.report_title || payload?.report_title || "", "辩论资料库");
  let mdOnly = params.md_only === true || String(params.output_format || "").toLowerCase() === "md";

  const jobId = makeJobId();
  const outRoot = payload?.output_root || path.join(process.cwd(), "offline-jobs");
  const jobRoot = path.join(outRoot, jobId);
  const artDir = path.join(jobRoot, "artifacts");
  const paperMdDir = path.join(artDir, "paper_markdown");
  ensureDir(artDir);

  const warnings = [];
  const glueFirstResult = await tryRunGlueFirstCleaning({ payload, params, outRoot, jobId, warnings });
  if (glueFirstResult && glueFirstResult.ok) {
    glueFirstResult.run = glueFirstResult.run || {
      ok: true,
      seconds: Math.round(((Date.now() - t0) / 1000) * 1000) / 1000,
    };
    return glueFirstResult;
  }

  const runtime = { paperMdDir, paperMdRecords: [], fileQualityRecords: [] };
  const rawRows = await readInputRows(params, warnings, runtime);
  const cleaned = cleanRows(rawRows, params);
  let rows = cleaned.rows;
  let quality = cleaned.quality;
  const execution = {
    execution_mode: String(cleaned?.execution_mode || ""),
    execution_audit: cleaned?.execution_audit && typeof cleaned.execution_audit === "object" ? cleaned.execution_audit : {},
    eligibility_reason: String(cleaned?.eligibility_reason || ""),
    requested_rust_v2_mode: String(cleaned?.requested_rust_v2_mode || ""),
    effective_rust_v2_mode: String(cleaned?.effective_rust_v2_mode || ""),
    verify_on_default: !!cleaned?.verify_on_default,
    shadow_compare: cleaned?.shadow_compare && typeof cleaned.shadow_compare === "object"
      ? cleaned.shadow_compare
      : {
          status: "skipped",
          matched: false,
          mismatch_count: 0,
          mismatches: [],
          skipped_reason: "offline_engine_no_compare",
          compare_fields: ["rows", "quality", "reason_counts"],
        },
  };
  let qualityGate = { evaluated: false, passed: false };
  const fidelity = { enabled: false, reasons: [] };

  const evalTextRows = (rows || [])
    .map((row) => rowTextForQuality(row))
    .filter((text) => String(text || "").length >= 4);
  const corruptRows = evalTextRows.filter((text) => isLikelyCorruptedText(text)).length;
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

  const qualityScore = computeOfficeQualityScore(rows, quality, warnings, { md_only: mdOnly });
  const contentQuality = assessContentQuality(rows, params);
  let filteredNoiseWritten = false;
  const outputGateMeta = {
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
    outputGateMeta,
  });

  if (!mdOnly) {
    try {
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
    } catch (e) {
      if (!isMissingNodeModuleError(e)) throw e;
      mdOnly = true;
      outputGateMeta.office_dependency_fallback = true;
      warnings.push(`Office 生成依赖缺失，已回退为仅输出 Markdown: ${String(e.message || e)}`);
    }
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
    execution,
    quality_score: qualityScore,
    quality_gate: qualityGate,
    warnings,
    artifacts,
    artifacts_dir: artDir,
  };
}

module.exports = { runOfflineCleaning, runOfflinePrecheck, runOfflineDebatePreview, listCleaningTemplates };
