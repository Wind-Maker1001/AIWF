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
  const runtime = { paperMdDir, paperMdRecords: [], fileQualityRecords: [] };
  const rawRows = await readInputRows(params, warnings, runtime);
  const cleaned = cleanRows(rawRows, params);
  let rows = cleaned.rows;
  let quality = cleaned.quality;
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
    quality_score: qualityScore,
    quality_gate: qualityGate,
    warnings,
    artifacts,
  };
}

module.exports = { runOfflineCleaning, runOfflinePrecheck, runOfflineDebatePreview, listCleaningTemplates };
