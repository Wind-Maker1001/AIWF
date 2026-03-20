const path = require("path");
const {
  normalizeLineText,
  isLikelyCorruptedText,
  looksLikeReferenceEntry,
  rowTextForQuality,
} = require("./offline_paper");
const { createOfflineOutputAnalysis } = require("./offline_outputs_analysis");
const { createOfflineOutputMarkdown } = require("./offline_outputs_markdown");
const { createOfflineOutputSupport } = require("./offline_outputs_support");
const { createOfflineOutputXlsx } = require("./offline_outputs_xlsx");
const { createOfflineOutputDocx } = require("./offline_outputs_docx");
const { createOfflineOutputPptx } = require("./offline_outputs_pptx");

function createLazyModuleLoader(moduleName) {
  let cached = null;
  let loadError = null;
  return function loadModule() {
    if (cached) return cached;
    if (loadError) throw loadError;
    try {
      cached = require(moduleName);
      return cached;
    } catch (e) {
      loadError = e;
      throw e;
    }
  };
}

const getExcelJSModule = createLazyModuleLoader("exceljs");
const getDocxModule = createLazyModuleLoader("docx");
const getPptxGenJSModule = createLazyModuleLoader("pptxgenjs");

function createOfflineOutputs({ resolveOfficeTheme, resolveOfficeFont, resolveOfficeLayout }) {
  const {
    isQuestionMarkHeavy,
    filterRowsForOffice,
    assessContentQuality,
    computeOfficeQualityScore,
    readImageSize,
    cleanOfficeText,
    toBulletList,
    paginateBullets,
    fitRect,
    fitTableBodyRowCount,
    resolveVisualPack,
    pickIllustrationImage,
    resolveBundledFluentChartImage,
    unionColumns,
    isDebateStyleTemplate,
    buildDebateTableRows,
    groupDebateRowsByBattlefield,
    buildDebatePreview,
    buildDataQualityInsights,
    buildEvidenceHighlights,
  } = createOfflineOutputAnalysis({
    normalizeLineText,
    isLikelyCorruptedText,
    looksLikeReferenceEntry,
    rowTextForQuality,
    pathModule: path,
  });

  const {
    writeMarkdown,
    writeFilteredNoiseMarkdown,
  } = createOfflineOutputMarkdown({
    normalizeLineText,
    rowTextForQuality,
    pathModule: path,
    buildDataQualityInsights,
    buildEvidenceHighlights,
  });

  const {
    buildNumericColumnStats,
    buildNarrativeBullets,
  } = createOfflineOutputSupport({
    unionColumns,
    cleanOfficeText,
    looksLikeReferenceEntry,
    isLikelyCorruptedText,
    isQuestionMarkHeavy,
    buildEvidenceHighlights,
  });

  const { writeXlsx } = createOfflineOutputXlsx({
    getExcelJSModule,
    resolveOfficeTheme,
    resolveOfficeFont,
    resolveOfficeLayout,
    resolveVisualPack,
    isDebateStyleTemplate,
    buildDebateTableRows,
    unionColumns,
    readImageSize,
    cleanOfficeText,
    assessContentQuality,
    computeOfficeQualityScore,
    buildDataQualityInsights,
    buildEvidenceHighlights,
    buildNumericColumnStats,
    resolveBundledFluentChartImage,
  });

  const { writeDocx } = createOfflineOutputDocx({
    getDocxModule,
    resolveOfficeTheme,
    resolveOfficeFont,
    resolveOfficeLayout,
    resolveVisualPack,
    buildDataQualityInsights,
    buildEvidenceHighlights,
    buildNarrativeBullets,
    pickIllustrationImage,
    unionColumns,
    isDebateStyleTemplate,
    buildDebateTableRows,
    cleanOfficeText,
  });

  const { writePptx } = createOfflineOutputPptx({
    getPptxGenJSModule,
    resolveOfficeTheme,
    resolveOfficeFont,
    resolveOfficeLayout,
    resolveVisualPack,
    isDebateStyleTemplate,
    buildDebateTableRows,
    groupDebateRowsByBattlefield,
    buildDataQualityInsights,
    buildEvidenceHighlights,
    buildNarrativeBullets,
    buildNumericColumnStats,
    pickIllustrationImage,
    unionColumns,
    cleanOfficeText,
    fitRect,
    fitTableBodyRowCount,
    readImageSize,
    toBulletList,
    paginateBullets,
  });

  return {
    isQuestionMarkHeavy,
    filterRowsForOffice,
    buildDataQualityInsights,
    buildEvidenceHighlights,
    computeOfficeQualityScore,
    assessContentQuality,
    unionColumns,
    buildDebatePreview,
    writeXlsx,
    writeDocx,
    writePptx,
    writeMarkdown,
    writeFilteredNoiseMarkdown,
  };
}

module.exports = {
  createOfflineOutputs,
};
