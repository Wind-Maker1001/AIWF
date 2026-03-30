function createOfflineIngestParsers(deps = {}) {
  const {
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
    IMG_EXT,
    splitTextToChunks,
    rowFromText,
    toRowsFromWorksheet,
    fetch,
    glueSidecarUrl,
  } = deps;

  function resolveGlueSidecarUrl(params = {}) {
    const fromParams = String(params.glue_sidecar_url || params.glueUrl || "").trim();
    if (fromParams) return fromParams.replace(/\/$/, "");
    if (typeof glueSidecarUrl === "function") {
      const resolved = String(glueSidecarUrl(params) || "").trim();
      if (resolved) return resolved.replace(/\/$/, "");
    } else if (typeof glueSidecarUrl === "string" && String(glueSidecarUrl).trim()) {
      return String(glueSidecarUrl).trim().replace(/\/$/, "");
    }
    const env = String(process.env.AIWF_GLUE_URL || process.env.AIWF_GLUE_SIDECAR_URL || "").trim();
    return env ? env.replace(/\/$/, "") : "";
  }

  async function extractViaGlueSidecar(filePath, warnings, params = {}) {
    const ext = path.extname(filePath).toLowerCase();
    if (!(ext === ".xlsx" || IMG_EXT.has(ext))) return null;
    if (typeof fetch !== "function") return null;
    const base = resolveGlueSidecarUrl(params);
    if (!base) return null;

    const body = {
      input_path: filePath,
      ocr_enabled: params.ocr_enabled !== false,
      ocr_lang: params.ocr_lang || "chi_sim+eng",
      ocr_config: params.ocr_config || "--oem 1 --psm 6",
      ocr_preprocess: params.ocr_preprocess || "adaptive",
      xlsx_all_sheets: params.xlsx_all_sheets !== false,
      include_hidden_sheets: params.include_hidden_sheets === true,
      sheet_allowlist: Array.isArray(params.sheet_allowlist) ? params.sheet_allowlist : [],
      quality_rules: params.quality_rules && typeof params.quality_rules === "object" ? params.quality_rules : {},
      image_rules: params.image_rules && typeof params.image_rules === "object" ? params.image_rules : {},
      xlsx_rules: params.xlsx_rules && typeof params.xlsx_rules === "object" ? params.xlsx_rules : {},
      sheet_profiles: params.sheet_profiles && typeof params.sheet_profiles === "object" ? params.sheet_profiles : {},
      on_file_error: "raise",
    };

    try {
      const resp = await fetch(`${base}/ingest/extract`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });
      const payload = await resp.json().catch(() => ({}));
      if (!resp.ok || !payload || payload.ok === false) {
        const err = String((payload && payload.error) || `http ${resp.status}`);
        warnings.push(`glue sidecar 提取失败，已回退本地解析: ${path.basename(filePath)} (${err})`);
        return null;
      }
      if (payload.quality_blocked) {
        const blocked = Array.isArray(payload.blocked_inputs) ? payload.blocked_inputs : [];
        const firstError = blocked.length > 0 ? String(blocked[0].error || "quality blocked") : "quality blocked";
        warnings.push(`输入质量门禁阻断: ${path.basename(filePath)} (${firstError})`);
        return { rows: [], blocked: true, meta: payload };
      }
      return { rows: Array.isArray(payload.rows) ? payload.rows : [], blocked: false, meta: payload };
    } catch (error) {
      warnings.push(`glue sidecar 不可用，已回退本地解析: ${path.basename(filePath)} (${String(error && error.message ? error.message : error)})`);
      return null;
    }
  }

  async function parseCsvOrXlsxFile(filePath, warnings, params = {}) {
    if (path.extname(filePath).toLowerCase() === ".xlsx") {
      const sidecar = await extractViaGlueSidecar(filePath, warnings, params);
      if (sidecar) return sidecar.rows;
    }
    const wb = new ExcelJS.Workbook();
    if (path.extname(filePath).toLowerCase() === ".csv") {
      const csvText = readTextFileSmart(filePath);
      await wb.csv.read(Readable.from([csvText]));
    } else {
      await wb.xlsx.readFile(filePath);
    }
    if (path.extname(filePath).toLowerCase() === ".csv") {
      const ws = wb.worksheets[0];
      if (!ws) {
        warnings.push(`表格无可读工作表: ${filePath}`);
        return [];
      }
      return toRowsFromWorksheet(ws, filePath);
    }
    const allSheets = params.xlsx_all_sheets !== false;
    const sheets = allSheets ? wb.worksheets : wb.worksheets.slice(0, 1);
    if (!sheets || sheets.length === 0) {
      warnings.push(`表格无可读工作表: ${filePath}`);
      return [];
    }
    const rows = [];
    for (const ws of sheets) {
      rows.push(...toRowsFromWorksheet(ws, filePath));
    }
    return rows;
  }

  function parseTxtFile(filePath) {
    const txt = readTextFileSmart(filePath);
    return splitTextToChunks(txt).map((s, i) => rowFromText(filePath, "txt", s, i));
  }

  async function parseDocxFile(filePath, params = {}, runtime = {}) {
    const res = await mammoth.extractRawText({ path: filePath });
    const chunks = splitTextToChunks(res.value);
    const mdChunks = materializePaperMarkdown(filePath, "docx", chunks, runtime, params);
    return mdChunks.map((s, i) => rowFromText(filePath, "docx_md", s, i));
  }

  async function parsePdfFile(filePath, warnings, params = {}, runtime = {}) {
    const pdfText = await extractPdfTextFromBuffer(fs.readFileSync(filePath));
    const chunks = splitPdfTextToEvidence(pdfText);
    const enoughText = chunks.length >= 3 || String(pdfText || "").trim().length >= 120;
    const qualityScore = scorePdfExtractText(pdfText);
    const corrupted = isLikelyCorruptedText(pdfText);
    const lowQuality = qualityScore < 8 || corrupted;
    if (Array.isArray(runtime.fileQualityRecords)) {
      runtime.fileQualityRecords.push({
        source_file: filePath,
        source_type: "pdf_text_layer",
        quality_score: Math.round(qualityScore * 1000) / 1000,
        text_chars: String(pdfText || "").length,
        chunks: chunks.length,
        corrupted,
      });
    }
    if (enoughText && !lowQuality) {
      const mdChunks = materializePaperMarkdown(filePath, "pdf", chunks, runtime, params);
      return mdChunks.map((s, i) => rowFromText(filePath, "pdf_md", s, i));
    }

    const ocr = runPdfOcr(filePath, params);
    if (ocr.ok) {
      if (corrupted) warnings.push(`PDF 文本层疑似乱码，已切换 OCR: ${path.basename(filePath)}`);
      else warnings.push(`PDF 文本层质量较低，已自动切换 OCR: ${path.basename(filePath)}`);

      const ocrChunks = splitPdfTextToEvidence(ocr.text);
      if (Array.isArray(runtime.fileQualityRecords)) {
        runtime.fileQualityRecords.push({
          source_file: filePath,
          source_type: "pdf_ocr_layer",
          quality_score: Math.round(scorePdfExtractText(ocr.text) * 1000) / 1000,
          text_chars: String(ocr.text || "").length,
          chunks: ocrChunks.length,
          corrupted: isLikelyCorruptedText(ocr.text),
        });
      }
      const mdChunks = materializePaperMarkdown(filePath, "pdf_ocr", ocrChunks, runtime, params);
      return mdChunks.map((s, i) => rowFromText(filePath, "pdf_ocr_md", s, i));
    }

    warnings.push(`PDF 文本提取质量较低且 OCR 未完成(${ocr.reason || "unknown"})，可能漏读: ${path.basename(filePath)}`);
    const mdChunks = materializePaperMarkdown(filePath, "pdf_fallback", chunks, runtime, params);
    return mdChunks.map((s, i) => rowFromText(filePath, "pdf_md_fallback", s, i));
  }

  function parseImageFile(filePath, warnings, params = {}) {
    const sidecarPromise = extractViaGlueSidecar(filePath, warnings, params);
    return Promise.resolve(sidecarPromise).then((sidecar) => {
      if (sidecar) return sidecar.rows;
      if (params.ocr_enabled === false) {
        warnings.push(`图片 OCR 已关闭，已跳过: ${path.basename(filePath)}`);
        return [];
      }

      const ocr = runImageOcr(filePath, params);
      if (ocr.ok) {
        const chunks = splitTextToChunks(ocr.text);
        if (chunks.length > 0) return chunks.map((s, i) => rowFromText(filePath, "image_ocr", s, i));
        warnings.push(`图片 OCR 结果为空，已阻断: ${path.basename(filePath)}`);
        return [];
      } else if (ocr.reason === "tesseract_missing") {
        warnings.push(`未检测到 Tesseract，图片已跳过: ${path.basename(filePath)}`);
      } else {
        warnings.push(`图片 OCR 失败，图片已跳过: ${path.basename(filePath)}`);
      }
      return [];
    });
  }

  async function parseOneFile(filePath, warnings, params = {}, runtime = {}) {
    const ext = path.extname(filePath).toLowerCase();
    if (!fs.existsSync(filePath)) {
      warnings.push(`文件不存在，已跳过: ${filePath}`);
      return [];
    }
    if (ext === ".csv" || ext === ".xlsx") return parseCsvOrXlsxFile(filePath, warnings, params);
    if (ext === ".txt") return parseTxtFile(filePath);
    if (ext === ".docx") return parseDocxFile(filePath, params, runtime);
    if (ext === ".pdf") return parsePdfFile(filePath, warnings, params, runtime);
    if (IMG_EXT.has(ext)) return await parseImageFile(filePath, warnings, params);

    warnings.push(`不支持的文件类型，已跳过: ${filePath}`);
    return [];
  }

  return {
    parseOneFile,
  };
}

module.exports = {
  createOfflineIngestParsers,
};
