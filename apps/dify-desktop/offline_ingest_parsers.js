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
  } = deps;

  async function parseCsvOrXlsxFile(filePath, warnings) {
    const wb = new ExcelJS.Workbook();
    if (path.extname(filePath).toLowerCase() === ".csv") {
      const csvText = readTextFileSmart(filePath);
      await wb.csv.read(Readable.from([csvText]));
    } else {
      await wb.xlsx.readFile(filePath);
    }
    const ws = wb.worksheets[0];
    if (!ws) {
      warnings.push(`表格无可读工作表: ${filePath}`);
      return [];
    }
    return toRowsFromWorksheet(ws, filePath);
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
      if (corrupted) warnings.push(`PDF 文本层疑似乱码，已强制切换 OCR: ${path.basename(filePath)}`);
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

    warnings.push(`PDF 文本提取质量较低，OCR 未完成(${ocr.reason || "unknown"})，可能漏读: ${path.basename(filePath)}`);
    const mdChunks = materializePaperMarkdown(filePath, "pdf_fallback", chunks, runtime, params);
    return mdChunks.map((s, i) => rowFromText(filePath, "pdf_md_fallback", s, i));
  }

  function parseImageFile(filePath, warnings, params = {}) {
    if (params.ocr_enabled === false) {
      warnings.push(`图片 OCR 已关闭，按文件信息入库: ${path.basename(filePath)}`);
      return [
        { source_file: filePath, source_type: "image", row_no: 1, text: `IMAGE:${path.basename(filePath)}`, amount: "" },
      ];
    }

    const ocr = runImageOcr(filePath, params);
    if (ocr.ok) {
      const chunks = splitTextToChunks(ocr.text);
      if (chunks.length > 0) return chunks.map((s, i) => rowFromText(filePath, "image_ocr", s, i));
      warnings.push(`图片 OCR 结果为空，按文件信息入库: ${path.basename(filePath)}`);
    } else if (ocr.reason === "tesseract_missing") {
      warnings.push(`未检测到 Tesseract，图片按文件信息入库: ${path.basename(filePath)}`);
    } else {
      warnings.push(`图片 OCR 失败，按文件信息入库: ${path.basename(filePath)}`);
    }

    let dim = "";
    try {
      const d = imageSize.imageSize(filePath);
      if (d && d.width && d.height) dim = `${d.width}x${d.height}`;
    } catch {}
    return [
      {
        source_file: filePath,
        source_type: "image",
        row_no: 1,
        text: `IMAGE:${path.basename(filePath)}${dim ? ` (${dim})` : ""}`,
        amount: "",
      },
    ];
  }

  async function parseOneFile(filePath, warnings, params = {}, runtime = {}) {
    const ext = path.extname(filePath).toLowerCase();
    if (!fs.existsSync(filePath)) {
      warnings.push(`文件不存在，已跳过: ${filePath}`);
      return [];
    }
    if (ext === ".csv" || ext === ".xlsx") return parseCsvOrXlsxFile(filePath, warnings);
    if (ext === ".txt") return parseTxtFile(filePath);
    if (ext === ".docx") return parseDocxFile(filePath, params, runtime);
    if (ext === ".pdf") return parsePdfFile(filePath, warnings, params, runtime);
    if (IMG_EXT.has(ext)) return parseImageFile(filePath, warnings, params);

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
