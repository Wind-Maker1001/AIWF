const fs = require("fs");
const path = require("path");
const ExcelJS = require("exceljs");
const { Document, Packer, Paragraph, HeadingLevel, Table, TableRow, TableCell, TextRun, WidthType } = require("docx");
const PptxGenJS = require("pptxgenjs");
const {
  normalizeLineText,
  isLikelyCorruptedText,
  looksLikeReferenceEntry,
} = require("./offline_paper");

function createOfflineOutputs({ resolveOfficeTheme, resolveOfficeFont, resolveOfficeLayout }) {
  function unionColumns(rows) {
    const pref = ["source_file", "source_type", "row_no", "id", "text", "amount"];
    const set = new Set();
    rows.forEach((r) => Object.keys(r).forEach((k) => set.add(k)));
    const tail = Array.from(set).filter((k) => !pref.includes(k)).sort();
    return [...pref.filter((k) => set.has(k)), ...tail];
  }

  function buildDataQualityInsights(rows) {
    const insights = [];
    if (!Array.isArray(rows) || rows.length === 0) return insights;
    const keys = unionColumns(rows);

    const dateKeys = keys.filter((k) => /(date|时间|日期)/i.test(k));
    for (const k of dateKeys) {
      let slash = 0;
      let dash = 0;
      for (const r of rows.slice(0, 500)) {
        const v = String(r[k] || "").trim();
        if (!v) continue;
        if (/\d{4}[/-]\d{1,2}[/-]\d{1,2}/.test(v)) {
          if (v.includes("/")) slash += 1;
          if (v.includes("-")) dash += 1;
        }
      }
      if (slash > 0 && dash > 0) insights.push(`日期列 "${k}" 存在多种格式（/ 与 - 混用）。`);
    }

    const trusted = rows
      .filter((r) => !isLikelyCorruptedText(String(r.text || "")))
      .slice(0, 1500);
    const sampleText = trusted.map((r) => Object.values(r).join(" ")).join("\n");
    const cnyCount = (sampleText.match(/¥|￥|人民币|CNY/gi) || []).length;
    const usdCount = (sampleText.match(/\$|USD|美元/gi) || []).length;
    if (cnyCount >= 8 && usdCount >= 8) insights.push("检测到人民币与美元符号混用，建议统一币种口径。");

    const sparse = [];
    for (const k of keys) {
      let empty = 0;
      for (const r of rows) {
        if (String(r[k] || "").trim() === "") empty += 1;
      }
      if (empty / rows.length >= 0.7) sparse.push(k);
    }
    if (sparse.length > 0) insights.push(`高缺失列（>=70%）: ${sparse.slice(0, 6).join("、")}`);

    return insights;
  }

  function buildEvidenceHighlights(rows, maxItems = 8) {
    const out = [];
    const seen = new Set();
    const perSourceCap = Math.max(1, Math.floor(maxItems / 4));
    const sourceCount = new Map();
    for (const r of rows || []) {
      const sourceType = String(r.source_type || "");
      if (!["pdf", "pdf_ocr", "docx", "txt", "image_ocr", "pdf_md", "pdf_ocr_md", "docx_md", "pdf_md_fallback"].includes(sourceType)) continue;
      const text = normalizeLineText(r.text || "");
      if (text.length < 40) continue;
      if (isLikelyCorruptedText(text)) continue;
      if (looksLikeReferenceEntry(text)) continue;
      const key = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
      if (!key || key.length < 20 || seen.has(key)) continue;
      const src = path.basename(String(r.source_file || ""));
      const cnt = sourceCount.get(src) || 0;
      if (cnt >= perSourceCap) continue;
      seen.add(key);
      out.push({
        source: src,
        text: text.length > 180 ? `${text.slice(0, 180)}...` : text,
      });
      sourceCount.set(src, cnt + 1);
      if (out.length >= maxItems) break;
    }
    if (out.length < maxItems) {
      for (const r of rows || []) {
        const sourceType = String(r.source_type || "");
        if (!["pdf", "pdf_ocr", "docx", "txt", "image_ocr", "pdf_md", "pdf_ocr_md", "docx_md", "pdf_md_fallback"].includes(sourceType)) continue;
        const text = normalizeLineText(r.text || "");
        if (text.length < 40 || isLikelyCorruptedText(text) || looksLikeReferenceEntry(text)) continue;
        const key = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!key || key.length < 20 || seen.has(key)) continue;
        seen.add(key);
        out.push({
          source: path.basename(String(r.source_file || "")),
          text: text.length > 180 ? `${text.slice(0, 180)}...` : text,
        });
        if (out.length >= maxItems) break;
      }
    }
    return out;
  }

  async function writeXlsx(filePath, rows, quality, warnings, options = {}) {
    const theme = resolveOfficeTheme(options.office_theme);
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontName = resolveOfficeFont(options, warnings);
    const wb = new ExcelJS.Workbook();
    const ws = wb.addWorksheet(String(layout.xlsx_data_sheet_name || "cleaned"));
    const keys = rows.length > 0 ? unionColumns(rows) : ["id", "amount", "text"];
    ws.columns = keys.map((k) => ({ header: k, key: k, width: 26 }));
    ws.views = [{ state: "frozen", ySplit: 1 }];
    ws.autoFilter = {
      from: { row: 1, column: 1 },
      to: { row: 1, column: Math.max(1, keys.length) },
    };
    rows.forEach((r) => ws.addRow(r));
    ws.getRow(1).eachCell((cell) => {
      cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${theme.primary}` } };
    });
    if (highQuality) {
      for (let i = 2; i <= ws.rowCount; i += 1) {
        if (i % 2 === 0) {
          ws.getRow(i).eachCell((cell) => {
            cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: "FFF7FAFD" } };
          });
        }
      }
    }
    for (let i = 2; i <= ws.rowCount; i += 1) {
      ws.getRow(i).eachCell((cell) => {
        const prev = cell.font || {};
        cell.font = { ...prev, name: fontName };
        cell.alignment = { vertical: "top", horizontal: "left", wrapText: true };
        cell.border = {
          top: { style: "thin", color: { argb: "FFE6EDF5" } },
          left: { style: "thin", color: { argb: "FFE6EDF5" } },
          bottom: { style: "thin", color: { argb: "FFE6EDF5" } },
          right: { style: "thin", color: { argb: "FFE6EDF5" } },
        };
      });
    }

    const summary = wb.addWorksheet(String(layout.xlsx_summary_sheet_name || "summary"));
    summary.addRow(["指标", "数值"]);
    summary.getRow(1).eachCell((cell) => {
      cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${theme.primary}` } };
    });
    summary.addRow(["主题", theme.title]);
    summary.addRow(["质量模式", highQuality ? "high" : "standard"]);
    summary.addRow(["输入行", quality.input_rows]);
    summary.addRow(["输出行", quality.output_rows]);
    summary.addRow(["过滤行", quality.filtered_rows]);
    summary.addRow(["无效行", quality.invalid_rows]);
    summary.addRow(["去重移除", quality.duplicate_rows_removed]);
    summary.columns = [{ width: 22 }, { width: 24 }];
    for (let i = 2; i <= summary.rowCount; i += 1) {
      summary.getRow(i).eachCell((cell) => {
        cell.font = { name: fontName };
      });
    }

    if (warnings.length > 0) {
      const warn = wb.addWorksheet("warnings");
      warn.addRow(["告警"]);
      warn.getRow(1).eachCell((cell) => {
        cell.font = { name: fontName, bold: true };
      });
      warnings.forEach((w) => warn.addRow([w]));
      for (let i = 2; i <= warn.rowCount; i += 1) {
        warn.getRow(i).eachCell((cell) => {
          cell.font = { name: fontName };
        });
      }
      warn.columns = [{ width: 120 }];
    }

    await wb.xlsx.writeFile(filePath);
  }

  function makeTableRowsForDocx(rows, cols, fontName, maxRows = 20) {
    const head = new TableRow({
      children: cols.map((k) => new TableCell({
        children: [new Paragraph({ children: [new TextRun({ text: k, bold: true, font: fontName })] })],
      })),
    });
    const body = rows.slice(0, maxRows).map((r) => new TableRow({
      children: cols.map((k) => new TableCell({
        children: [new Paragraph({ children: [new TextRun({ text: String(r[k] ?? ""), font: fontName })] })],
      })),
    }));
    return [head, ...body];
  }

  async function writeDocx(filePath, jobId, reportTitle, rows, quality, warnings, options = {}) {
    const theme = resolveOfficeTheme(options.office_theme);
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontName = resolveOfficeFont(options, warnings);
    const insights = buildDataQualityInsights(rows);
    const highlights = buildEvidenceHighlights(rows, 10);
    const cols = rows.length > 0 ? unionColumns(rows) : ["id", "amount", "text"];
    const docxMaxRows = Math.max(8, Number(layout.docx_max_table_rows || 20));
    const table = new Table({
      width: { size: 100, type: WidthType.PERCENTAGE },
      rows: makeTableRowsForDocx(rows, cols, fontName, docxMaxRows),
    });

    const children = [
      new Paragraph({
        children: [new TextRun({ text: reportTitle || "离线清洗报告", bold: true, color: theme.primary, size: 34, font: fontName })],
        heading: HeadingLevel.HEADING_1,
      }),
      new Paragraph({ children: [new TextRun({ text: `主题: ${theme.title}   质量模式: ${highQuality ? "high" : "standard"}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `任务ID: ${jobId}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `输入行: ${quality.input_rows}，输出行: ${quality.output_rows}`, font: fontName })] }),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "核心发现", font: fontName, bold: true })] }),
      ...((insights.length ? insights : ["数据清洗过程完成，当前未发现显著结构冲突。"])
        .slice(0, 6)
        .map((x) => new Paragraph({ children: [new TextRun({ text: `- ${x}`, font: fontName })] }))),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "证据摘录", font: fontName, bold: true })] }),
      ...((highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
        .slice(0, 10)
        .map((x) => new Paragraph({ children: [new TextRun({ text: `- [${x.source}] ${x.text}`, font: fontName })] }))),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "样例数据（前 20 行）", font: fontName, bold: true })] }),
      table,
    ];

    if (warnings.length > 0) {
      children.push(new Paragraph(""));
      children.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "告警", font: fontName, bold: true })] }));
      warnings.slice(0, 30).forEach((w) => children.push(new Paragraph({ children: [new TextRun({ text: `- ${w}`, font: fontName })] })));
    }

    const doc = new Document({ sections: [{ children }] });
    const buf = await Packer.toBuffer(doc);
    fs.writeFileSync(filePath, buf);
  }

  async function writePptx(filePath, reportTitle, rows, quality, warnings, options = {}) {
    const theme = resolveOfficeTheme(options.office_theme);
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontFace = resolveOfficeFont(options, warnings);
    const insights = buildDataQualityInsights(rows);
    const maxFindings = Math.max(3, Number(layout.pptx_max_findings || 6));
    const maxEvidence = Math.max(3, Number(layout.pptx_max_evidence || 6));
    const highlights = buildEvidenceHighlights(rows, maxEvidence);
    const pptx = new PptxGenJS();
    pptx.layout = "LAYOUT_WIDE";

    const s1 = pptx.addSlide();
    s1.background = { color: theme.bg };
    s1.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: 13.33, h: 0.2, fill: { color: theme.primary }, line: { color: theme.primary } });
    s1.addText(reportTitle || "离线清洗简报", { x: 0.4, y: 0.3, w: 11.8, h: 0.7, fontSize: highQuality ? 34 : 30, bold: true, color: theme.primary, fontFace });
    s1.addText(`离线模式：无网络可用  |  主题：${theme.title}`, { x: 0.5, y: 1.1, w: 10, h: 0.5, fontSize: 16, color: theme.secondary, fontFace });
    s1.addShape(pptx.ShapeType.roundRect, { x: 0.5, y: 1.8, w: 12.2, h: 4.7, fill: { color: theme.bg }, line: { color: theme.secondary } });
    s1.addText(`输入行: ${quality.input_rows}    输出行: ${quality.output_rows}    去重移除: ${quality.duplicate_rows_removed}`, { x: 0.8, y: 2.2, w: 11, h: 0.7, fontSize: 20, fontFace });

    const s2 = pptx.addSlide();
    s2.background = { color: theme.bg };
    s2.addText("质量摘要", { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 24, bold: true, fontFace });
    s2.addTable([
      [{ text: "指标", options: { bold: true } }, { text: "数值", options: { bold: true } }],
      ["输入行", String(quality.input_rows)],
      ["输出行", String(quality.output_rows)],
      ["过滤行", String(quality.filtered_rows)],
      ["无效行", String(quality.invalid_rows)],
      ["去重移除", String(quality.duplicate_rows_removed)],
    ], { x: 0.6, y: 1.1, w: 6.4, h: 3.4, border: { pt: 1, color: "DDE7F2" }, fontFace });

    const s3 = pptx.addSlide();
    s3.background = { color: theme.bg };
    s3.addText("样例数据（前 8 行）", { x: 0.5, y: 0.4, w: 6, h: 0.5, fontSize: 22, bold: true, fontFace });
    const cols = rows.length > 0 ? unionColumns(rows).slice(0, 4) : ["id", "text", "amount"];
    const tableRows = [[...cols]];
    const sampleRows = Math.max(4, Number(layout.pptx_sample_rows || 8));
    rows.slice(0, sampleRows).forEach((r) => tableRows.push(cols.map((c) => String(r[c] ?? ""))));
    s3.addTable(tableRows, { x: 0.5, y: 1.0, w: 12.2, h: 4.9, border: { pt: 1, color: "DDE7F2" }, fontFace });

    const s4 = pptx.addSlide();
    s4.background = { color: theme.bg };
    s4.addText("关键发现", { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 22, bold: true, fontFace });
    const findingText = (insights.length ? insights : ["暂无显著结构冲突，建议继续补充样本规模。"])
      .slice(0, maxFindings)
      .map((x) => `• ${x}`)
      .join("\n");
    s4.addText(findingText, { x: 0.6, y: 1.0, w: 7.6, h: 5.2, fontSize: 15, color: "1F2D3D", fontFace });
    const sourceCount = {};
    rows.forEach((r) => {
      const k = String(r.source_type || "unknown");
      sourceCount[k] = (sourceCount[k] || 0) + 1;
    });
    const top = Object.entries(sourceCount).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top.length > 0) {
      try {
        s4.addChart(pptx.ChartType.bar, [{
          name: "行数",
          labels: top.map((x) => x[0]),
          values: top.map((x) => x[1]),
        }], {
          x: 8.4, y: 1.0, w: 4.4, h: 4.2,
          barDir: "col",
          catAxisLabelRotate: -30,
          showLegend: false,
        });
      } catch {}
    }

    const sEvidence = pptx.addSlide();
    sEvidence.background = { color: theme.bg };
    sEvidence.addText("证据摘录", { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 22, bold: true, fontFace });
    const evText = (highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
      .slice(0, 6)
      .map((x) => `• [${x.source}] ${x.text}`)
      .join("\n");
    sEvidence.addText(evText, { x: 0.6, y: 1.0, w: 12.0, h: 5.9, fontSize: 14, color: "1F2D3D", fontFace });

    if (warnings.length > 0) {
      const sWarn = pptx.addSlide();
      sWarn.background = { color: theme.bg };
      sWarn.addText("告警", { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 22, bold: true, color: "B42318", fontFace });
      const content = warnings.slice(0, 12).map((w) => `• ${w}`).join("\n");
      sWarn.addText(content, { x: 0.6, y: 1.0, w: 12.0, h: 5.8, fontSize: 16, color: "1F2D3D", fontFace });
    }

    if (highQuality) {
      const s5 = pptx.addSlide();
      s5.background = { color: theme.bg };
      s5.addText("建议与行动", { x: 0.5, y: 0.4, w: 6, h: 0.5, fontSize: 24, bold: true, color: theme.primary, fontFace });
      s5.addText(
        "1) 优先排查无效行来源并修正录入规则\n2) 直接复用本报告中的指标表和样例表\n3) 下次作业沿用同一主题，保持视觉一致",
        { x: 0.7, y: 1.2, w: 12.0, h: 3.8, fontSize: 17, color: theme.secondary, fontFace }
      );
    }

    await pptx.writeFile({ fileName: filePath });
  }

  function groupRowsBySource(rows) {
    const m = new Map();
    for (const r of rows || []) {
      const src = path.basename(String(r.source_file || "unknown"));
      if (!m.has(src)) m.set(src, []);
      m.get(src).push(r);
    }
    return m;
  }

  async function writeMarkdown(filePath, jobId, reportTitle, rows, quality, warnings) {
    const highlights = buildEvidenceHighlights(rows, 24);
    const grouped = groupRowsBySource(rows);
    const lines = [];
    lines.push(`# ${reportTitle || "离线作业成品（Markdown）"}`);
    lines.push("");
    lines.push(`- 任务ID: \`${jobId}\``);
    lines.push(`- 输入行: ${quality.input_rows}`);
    lines.push(`- 输出行: ${quality.output_rows}`);
    lines.push(`- 去重移除: ${quality.duplicate_rows_removed}`);
    lines.push("");
    lines.push("## 关键发现");
    const insights = buildDataQualityInsights(rows);
    (insights.length ? insights : ["当前未检测到显著结构冲突。"]).slice(0, 10).forEach((x) => lines.push(`- ${x}`));
    lines.push("");
    lines.push("## 证据摘录");
    (highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
      .forEach((x, i) => lines.push(`${i + 1}. [${x.source}] ${x.text}`));
    lines.push("");
    lines.push("## 分文件提取摘要");
    for (const [src, items] of grouped.entries()) {
      lines.push(`### ${src}`);
      lines.push(`- 提取片段数: ${items.length}`);
      const snippets = items
        .map((r) => normalizeLineText(r.text || ""))
        .filter((t) => t.length >= 30)
        .slice(0, 8);
      if (snippets.length === 0) {
        lines.push("- （无有效文本片段）");
      } else {
        snippets.forEach((t) => lines.push(`- ${t.length > 220 ? `${t.slice(0, 220)}...` : t}`));
      }
      lines.push("");
    }
    if (warnings && warnings.length) {
      lines.push("## 告警");
      warnings.slice(0, 30).forEach((w) => lines.push(`- ${w}`));
      lines.push("");
    }
    fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
  }

  return {
    buildDataQualityInsights,
    buildEvidenceHighlights,
    unionColumns,
    writeXlsx,
    writeDocx,
    writePptx,
    writeMarkdown,
  };
}

module.exports = {
  createOfflineOutputs,
};
