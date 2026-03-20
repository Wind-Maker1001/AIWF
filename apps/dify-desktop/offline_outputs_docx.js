const fs = require("fs");
const path = require("path");

function createOfflineOutputDocx({
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
}) {
  function makeTableRowsForDocx(rows, cols, fontName, maxRows = 20, headerFill = "EAF3FB") {
    const { Paragraph, TableRow, TableCell, TextRun } = getDocxModule();
    const head = new TableRow({
      children: cols.map((k) => new TableCell({
        shading: { fill: headerFill, color: "auto", type: "clear" },
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
    const { Document, Packer, Paragraph, HeadingLevel, Table, TableRow, TableCell, TextRun, WidthType, ImageRun } = getDocxModule();
    const theme = resolveOfficeTheme(options.office_theme);
    const themeName = String(options.office_theme || "").toLowerCase();
    const isStrong = /fluent_ms_strong/i.test(themeName);
    const isVibrant = /fluent_ms_vibrant/i.test(themeName);
    const visual = resolveVisualPack(theme, { isStrong, isVibrant });
    const docxGlassFill = (visual.panel_fill || "FFEAF3FB").slice(2);
    const docxStripFill = (visual.card_fill || "FFF2F8FF").slice(2);
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontName = resolveOfficeFont(options, warnings);
    const insights = buildDataQualityInsights(rows);
    const highlights = buildEvidenceHighlights(rows, 10);
    const narrativeBullets = buildNarrativeBullets(rows, 8);
    const docImage = pickIllustrationImage(rows, options);
    const cols = rows.length > 0 ? unionColumns(rows) : ["id", "amount", "text"];
    const gate = options.output_gate_meta && typeof options.output_gate_meta === "object" ? options.output_gate_meta : {};
    const src = Array.isArray(gate.source_files) ? gate.source_files.slice(0, 3).map((p) => path.basename(String(p || ""))).filter(Boolean).join(", ") : "";
    const docxMaxRows = Math.max(8, Number(layout.docx_max_table_rows || 20));
    if (isDebateStyleTemplate(options)) {
      const blocks = buildDebateTableRows(rows, options);
      const children = [
        new Paragraph({
          children: [new TextRun({ text: reportTitle || "辩论资料库", bold: true, color: theme.primary, size: 36, font: fontName })],
          heading: HeadingLevel.HEADING_1,
        }),
        new Paragraph({ children: [new TextRun({ text: `任务ID: ${jobId}`, font: fontName })] }),
        new Paragraph({ children: [new TextRun({ text: `输入行: ${quality.input_rows}，输出行: ${quality.output_rows}`, font: fontName })] }),
        new Paragraph({ children: [new TextRun({ text: `数据来源: ${src || "N/A"}`, font: fontName })] }),
        new Paragraph(""),
      ];
      blocks.slice(0, 80).forEach((b) => {
        children.push(new Paragraph({
          heading: HeadingLevel.HEADING_2,
          children: [new TextRun({ text: `【${b.battlefield}】`, font: fontName, bold: true, color: theme.primary })],
        }));
        children.push(new Paragraph({ children: [new TextRun({ text: `支持论点: ${b.support_claim}  |  证据类型: ${b.evidence_type}  |  相关性: ${b.relevance_score}`, font: fontName })] }));
        children.push(new Paragraph({ children: [new TextRun({ text: b.content, font: fontName })] }));
        children.push(new Paragraph({ children: [new TextRun({ text: `——${b.source_org}`, font: fontName })] }));
        if (b.source_url) children.push(new Paragraph({ children: [new TextRun({ text: b.source_url, font: fontName })] }));
      });
      if (warnings.length > 0) {
        children.push(new Paragraph(""));
        children.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "告警", font: fontName, bold: true })] }));
        warnings.slice(0, 20).forEach((w) => children.push(new Paragraph({ children: [new TextRun({ text: `- ${w}`, font: fontName })] })));
      }
      const docDebate = new Document({ sections: [{ children }] });
      const bufDebate = await Packer.toBuffer(docDebate);
      fs.writeFileSync(filePath, bufDebate);
      return;
    }
    const table = new Table({
      width: { size: 100, type: WidthType.PERCENTAGE },
      rows: makeTableRowsForDocx(rows, cols, fontName, docxMaxRows, isStrong ? "DDEBFB" : "EAF3FB"),
    });
    const qualityTable = new Table({
      width: { size: 100, type: WidthType.PERCENTAGE },
      rows: [
        new TableRow({
          children: [
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: "质量指标", bold: true, font: fontName })] })] }),
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: "数值", bold: true, font: fontName })] })] }),
          ],
        }),
        new TableRow({
          children: [
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: "输入行", font: fontName })] })] }),
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: String(quality.input_rows || 0), font: fontName })] })] }),
          ],
        }),
        new TableRow({
          children: [
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: "输出行", font: fontName })] })] }),
            new TableCell({ children: [new Paragraph({ children: [new TextRun({ text: String(quality.output_rows || 0), font: fontName })] })] }),
          ],
        }),
      ],
    });
    const glassCard = new Table({
      width: { size: 100, type: WidthType.PERCENTAGE },
      rows: [
        new TableRow({
          children: [
            new TableCell({
              shading: { fill: docxGlassFill, color: "auto", type: "clear" },
              children: [
                new Paragraph({
                  children: [new TextRun({ text: reportTitle || "离线清洗报告", bold: true, color: theme.primary, size: 36, font: fontName })],
                }),
                new Paragraph({
                  children: [new TextRun({ text: `Fluent 风格摘要卡  |  任务ID: ${jobId}  |  输入 ${quality.input_rows} / 输出 ${quality.output_rows}`, color: "4A5E74", font: fontName })],
                }),
              ],
            }),
          ],
        }),
      ],
    });
    const kpiStrip = new Table({
      width: { size: 100, type: WidthType.PERCENTAGE },
      rows: [
        new TableRow({
          children: [
            new TableCell({
              shading: { fill: docxStripFill, color: "auto", type: "clear" },
              children: [new Paragraph({ children: [new TextRun({ text: `输入行 ${Number(quality.input_rows || 0)}`, bold: true, font: fontName, color: theme.primary })] })],
            }),
            new TableCell({
              shading: { fill: docxStripFill, color: "auto", type: "clear" },
              children: [new Paragraph({ children: [new TextRun({ text: `输出行 ${Number(quality.output_rows || 0)}`, bold: true, font: fontName, color: theme.primary })] })],
            }),
            new TableCell({
              shading: { fill: docxStripFill, color: "auto", type: "clear" },
              children: [new Paragraph({ children: [new TextRun({ text: `去重移除 ${Number(quality.duplicate_rows_removed || 0)}`, bold: true, font: fontName, color: theme.primary })] })],
            }),
          ],
        }),
      ],
    });

    const children = [
      glassCard,
      new Paragraph(""),
      kpiStrip,
      new Paragraph(""),
      new Paragraph({ children: [new TextRun({ text: `主题: ${theme.title}   质量模式: ${highQuality ? "high" : "standard"}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `任务ID: ${jobId}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `输入行: ${quality.input_rows}，输出行: ${quality.output_rows}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `数据来源: ${src || "N/A"}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `预检风险: ${String(gate.preflight_risk_label || "-")} (${Number(gate.preflight_risk_score || 0)}/100)`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `质量分: ${Number(gate.quality_score || 0)}  invalid_ratio: ${Number(gate.invalid_ratio || 0)}  gibberish_ratio: ${Number(gate.gibberish_ratio || 0)}`, font: fontName })] }),
      new Paragraph({ children: [new TextRun({ text: `问号密集过滤行: ${Number(gate.filtered_question_mark_rows || 0)}`, font: fontName })] }),
      qualityTable,
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "摘要要点（适配外部AI提炼文本）", font: fontName, bold: true })] }),
      ...((narrativeBullets.length ? narrativeBullets : ["未检测到可直接复用的摘要字段，可继续使用证据摘录。"])
        .slice(0, 8)
        .map((x) => new Paragraph({ children: [new TextRun({ text: `- ${x}`, font: fontName })] }))),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "核心发现", font: fontName, bold: true })] }),
      ...((insights.length ? insights : ["数据清洗过程完成，当前未发现显著结构冲突。"])
        .slice(0, 6)
        .map((x) => new Paragraph({ children: [new TextRun({ text: `- ${x}`, font: fontName })] }))),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "证据摘录", font: fontName, bold: true })] }),
      ...((highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
        .slice(0, 10)
        .map((x) => new Paragraph({ children: [new TextRun({ text: `- [${cleanOfficeText(x.source, 60)}] ${cleanOfficeText(x.text, 180)}`, font: fontName })] }))),
      new Paragraph(""),
      new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "样例数据（前 20 行）", font: fontName, bold: true })] }),
      table,
    ];

    if (docImage) {
      try {
        const imgBuf = fs.readFileSync(docImage);
        const ext = path.extname(docImage).toLowerCase();
        const docImageType = ext === ".jpg" || ext === ".jpeg"
          ? "jpg"
          : ext === ".gif"
            ? "gif"
            : "png";
        children.push(new Paragraph(""));
        children.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "图示样本", font: fontName, bold: true })] }));
        children.push(new Paragraph({
          children: [
            new ImageRun({
              data: imgBuf,
              type: docImageType,
              transformation: { width: 520, height: 290 },
            }),
          ],
        }));
        children.push(new Paragraph({ children: [new TextRun({ text: `来源: ${path.basename(docImage)}`, font: fontName })] }));
      } catch {}
    }

    if (warnings.length > 0) {
      children.push(new Paragraph(""));
      children.push(new Paragraph({ heading: HeadingLevel.HEADING_2, children: [new TextRun({ text: "告警", font: fontName, bold: true })] }));
      warnings.slice(0, 30).forEach((w) => children.push(new Paragraph({ children: [new TextRun({ text: `- ${w}`, font: fontName })] })));
    }

    const doc = new Document({
      sections: [{
        properties: {
          page: {
            margin: { top: 900, right: 900, bottom: 900, left: 900 },
          },
        },
        children,
      }],
    });
    const buf = await Packer.toBuffer(doc);
    fs.writeFileSync(filePath, buf);
  }

  return {
    writeDocx,
  };
}

module.exports = {
  createOfflineOutputDocx,
};
