const path = require("path");

function createOfflineOutputPptx({
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
}) {
  async function writePptx(filePath, reportTitle, rows, quality, warnings, options = {}) {
    const PptxGenJS = getPptxGenJSModule();
    const theme = resolveOfficeTheme(options.office_theme);
    const themeName = String(options.office_theme || "").toLowerCase();
    const isStrong = /fluent_ms_strong/i.test(themeName);
    const isVibrant = /fluent_ms_vibrant/i.test(themeName);
    const visual = resolveVisualPack(theme, { isStrong, isVibrant });
    const accentPrimary = visual.accent_primary;
    const accentSecondary = visual.accent_secondary;
    const chromeCardFill = visual.chrome_fill;
    const chromeCardTrans = visual.chrome_transparency;
    const chromeLine = visual.chrome_line;
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontFace = resolveOfficeFont(options, warnings);
    const gate = options.output_gate_meta && typeof options.output_gate_meta === "object" ? options.output_gate_meta : {};
    const src = Array.isArray(gate.source_files)
      ? gate.source_files.slice(0, 2).map((p) => path.basename(String(p || ""))).filter(Boolean).join(", ")
      : "";
    const insights = buildDataQualityInsights(rows);
    const maxFindings = Math.max(3, Number(layout.pptx_max_findings || 6));
    const maxEvidence = Math.max(3, Number(layout.pptx_max_evidence || 6));
    const highlights = buildEvidenceHighlights(rows, Math.max(maxEvidence, 12));
    const narrativeBullets = buildNarrativeBullets(rows, 8);
    const numericStats = buildNumericColumnStats(rows, 5);
    const pptImage = pickIllustrationImage(rows, options);
    const pptx = new PptxGenJS();
    pptx.layout = "LAYOUT_WIDE";

    const addFluentChrome = (slide, title = "") => {
      slide.background = { color: theme.bg };
      slide.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: 13.33, h: 0.18, fill: { color: accentPrimary }, line: { color: accentPrimary } });
      slide.addShape(pptx.ShapeType.roundRect, { x: 0.45, y: 0.92, w: 12.35, h: 5.55, fill: { color: chromeCardFill, transparency: chromeCardTrans }, line: { color: chromeLine }, radius: 0.12 });
      slide.addShape(pptx.ShapeType.roundRect, { x: 11.5, y: -0.7, w: 2.4, h: 2.0, fill: { color: "E8F3FB", transparency: isStrong ? 0 : 10 }, line: { color: "E8F3FB" } });
      if (title) slide.addText(title, { x: 0.5, y: 0.38, w: 8.0, h: 0.5, fontSize: isStrong ? 23 : 22, bold: true, color: accentPrimary, fontFace });
    };

    if (isDebateStyleTemplate(options)) {
      const blocks = buildDebateTableRows(rows, options);
      const groups = groupDebateRowsByBattlefield(blocks).slice(0, 12);
      const typeMap = new Map();
      const claimMap = new Map();
      blocks.forEach((block) => {
        const evidenceType = String(block.evidence_type || "实证");
        typeMap.set(evidenceType, (typeMap.get(evidenceType) || 0) + 1);
        const supportClaim = String(block.support_claim || "核心论点");
        claimMap.set(supportClaim, (claimMap.get(supportClaim) || 0) + 1);
      });

      const cover = pptx.addSlide();
      addFluentChrome(cover);
      cover.addText(reportTitle || "辩论资料库", { x: 0.6, y: 0.55, w: 11.8, h: 0.8, fontSize: isStrong ? 36 : 34, bold: true, color: accentPrimary, fontFace });
      cover.addText(`输入行: ${quality.input_rows}  输出行: ${quality.output_rows}  去重移除: ${quality.duplicate_rows_removed}`, { x: 0.7, y: 1.5, w: 12.0, h: 0.5, fontSize: 14, color: accentSecondary, fontFace });
      cover.addText(`战场数: ${groups.length}`, { x: 0.7, y: 1.9, w: 4.0, h: 0.4, fontSize: 13, color: accentSecondary, fontFace });
      if (pptImage) {
        try {
          const dim = readImageSize(pptImage);
          const fit = fitRect(Number(dim.width || 1), Number(dim.height || 1), 5.2, 3.0);
          const x = 7.6 + (5.2 - fit.w) / 2;
          const y = 2.2 + (3.0 - fit.h) / 2;
          cover.addImage({ path: pptImage, x, y, w: fit.w, h: fit.h });
        } catch {}
      }

      const toc = pptx.addSlide();
      addFluentChrome(toc, "目录 / 战场列表");
      const tocLines = groups.length ? groups.map((group, index) => `${index + 1}. ${group.battlefield}（${group.items.length}）`) : ["1. 论点支撑（0）"];
      toc.addText(tocLines.join("\n"), { x: 0.8, y: 1.2, w: 12.0, h: 5.2, fontSize: 16, color: "1F2D3D", fontFace, breakLine: true, valign: "top" });

      const sType = pptx.addSlide();
      addFluentChrome(sType, "证据类型分布图");
      const typePairs = Array.from(typeMap.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6);
      if (typePairs.length > 0) {
        try {
          sType.addChart(pptx.ChartType.pie, [{
            name: "证据条数",
            labels: typePairs.map((x) => x[0]),
            values: typePairs.map((x) => x[1]),
          }], {
            x: 0.9, y: 1.2, w: 5.8, h: 4.8,
            showLegend: true,
          });
        } catch {}
      }
      const claimPairs = Array.from(claimMap.entries()).sort((a, b) => b[1] - a[1]).slice(0, 6);
      if (claimPairs.length > 0) {
        try {
          sType.addChart(pptx.ChartType.bar, [{
            name: "论点条数",
            labels: claimPairs.map((x) => cleanOfficeText(x[0], 20)),
            values: claimPairs.map((x) => x[1]),
          }], {
            x: 7.0, y: 1.2, w: 5.9, h: 4.8,
            showLegend: false,
            catAxisLabelRotate: -20,
          });
        } catch {}
      }

      groups.forEach((group, index) => {
        const slide = pptx.addSlide();
        addFluentChrome(slide, `${index + 1}. ${group.battlefield}`);
        const picks = group.items.slice(0, 4);
        const left = [];
        const right = [];
        picks.forEach((item, itemIndex) => {
          left.push(`观点${itemIndex + 1}: ${cleanOfficeText(item.support_claim || item.conclusion, 44)}`);
          left.push(`类型/相关性: ${item.evidence_type || "实证"} / ${Number(item.relevance_score || 0)}`);
          left.push(`证据: ${cleanOfficeText(item.content, 90)}`);
          left.push("");
          const sourceLine = cleanOfficeText(item.source_org || "未标注", 48);
          const urlLine = cleanOfficeText(item.source_url || "", 88);
          right.push(`${itemIndex + 1}) ${sourceLine}${urlLine ? `\n${urlLine}` : ""}`);
          right.push("");
        });
        slide.addShape(pptx.ShapeType.roundRect, { x: 0.55, y: 1.05, w: 8.15, h: 5.75, fill: { color: "FFFFFF" }, line: { color: "DDE7F2" } });
        slide.addShape(pptx.ShapeType.roundRect, { x: 8.9, y: 1.05, w: 3.9, h: 5.75, fill: { color: "FFFFFF" }, line: { color: "DDE7F2" } });
        slide.addText(left.join("\n"), { x: 0.78, y: 1.3, w: 7.7, h: 5.25, fontSize: 12, color: "1F2D3D", fontFace, breakLine: true, valign: "top" });
        slide.addText(`出处\n\n${right.join("\n")}`, { x: 9.1, y: 1.3, w: 3.45, h: 5.25, fontSize: 11.5, color: "3A4A5E", fontFace, breakLine: true, valign: "top" });
      });

      if (warnings.length > 0) {
        const sWarn = pptx.addSlide();
        addFluentChrome(sWarn, "告警");
        sWarn.addText("告警", { x: 0.6, y: 0.45, w: 4.0, h: 0.6, fontSize: 23, bold: true, color: "B42318", fontFace });
        sWarn.addText(toBulletList(warnings.slice(0, 12)).join("\n"), { x: 0.8, y: 1.2, w: 12.0, h: 5.8, fontSize: 14, color: "1F2D3D", fontFace, breakLine: true, valign: "top" });
      }

      await pptx.writeFile({ fileName: filePath });
      return;
    }

    const s1 = pptx.addSlide();
    s1.background = { color: theme.bg };
    s1.addShape(pptx.ShapeType.rect, { x: 0, y: 0, w: 13.33, h: 0.24, fill: { color: accentPrimary }, line: { color: accentPrimary } });
    s1.addShape(pptx.ShapeType.roundRect, { x: 10.9, y: -0.7, w: 3.8, h: 3.2, fill: { color: "E8F3FB", transparency: 0 }, line: { color: "E8F3FB" } });
    s1.addShape(pptx.ShapeType.roundRect, { x: -0.8, y: 5.6, w: 4.2, h: 2.4, fill: { color: "E8F3FB", transparency: 20 }, line: { color: "E8F3FB" } });
    s1.addText(reportTitle || "离线清洗简报", { x: 0.55, y: 0.35, w: 11.2, h: 0.7, fontSize: highQuality ? (isStrong ? 36 : 35) : 31, bold: true, color: accentPrimary, fontFace });
    s1.addText(`Fluent 版式  |  离线模式  |  主题：${theme.title}`, { x: 0.6, y: 1.08, w: 10.6, h: 0.45, fontSize: 14, color: accentSecondary, fontFace });
    s1.addShape(pptx.ShapeType.roundRect, { x: 0.52, y: 1.72, w: 12.2, h: 4.85, fill: { color: chromeCardFill, transparency: isStrong ? 16 : 28 }, line: { color: chromeLine }, shadow: { type: "outer", color: "A8C5E6", blur: 4, angle: 45, distance: 2, opacity: 0.2 } });
    s1.addText(`输入行: ${quality.input_rows}    输出行: ${quality.output_rows}    去重移除: ${quality.duplicate_rows_removed}`, { x: 0.8, y: 2.1, w: 11, h: 0.7, fontSize: 20, fontFace });
    s1.addText(`来源: ${src || "N/A"}    风险: ${String(gate.preflight_risk_label || "-")}(${Number(gate.preflight_risk_score || 0)}/100)`, { x: 0.8, y: 2.8, w: 11.8, h: 0.5, fontSize: 13, fontFace, color: "4A5E74" });
    s1.addText(`质量分 ${Number(gate.quality_score || 0)} / invalid ${Number(gate.invalid_ratio || 0)} / gibberish ${Number(gate.gibberish_ratio || 0)}`, { x: 0.8, y: 3.2, w: 11.8, h: 0.5, fontSize: 12, fontFace, color: "4A5E74" });
    s1.addText(`问号密集过滤行 ${Number(gate.filtered_question_mark_rows || 0)}`, { x: 0.8, y: 3.55, w: 11.8, h: 0.4, fontSize: 12, fontFace, color: "4A5E74" });
    if (pptImage) {
      try {
        const dim = readImageSize(pptImage);
        const fit = fitRect(Number(dim.width || 1), Number(dim.height || 1), 5.0, 2.4);
        const x = 0.9 + (5.0 - fit.w) / 2;
        const y = 2.9 + (2.4 - fit.h) / 2;
        s1.addImage({ path: pptImage, x, y, w: fit.w, h: fit.h });
        s1.addText(`图示来源: ${path.basename(pptImage)}`, { x: 0.9, y: 5.4, w: 5.2, h: 0.25, fontSize: 10, color: accentSecondary, fontFace });
      } catch {}
    } else {
      s1.addShape(pptx.ShapeType.roundRect, { x: 7.65, y: 2.1, w: 4.8, h: 2.9, fill: { color: "F4F9FE" }, line: { color: "DCEBFA" } });
      s1.addText("图示占位区\n（建议拖入图片增强说明）", { x: 8.0, y: 3.0, w: 4.1, h: 1.2, fontSize: 14, color: "5B6F82", fontFace, align: "center" });
    }

    const s2 = pptx.addSlide();
    addFluentChrome(s2, "质量摘要");
    s2.addTable([
      [{ text: "指标", options: { bold: true } }, { text: "数值", options: { bold: true } }],
      ["输入行", String(quality.input_rows)],
      ["输出行", String(quality.output_rows)],
      ["过滤行", String(quality.filtered_rows)],
      ["无效行", String(quality.invalid_rows)],
      ["去重移除", String(quality.duplicate_rows_removed)],
    ], { x: 0.6, y: 1.1, w: 6.4, h: 3.4, border: { pt: 1, color: "DDE7F2" }, fontFace });
    const sourceCount2 = {};
    rows.forEach((row) => {
      const key = String(row.source_type || "unknown");
      sourceCount2[key] = (sourceCount2[key] || 0) + 1;
    });
    const top2 = Object.entries(sourceCount2).sort((a, b) => b[1] - a[1]).slice(0, 5);
    if (top2.length > 0) {
      try {
        s2.addChart(pptx.ChartType.bar, [{
          name: "来源分布",
          labels: top2.map((x) => cleanOfficeText(x[0], 16)),
          values: top2.map((x) => x[1]),
        }], {
          x: 7.35, y: 1.1, w: 5.1, h: 3.9,
          barDir: "col",
          showLegend: false,
          catAxisLabelRotate: -20,
        });
      } catch {}
    }

    const sNarr = pptx.addSlide();
    addFluentChrome(sNarr, "摘要要点（适配外部AI提炼文本）");
    const narr = (narrativeBullets.length ? narrativeBullets : highlights.map((highlight) => highlight.text)).slice(0, 6);
    sNarr.addText(toBulletList(narr, "• ").join("\n"), {
      x: 0.7, y: 1.0, w: 7.2, h: 5.3, fontSize: 14, color: "1F2D3D", fontFace, breakLine: true, valign: "top",
    });
    if (numericStats.length > 0) {
      sNarr.addTable(
        [[
          { text: "数值字段", options: { bold: true } },
          { text: "均值", options: { bold: true } },
          { text: "最小", options: { bold: true } },
          { text: "最大", options: { bold: true } },
        ]].concat(
          numericStats.slice(0, 4).map((stat) => [String(stat.key), String(stat.avg), String(stat.min), String(stat.max)])
        ),
        { x: 8.1, y: 1.0, w: 4.5, h: 3.8, border: { pt: 1, color: "DDE7F2" }, fontFace, fontSize: 10 }
      );
    }

    const s3 = pptx.addSlide();
    addFluentChrome(s3, "样例数据（前 6 行）");
    const cols = rows.length > 0 ? unionColumns(rows).slice(0, 3) : ["id", "text", "amount"];
    const tableRows = [[...cols]];
    const sampleRowsByHeight = fitTableBodyRowCount(4.6, { header_rows: 1, row_height: 0.42, pad_top: 0.08, pad_bottom: 0.08 });
    const sampleRows = Math.min(sampleRowsByHeight, Math.max(3, Number(layout.pptx_sample_rows || 6)));
    rows.slice(0, sampleRows).forEach((row) => tableRows.push(cols.map((column) => cleanOfficeText(String(row[column] ?? ""), 44))));
    s3.addTable(tableRows, { x: 0.6, y: 1.05, w: 12.0, h: 4.6, border: { pt: 1, color: "DDE7F2" }, fontFace, fontSize: 11 });

    const s4 = pptx.addSlide();
    addFluentChrome(s4, "关键发现");
    const findingText = (insights.length ? insights : ["暂无显著结构冲突，建议继续补充样本规模。"])
      .slice(0, maxFindings)
      .map((item) => `• ${item}`)
      .join("\n");
    s4.addText(findingText, { x: 0.6, y: 1.0, w: 7.6, h: 5.2, fontSize: 15, color: "1F2D3D", fontFace });
    const sourceCount = {};
    rows.forEach((row) => {
      const key = String(row.source_type || "unknown");
      sourceCount[key] = (sourceCount[key] || 0) + 1;
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

    const evBullets = (highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
      .map((item) => `[${cleanOfficeText(item.source, 40)}] ${cleanOfficeText(item.text, 150)}`);
    const evPages = paginateBullets(evBullets, Math.max(4, Math.min(7, Number(layout.pptx_evidence_page_size || 6))));
    evPages.forEach((page, index) => {
      const sEvidence = pptx.addSlide();
      addFluentChrome(sEvidence, `证据摘录${evPages.length > 1 ? ` (${index + 1}/${evPages.length})` : ""}`);
      sEvidence.addText(toBulletList(page).join("\n"), {
        x: 0.6, y: 1.0, w: 12.0, h: 5.7, fontSize: 13.5, color: "1F2D3D", fontFace, breakLine: true, valign: "top",
      });
    });

    if (warnings.length > 0) {
      const warnPages = paginateBullets(warnings.map((warning) => cleanOfficeText(warning, 140)), 8);
      warnPages.forEach((page, index) => {
        const sWarn = pptx.addSlide();
        addFluentChrome(sWarn, `告警${warnPages.length > 1 ? ` (${index + 1}/${warnPages.length})` : ""}`);
        sWarn.addText(`告警${warnPages.length > 1 ? ` (${index + 1}/${warnPages.length})` : ""}`, { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 22, bold: true, color: "B42318", fontFace });
        sWarn.addText(toBulletList(page).join("\n"), { x: 0.6, y: 1.0, w: 12.0, h: 5.7, fontSize: 14.5, color: "1F2D3D", fontFace, breakLine: true, valign: "top" });
      });
    }

    if (highQuality) {
      const s5 = pptx.addSlide();
      addFluentChrome(s5, "建议与行动");
      s5.addText(
        "1) 优先排查无效行来源并修正录入规则\n2) 直接复用本报告中的指标表和样例表\n3) 下次作业沿用同一主题，保持视觉一致",
        { x: 0.7, y: 1.2, w: 12.0, h: 3.8, fontSize: 17, color: accentSecondary, fontFace }
      );
    }

    await pptx.writeFile({ fileName: filePath });
  }

  return {
    writePptx,
  };
}

module.exports = {
  createOfflineOutputPptx,
};
