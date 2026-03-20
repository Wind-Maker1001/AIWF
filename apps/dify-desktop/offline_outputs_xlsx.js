const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

function createOfflineOutputXlsx({
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
}) {
  function excelColName(col) {
    let n = Number(col || 1);
    if (!Number.isFinite(n) || n <= 0) return "A";
    let out = "";
    while (n > 0) {
      const m = (n - 1) % 26;
      out = String.fromCharCode(65 + m) + out;
      n = Math.floor((n - 1) / 26);
    }
    return out;
  }

  function tryInjectXlsxCharts(filePath, warnings = []) {
    const warnList = Array.isArray(warnings) ? warnings : [];
    try {
      const py = [
        "from openpyxl import load_workbook",
        "from openpyxl.chart import BarChart, PieChart, Reference",
        "from openpyxl.styles import Font",
        "import sys",
        "p=sys.argv[1]",
        "wb=load_workbook(p)",
        "if 'chart_data' not in wb.sheetnames: raise RuntimeError('chart_data_missing')",
        "src=wb['chart_data']",
        "dash=wb['cleaned'] if 'cleaned' in wb.sheetnames else (wb['dashboard'] if 'dashboard' in wb.sheetnames else wb.create_sheet('dashboard'))",
        "for chart in list(dash._charts): dash._charts.remove(chart)",
        "dash['G2']='来源分布（柱状图）'; dash['N2']='来源分布（饼图）'; dash['G18']='质量指标（柱状图）'",
        "max_row=src.max_row",
        "source_rows=[r for r in range(2, max_row+1) if str(src.cell(r,3).value or '')=='source_type_count']",
        "quality_rows=[r for r in range(2, max_row+1) if str(src.cell(r,3).value or '')=='quality']",
        "if source_rows:",
        "  src_start=min(source_rows); src_end=max(source_rows)",
        "  b=BarChart(); b.title='来源类型计数'; b.height=7; b.width=10",
        "  b.add_data(Reference(src, min_col=2, min_row=1, max_row=src_end), titles_from_data=True)",
        "  b.set_categories(Reference(src, min_col=1, min_row=src_start, max_row=src_end)); dash.add_chart(b,'G3')",
        "  p1=PieChart(); p1.title='来源占比'; p1.height=7; p1.width=7",
        "  p1.add_data(Reference(src, min_col=2, min_row=src_start, max_row=src_end), titles_from_data=False)",
        "  p1.set_categories(Reference(src, min_col=1, min_row=src_start, max_row=src_end)); dash.add_chart(p1,'N3')",
        "if quality_rows:",
        "  q_start=min(quality_rows); q_end=max(quality_rows)",
        "  q=BarChart(); q.title='质量指标'; q.height=6; q.width=10",
        "  q.add_data(Reference(src, min_col=2, min_row=q_start, max_row=q_end), titles_from_data=False)",
        "  q.set_categories(Reference(src, min_col=1, min_row=q_start, max_row=q_end)); dash.add_chart(q,'G19')",
        "wb.save(p)",
      ].join("\n");
      const r = spawnSync("python", ["-c", py, filePath], {
        encoding: "utf8",
        windowsHide: true,
        timeout: 15000,
      });
      if (r.status !== 0) {
        const err = String((r.stderr || r.stdout || "").trim());
        if (err) warnList.push(`xlsx_chart_postprocess_failed:${err.slice(0, 240)}`);
      }
    } catch (e) {
      warnList.push(`xlsx_chart_postprocess_failed:${String(e && e.message ? e.message : e)}`);
    }
  }

  async function writeXlsx(filePath, rows, quality, warnings, options = {}) {
    const ExcelJS = getExcelJSModule();
    const theme = resolveOfficeTheme(options.office_theme);
    const themeName = String(options.office_theme || "").toLowerCase();
    const isStrong = /fluent_ms_strong/i.test(themeName);
    const isVibrant = /fluent_ms_vibrant/i.test(themeName);
    const visual = resolveVisualPack(theme, { isStrong, isVibrant });
    const accentPrimary = visual.accent_primary;
    const accentSecondary = visual.accent_secondary;
    const lightPanelFill = visual.panel_fill;
    const lightCardFill = visual.card_fill;
    const borderColor = visual.border;
    const zebraFill = visual.zebra;
    const layout = typeof resolveOfficeLayout === "function" ? resolveOfficeLayout(options.office_theme) : {};
    const highQuality = String(options.office_quality_mode || "high").toLowerCase() !== "standard";
    const fontName = resolveOfficeFont(options, warnings);
    const warnList = Array.isArray(warnings) ? warnings : [];
    const wb = new ExcelJS.Workbook();
    const requestedTemplatePath = String(options.xlsx_template_path || options.office_xlsx_template_path || "").trim();
    const templateCandidates = requestedTemplatePath ? [requestedTemplatePath] : [];
    const templatePath = templateCandidates.find((p) => {
      try {
        const abs = path.isAbsolute(p) ? p : path.resolve(process.cwd(), p);
        return fs.existsSync(abs);
      } catch {
        return false;
      }
    }) || "";
    if (templatePath) {
      try {
        const abs = path.isAbsolute(templatePath) ? templatePath : path.resolve(process.cwd(), templatePath);
        if (fs.existsSync(abs)) await wb.xlsx.readFile(abs);
        else warnList.push(`xlsx_template_not_found:${templatePath}`);
      } catch (e) {
        warnList.push(`xlsx_template_load_failed:${String(e && e.message ? e.message : e)}`);
      }
    } else if (requestedTemplatePath) {
      warnList.push(`xlsx_template_not_found:${requestedTemplatePath}`);
    }

    const getSheet = (name, create = true) => {
      const n = String(name || "").trim();
      if (!n) return null;
      let ws = wb.getWorksheet(n);
      if (!ws && create) ws = wb.addWorksheet(n);
      return ws || null;
    };
    const resetSheet = (ws) => {
      if (!ws) return;
      if (ws.rowCount > 0) ws.spliceRows(1, ws.rowCount);
    };
    const sourceTypeMap = new Map();
    (rows || []).forEach((r) => {
      const k = String(r?.source_type || "unknown");
      sourceTypeMap.set(k, Number(sourceTypeMap.get(k) || 0) + 1);
    });

    if (isDebateStyleTemplate(options)) {
      const ws = getSheet(String(layout.xlsx_data_sheet_name || "Sheet1")) || wb.addWorksheet("Sheet1");
      resetSheet(ws);
      ws.properties = { defaultRowHeight: 22 };
      ws.views = [{ state: "frozen", ySplit: 1 }];
      ws.columns = [
        { width: 4 }, { width: 16 }, { width: 24 }, { width: 30 },
        { width: 56 }, { width: 20 }, { width: 34 }, { width: 56 },
      ];
      ws.addRow([null, null, "战场", "结论", "内容", "出处", "来源", "例子（推论用）"]);
      const packRows = buildDebateTableRows(rows, options);
      packRows.forEach((r) => ws.addRow([
        null, r.battlefield, r.support_claim, `[${r.evidence_type}|${r.relevance_score}] ${r.conclusion}`,
        r.content, r.source_org, r.source_url, r.example,
      ]));
      ws.getRow(1).eachCell((cell, col) => {
        if (col < 3) return;
        cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentPrimary}` } };
      });
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
    } else {
      const ws = getSheet(String(layout.xlsx_data_sheet_name || "cleaned")) || wb.addWorksheet("cleaned");
      resetSheet(ws);
      ws.properties = { defaultRowHeight: 20 };
      const keys = rows.length > 0 ? unionColumns(rows) : ["id", "amount", "text"];
      const visualCols = Math.max(keys.length, 14);
      ws.columns = Array.from({ length: visualCols }).map((_, i) => ({ width: i < keys.length ? 23 : 15 }));
      ws.views = [{ state: "frozen", ySplit: 29 }];

      ws.getCell("A1").value = "Fluent 数据看板 + 清洗结果（单页）";
      ws.mergeCells("A1:F1");
      ws.getCell("A1").font = { name: fontName, bold: true, size: 18, color: { argb: `FF${accentPrimary}` } };
      ws.getCell("A1").alignment = { vertical: "middle", horizontal: "left" };
      ws.getCell("A2").value = "左侧为统计与质量指标，右侧为图表区域，下方为 cleaned 明细表。";
      ws.mergeCells("A2:F2");
      ws.getCell("A2").font = { name: fontName, size: 11, color: { argb: "FF4A5E74" } };
      ws.mergeCells("A3:N3");
      ws.getCell("A3").fill = { type: "pattern", pattern: "solid", fgColor: { argb: lightPanelFill } };

      const gateScore = Number((options.output_gate_meta && options.output_gate_meta.quality_score) || 0);
      ws.mergeCells("A4:B6");
      ws.mergeCells("C4:D6");
      ws.mergeCells("E4:F6");
      ws.getCell("A4").value = `输入行\n${Number(quality.input_rows || 0)}`;
      ws.getCell("C4").value = `输出行\n${Number(quality.output_rows || 0)}`;
      ws.getCell("E4").value = `质量分\n${Number.isFinite(gateScore) ? gateScore : 0}`;
      ["A4", "C4", "E4"].forEach((c) => {
        ws.getCell(c).font = { name: fontName, bold: true, size: 12, color: { argb: "FF103B66" } };
        ws.getCell(c).alignment = { vertical: "middle", horizontal: "center", wrapText: true };
        ws.getCell(c).fill = { type: "pattern", pattern: "solid", fgColor: { argb: lightCardFill } };
        ws.getCell(c).border = {
          top: { style: "thin", color: { argb: borderColor } },
          left: { style: "thin", color: { argb: borderColor } },
          bottom: { style: "thin", color: { argb: borderColor } },
          right: { style: "thin", color: { argb: borderColor } },
        };
      });

      const sourcePairs = Array.from(sourceTypeMap.entries()).sort((a, b) => b[1] - a[1]).slice(0, 10);
      ws.getRow(10).values = ["来源类型", "数量"];
      ws.getRow(10).eachCell((cell) => {
        cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentPrimary}` } };
      });
      const sourceStartRow = 11;
      sourcePairs.forEach(([k, v], idx) => ws.getRow(sourceStartRow + idx).values = [`source:${k}`, Number(v || 0)]);
      const sourceEndRow = sourceStartRow + Math.max(0, sourcePairs.length - 1);

      ws.getRow(18).values = ["质量指标", "数值"];
      ws.getRow(18).eachCell((cell) => {
        cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentSecondary}` } };
      });
      const qStart = 19;
      const qualityPairs = [
        ["input_rows", Number(quality.input_rows || 0)],
        ["output_rows", Number(quality.output_rows || 0)],
        ["invalid_rows", Number(quality.invalid_rows || 0)],
        ["filtered_rows", Number(quality.filtered_rows || 0)],
      ];
      qualityPairs.forEach((x, i) => ws.getRow(qStart + i).values = x);
      const qEnd = qStart + qualityPairs.length - 1;
      ws.getCell("A24").value = "说明";
      ws.getCell("B24").value = "图表和数据表已整合在 cleaned 单页。";
      ws.getCell("A24").font = { name: fontName, bold: true };
      ws.getCell("B24").font = { name: fontName, color: { argb: "FF4A5E74" } };

      const numericStats = buildNumericColumnStats(rows, 3);
      const statsHeaderRow = 25;
      ws.getRow(statsHeaderRow).values = ["数值字段", "均值", "最小", "最大", "样本数"];
      ws.getRow(statsHeaderRow).eachCell((cell) => {
        cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentSecondary}` } };
      });
      numericStats.forEach((s, i) => {
        ws.getRow(statsHeaderRow + 1 + i).values = [String(s.key), Number(s.avg), Number(s.min), Number(s.max), Number(s.count)];
      });

      const dataHeaderRow = 31;
      ws.getRow(dataHeaderRow).values = keys;
      ws.getRow(dataHeaderRow).eachCell((cell) => {
        cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
        cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentPrimary}` } };
      });
      rows.forEach((r, idx) => {
        ws.getRow(dataHeaderRow + 1 + idx).values = keys.map((k) => r[k]);
      });
      ws.autoFilter = {
        from: { row: dataHeaderRow, column: 1 },
        to: { row: dataHeaderRow, column: Math.max(1, keys.length) },
      };
      if (highQuality) {
        for (let i = dataHeaderRow + 1; i <= dataHeaderRow + rows.length; i += 1) {
          if (i % 2 === 0) {
            ws.getRow(i).eachCell((cell) => {
              cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: zebraFill } };
            });
          }
        }
      }
      for (let i = 4; i <= dataHeaderRow + rows.length; i += 1) {
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
      try {
        if (sourceEndRow >= sourceStartRow) {
          ws.addConditionalFormatting({
            ref: `B${sourceStartRow}:B${sourceEndRow}`,
            rules: [{ type: "dataBar", cfvo: [{ type: "min" }, { type: "max" }], color: `FF${accentPrimary}`, showValue: true }],
          });
        }
        ws.addConditionalFormatting({
          ref: `B${qStart}:B${qEnd}`,
          rules: [{ type: "dataBar", cfvo: [{ type: "min" }, { type: "max" }], color: `FF${accentSecondary}`, showValue: true }],
        });
      } catch {}
      const numericKeys = numericStats.map((s) => s.key);
      try {
        if (rows.length > 0) {
          numericKeys.slice(0, 2).forEach((k) => {
            const idx = keys.indexOf(k);
            if (idx < 0) return;
            const colName = excelColName(idx + 1);
            ws.addConditionalFormatting({
              ref: `${colName}${dataHeaderRow + 1}:${colName}${dataHeaderRow + rows.length}`,
              rules: [{ type: "dataBar", cfvo: [{ type: "min" }, { type: "max" }], color: `FF${accentPrimary}`, showValue: true }],
            });
          });
        }
      } catch {}
      const bundledChartImage = resolveBundledFluentChartImage();
      if (bundledChartImage) {
        try {
          const imgId = wb.addImage({ filename: bundledChartImage, extension: "png" });
          ws.addImage(imgId, { tl: { col: 6.2, row: 23.8 }, ext: { width: 740, height: 220 } });
        } catch {}
      }
    }

      const summary = getSheet(String(layout.xlsx_summary_sheet_name || "summary")) || wb.addWorksheet("summary");
    resetSheet(summary);
    summary.properties = { defaultRowHeight: 20 };
    summary.addRow(["指标", "数值"]);
    summary.getRow(1).eachCell((cell) => {
      cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentPrimary}` } };
    });
    summary.addRow(["主题", theme.title]);
    summary.addRow(["质量模式", highQuality ? "high" : "standard"]);
    summary.addRow(["输入行", quality.input_rows]);
    summary.addRow(["输出行", quality.output_rows]);
    summary.addRow(["过滤行", quality.filtered_rows]);
    summary.addRow(["无效行", quality.invalid_rows]);
    summary.addRow(["去重移除", quality.duplicate_rows_removed]);
    const gate = options.output_gate_meta && typeof options.output_gate_meta === "object" ? options.output_gate_meta : {};
    const src = Array.isArray(gate.source_files) ? gate.source_files.slice(0, 3).map((p) => path.basename(String(p || ""))).filter(Boolean).join(", ") : "";
    if (src) summary.addRow(["数据来源", src]);
    if (gate.preflight_risk_label) summary.addRow(["预检风险等级", `${String(gate.preflight_risk_label)} (${Number(gate.preflight_risk_score || 0)}/100)`]);
    if (Number.isFinite(Number(gate.quality_score))) summary.addRow(["质量分", Number(gate.quality_score)]);
    if (Number.isFinite(Number(gate.invalid_ratio))) summary.addRow(["invalid_ratio", Number(gate.invalid_ratio)]);
    if (Number.isFinite(Number(gate.gibberish_ratio))) summary.addRow(["gibberish_ratio", Number(gate.gibberish_ratio)]);
    if (Number.isFinite(Number(gate.filtered_question_mark_rows))) summary.addRow(["过滤问号密集行", Number(gate.filtered_question_mark_rows)]);
    summary.columns = [{ width: 22 }, { width: 24 }];
    for (let i = 2; i <= summary.rowCount; i += 1) {
      summary.getRow(i).eachCell((cell) => {
        cell.font = { name: fontName };
        if (i % 2 === 0) cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: zebraFill } };
      });
    }

    const chartData = getSheet("chart_data") || wb.addWorksheet("chart_data");
    resetSheet(chartData);
    chartData.columns = [{ width: 24 }, { width: 16 }, { width: 16 }];
    chartData.addRow(["维度", "值", "备注"]);
    Array.from(sourceTypeMap.entries()).sort((a, b) => b[1] - a[1]).slice(0, 12).forEach(([k, v]) => chartData.addRow([`source:${k}`, v, "source_type_count"]));
    chartData.addRow(["quality:input_rows", Number(quality.input_rows || 0), "quality"]);
    chartData.addRow(["quality:output_rows", Number(quality.output_rows || 0), "quality"]);
    chartData.addRow(["quality:invalid_rows", Number(quality.invalid_rows || 0), "quality"]);
    chartData.addRow(["quality:filtered_rows", Number(quality.filtered_rows || 0), "quality"]);
    buildNumericColumnStats(rows, 4).forEach((s) => chartData.addRow([`numeric:${s.key}:avg`, Number(s.avg), "numeric_mean"]));
    chartData.getRow(1).eachCell((cell) => {
      cell.font = { name: fontName, bold: true, color: { argb: "FFFFFFFF" } };
      cell.fill = { type: "pattern", pattern: "solid", fgColor: { argb: `FF${accentPrimary}` } };
    });

    const maybeDashboard = getSheet("dashboard", false);
    if (maybeDashboard) wb.removeWorksheet(maybeDashboard.id);
    const maybeVisual = getSheet("visual", false);
    if (maybeVisual) wb.removeWorksheet(maybeVisual.id);

    if (warnList.length > 0) {
      const warn = getSheet("warnings") || wb.addWorksheet("warnings");
      resetSheet(warn);
      warn.addRow(["告警"]);
      warn.getRow(1).eachCell((cell) => { cell.font = { name: fontName, bold: true }; });
      warnList.forEach((w) => warn.addRow([w]));
      for (let i = 2; i <= warn.rowCount; i += 1) {
        warn.getRow(i).eachCell((cell) => { cell.font = { name: fontName }; });
      }
      warn.columns = [{ width: 120 }];
    }

    await wb.xlsx.writeFile(filePath);
    if (options.xlsx_embed_charts !== false) {
      tryInjectXlsxCharts(filePath, warnList);
    }
  }

  return {
    writeXlsx,
  };
}

module.exports = {
  createOfflineOutputXlsx,
};
