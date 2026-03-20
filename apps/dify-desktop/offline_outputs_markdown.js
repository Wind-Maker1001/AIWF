const pathModuleDefault = require("path");

function createOfflineOutputMarkdown({ normalizeLineText, rowTextForQuality, pathModule = pathModuleDefault, buildDataQualityInsights, buildEvidenceHighlights }) {
  function groupRowsBySource(rows) {
    const map = new Map();
    for (const row of rows || []) {
      const src = pathModule.basename(String(row.source_file || "unknown"));
      if (!map.has(src)) map.set(src, []);
      map.get(src).push(row);
    }
    return map;
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
    (insights.length ? insights : ["当前未检测到显著结构冲突。"]).slice(0, 10).forEach((item) => lines.push(`- ${item}`));
    lines.push("");
    lines.push("## 证据摘录");
    (highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
      .forEach((item, index) => lines.push(`${index + 1}. [${item.source}] ${item.text}`));
    lines.push("");
    lines.push("## 分文件提取摘要");
    for (const [src, items] of grouped.entries()) {
      lines.push(`### ${src}`);
      lines.push(`- 提取片段数: ${items.length}`);
      const snippets = items
        .map((row) => normalizeLineText(rowTextForQuality(row)))
        .filter((text) => text.length >= 30)
        .slice(0, 8);
      if (snippets.length === 0) {
        lines.push("- （无有效文本片段）");
      } else {
        snippets.forEach((text) => lines.push(`- ${text.length > 220 ? `${text.slice(0, 220)}...` : text}`));
      }
      lines.push("");
    }
    if (warnings && warnings.length) {
      lines.push("## 告警");
      warnings.slice(0, 30).forEach((warning) => lines.push(`- ${warning}`));
      lines.push("");
    }
    require("fs").writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
  }

  async function writeFilteredNoiseMarkdown(filePath, rows = []) {
    const list = Array.isArray(rows) ? rows : [];
    const lines = [];
    lines.push("# 过滤噪声清单（问号密集行）");
    lines.push("");
    lines.push(`- 条数: ${list.length}`);
    lines.push(`- 生成时间: ${new Date().toISOString()}`);
    lines.push("");
    if (!list.length) {
      lines.push("- （无）");
    } else {
      list.forEach((row, index) => {
        const src = pathModule.basename(String(row?.source_file || ""));
        const rowNo = String(row?.row_no ?? "");
        const text = normalizeLineText(rowTextForQuality(row)).slice(0, 300);
        lines.push(`## ${index + 1}. ${src}${rowNo ? `#${rowNo}` : ""}`);
        lines.push(`- source_type: ${String(row?.source_type || "")}`);
        lines.push(`- text: ${text}`);
        lines.push("");
      });
    }
    require("fs").writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
  }

  return {
    writeMarkdown,
    writeFilteredNoiseMarkdown,
  };
}

module.exports = {
  createOfflineOutputMarkdown,
};
