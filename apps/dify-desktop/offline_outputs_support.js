function createOfflineOutputSupport({
  unionColumns,
  cleanOfficeText,
  looksLikeReferenceEntry,
  isLikelyCorruptedText,
  isQuestionMarkHeavy,
  buildEvidenceHighlights,
}) {
  function toFiniteNumber(value) {
    if (value === null || value === undefined) return null;
    const normalized = String(value).replace(/[,\s，￥¥$]/g, "").trim();
    if (!normalized) return null;
    const numberValue = Number(normalized);
    return Number.isFinite(numberValue) ? numberValue : null;
  }

  function buildNumericColumnStats(rows = [], maxCols = 4) {
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) return [];
    const keys = unionColumns(list);
    const output = [];
    for (const key of keys) {
      if (/^(text|content|summary|title|source_file|source_type|id|url|link)$/i.test(key)) continue;
      const values = [];
      for (const row of list) {
        const numberValue = toFiniteNumber(row?.[key]);
        if (numberValue !== null) values.push(numberValue);
      }
      if (values.length < Math.max(6, Math.ceil(list.length * 0.15))) continue;
      const min = Math.min(...values);
      const max = Math.max(...values);
      const avg = values.reduce((a, b) => a + b, 0) / values.length;
      output.push({
        key,
        count: values.length,
        min: Number(min.toFixed(4)),
        max: Number(max.toFixed(4)),
        avg: Number(avg.toFixed(4)),
      });
    }
    output.sort((a, b) => b.count - a.count);
    return output.slice(0, Math.max(1, Math.min(8, Number(maxCols || 4))));
  }

  function buildNarrativeBullets(rows = [], maxItems = 8) {
    const output = [];
    const seen = new Set();
    const keys = unionColumns(Array.isArray(rows) ? rows : []);
    const textKeys = ["summary", "abstract", "conclusion", "claim", "point", "analysis", "text", "content"]
      .filter((key) => keys.includes(key));
    const scanKeys = textKeys.length
      ? textKeys
      : keys.filter((key) => /(summary|abstract|conclusion|claim|point|analysis|text|content|note|remark|观点|结论|摘要)/i.test(key));

    for (const row of rows || []) {
      for (const key of scanKeys) {
        const text = cleanOfficeText(row?.[key] || "", 180);
        if (!text || text.length < 20) continue;
        if (looksLikeReferenceEntry(text) || isLikelyCorruptedText(text) || isQuestionMarkHeavy(text)) continue;
        const signature = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!signature || signature.length < 20 || seen.has(signature)) continue;
        seen.add(signature);
        output.push(text);
        if (output.length >= maxItems) return output;
      }
    }

    if (output.length < maxItems) {
      const fallback = buildEvidenceHighlights(rows, maxItems);
      for (const item of fallback) {
        const text = cleanOfficeText(item.text || "", 180);
        if (!text) continue;
        const signature = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!signature || seen.has(signature)) continue;
        seen.add(signature);
        output.push(text);
        if (output.length >= maxItems) break;
      }
    }

    return output;
  }

  return {
    buildNumericColumnStats,
    buildNarrativeBullets,
    toFiniteNumber,
  };
}

module.exports = {
  createOfflineOutputSupport,
};
