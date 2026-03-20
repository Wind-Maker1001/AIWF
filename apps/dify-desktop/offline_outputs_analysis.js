const pathModuleDefault = require("path");

function createOfflineOutputAnalysis({ normalizeLineText, isLikelyCorruptedText, looksLikeReferenceEntry, rowTextForQuality, pathModule = pathModuleDefault }) {
  function isQuestionMarkHeavy(text) {
    const s = String(text || "").trim();
    if (!s || s.length < 12) return false;
    const marks = (s.match(/[?？]/g) || []).length;
    const ratio = marks / Math.max(1, s.length);
    return marks >= 4 && ratio >= 0.28;
  }

  function filterRowsForOffice(rows) {
    const list = Array.isArray(rows) ? rows : [];
    const kept = [];
    const removedRows = [];
    let filtered = 0;
    list.forEach((r) => {
      const t = rowTextForQuality(r);
      if (isQuestionMarkHeavy(t)) {
        filtered += 1;
        removedRows.push(r);
        return;
      }
      kept.push(r);
    });
    return { rows: kept, filtered, removedRows };
  }

  function looksLikeTitleLine(text) {
    const s = normalizeLineText(String(text || ""));
    if (!s) return false;
    if (s.length < 6 || s.length > 120) return false;
    if (isLikelyCorruptedText(s)) return false;
    if (looksLikeReferenceEntry(s)) return false;
    if (/^(摘要|abstract|关键词|keywords|introduction|引言|references|参考文献)[:：]?$/i.test(s)) return false;
    if (/[。！？!?；;]$/.test(s)) return false;
    return true;
  }

  function assessContentQuality(rows, options = {}) {
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) {
      return {
        score: 0,
        level: "poor",
        pass: false,
        thresholds: { min_score: 60 },
        metrics: {
          title_coverage: 0,
          paragraph_coverage: 0,
          coherent_paragraph_ratio: 0,
          numeric_consistency: 0,
        },
        reasons: ["empty_rows"],
      };
    }

    const bySource = new Map();
    list.forEach((r) => {
      const src = String(r?.source_file || "unknown");
      if (!bySource.has(src)) bySource.set(src, []);
      bySource.get(src).push(r);
    });
    let sourceWithTitle = 0;
    bySource.forEach((items) => {
      const top = items.slice(0, 8);
      if (top.some((r) => looksLikeTitleLine(rowTextForQuality(r)))) sourceWithTitle += 1;
    });

    let paraCount = 0;
    let coherentPara = 0;
    let numericRows = 0;
    let numericGood = 0;
    list.forEach((r) => {
      const t = rowTextForQuality(r);
      if (!t) return;
      if (t.length >= 40 && !isLikelyCorruptedText(t) && !looksLikeReferenceEntry(t)) {
        paraCount += 1;
        if (/[。！？!?；;.]$/.test(t) || t.length >= 80) coherentPara += 1;
      }
      const nums = t.match(/[-+]?\d+(?:[.,]\d+)?/g) || [];
      if (nums.length > 0) {
        numericRows += 1;
        const hasWeird = /[A-Za-z]{3,}\d+|\d+[A-Za-z]{3,}/.test(t);
        if (!hasWeird) numericGood += 1;
      }
    });

    const titleCoverage = bySource.size > 0 ? sourceWithTitle / bySource.size : 0;
    const paragraphCoverage = list.length > 0 ? paraCount / list.length : 0;
    const coherentRatio = paraCount > 0 ? coherentPara / paraCount : 0;
    const numericConsistency = numericRows > 0 ? numericGood / numericRows : 1;

    let score = 100;
    score -= Math.max(0, (0.85 - titleCoverage)) * 30;
    score -= Math.max(0, (0.35 - paragraphCoverage)) * 45;
    score -= Math.max(0, (0.55 - coherentRatio)) * 30;
    score -= Math.max(0, (0.85 - numericConsistency)) * 25;
    score = Math.max(0, Math.min(100, score));
    const minScore = Number.isFinite(Number(options.min_content_score))
      ? Math.max(0, Math.min(100, Number(options.min_content_score)))
      : 60;

    const reasons = [];
    if (titleCoverage < 0.85) reasons.push("title_coverage_low");
    if (paragraphCoverage < 0.35) reasons.push("paragraph_coverage_low");
    if (coherentRatio < 0.55) reasons.push("coherence_low");
    if (numericConsistency < 0.85) reasons.push("numeric_consistency_low");
    const level = score >= 85 ? "excellent" : (score >= 70 ? "good" : (score >= 55 ? "fair" : "poor"));
    return {
      score: Number(score.toFixed(2)),
      level,
      pass: score >= minScore,
      thresholds: { min_score: minScore },
      metrics: {
        title_coverage: Number(titleCoverage.toFixed(4)),
        paragraph_coverage: Number(paragraphCoverage.toFixed(4)),
        coherent_paragraph_ratio: Number(coherentRatio.toFixed(4)),
        numeric_consistency: Number(numericConsistency.toFixed(4)),
      },
      reasons,
    };
  }

  function computeOfficeQualityScore(rows, quality = {}, warnings = [], options = {}) {
    const total = Array.isArray(rows) ? rows.length : 0;
    if (total <= 0) return { score: 0, level: "poor", reasons: ["empty_rows"] };
    const evalRows = rows
      .map((r) => rowTextForQuality(r))
      .filter((t) => String(t || "").length >= 4);
    const bad = evalRows.filter((t) => isLikelyCorruptedText(t)).length;
    const gibberishRatio = evalRows.length > 0 ? (bad / evalRows.length) : 0;
    const invalidRatio = Number(quality.input_rows || 0) > 0
      ? Number(quality.invalid_rows || 0) / Number(quality.input_rows || 1)
      : 0;
    const warnPenalty = Math.min(18, (Array.isArray(warnings) ? warnings.length : 0) * 3);
    const mdOnlyPenalty = options?.md_only ? 6 : 0;
    let score = 100;
    score -= Math.min(45, gibberishRatio * 120);
    score -= Math.min(20, invalidRatio * 120);
    score -= warnPenalty;
    score -= mdOnlyPenalty;
    score = Math.max(0, Math.min(100, score));
    const level = score >= 85 ? "excellent" : (score >= 70 ? "good" : (score >= 55 ? "fair" : "poor"));
    const reasons = [];
    if (gibberishRatio >= 0.2) reasons.push("high_gibberish_ratio");
    if (invalidRatio >= 0.1) reasons.push("high_invalid_ratio");
    if ((Array.isArray(warnings) ? warnings.length : 0) >= 4) reasons.push("many_warnings");
    if (options?.md_only) reasons.push("md_only_mode");
    return {
      score: Number(score.toFixed(2)),
      level,
      gibberish_ratio: Number(gibberishRatio.toFixed(4)),
      invalid_ratio: Number(invalidRatio.toFixed(4)),
      warning_count: Array.isArray(warnings) ? warnings.length : 0,
      reasons,
    };
  }
  function readImageSize(filePath) {
    try {
      const imageSize = getImageSizeModule();
      if (typeof imageSize === "function") return imageSize(filePath);
      if (imageSize && typeof imageSize.imageSize === "function") return imageSize.imageSize(filePath);
      if (imageSize && typeof imageSize.default === "function") return imageSize.default(filePath);
    } catch {}
    return { width: 1, height: 1 };
  }

  function cleanOfficeText(input, maxLen = 260) {
    let s = normalizeLineText(String(input || ""));
    if (isQuestionMarkHeavy(s)) return "";
    if (isLikelyCorruptedText(s)) {
      s = s.replace(/\uFFFD/g, "").replace(/[^\x09\x0A\x0D\x20-\x7E\u4E00-\u9FFF。，、；：？！“”‘’（）【】《》—…·\-_/:%.,()]/g, "");
    }
    s = s.replace(/\s+/g, " ").trim();
    if (s.length > maxLen) return `${s.slice(0, maxLen)}...`;
    return s;
  }

  function toBulletList(items, prefix = "• ") {
    return (items || []).map((x) => `${prefix}${cleanOfficeText(x, 180)}`);
  }

  function paginateBullets(items, pageSize = 6) {
    const out = [];
    for (let i = 0; i < items.length; i += pageSize) out.push(items.slice(i, i + pageSize));
    return out.length > 0 ? out : [[]];
  }

  function fitRect(srcW, srcH, dstW, dstH) {
    if (!(srcW > 0) || !(srcH > 0) || !(dstW > 0) || !(dstH > 0)) return { w: dstW, h: dstH };
    const r = Math.min(dstW / srcW, dstH / srcH);
    return { w: srcW * r, h: srcH * r };
  }

  function fitTableBodyRowCount(maxHeight, opts = {}) {
    const headerRows = Number.isFinite(Number(opts.header_rows)) ? Math.max(0, Math.floor(Number(opts.header_rows))) : 1;
    const rowHeight = Number.isFinite(Number(opts.row_height)) ? Math.max(0.15, Number(opts.row_height)) : 0.42;
    const padTop = Number.isFinite(Number(opts.pad_top)) ? Math.max(0, Number(opts.pad_top)) : 0.06;
    const padBottom = Number.isFinite(Number(opts.pad_bottom)) ? Math.max(0, Number(opts.pad_bottom)) : 0.06;
    const avail = Math.max(0, Number(maxHeight || 0) - padTop - padBottom - headerRows * rowHeight);
    return Math.max(1, Math.floor(avail / rowHeight));
  }

  function resolveVisualPack(theme, variant = {}) {
    const isStrong = !!variant.isStrong;
    const isVibrant = !!variant.isVibrant;
    const primary = isStrong ? "005FB8" : (isVibrant ? "0A66C2" : String(theme?.primary || "0F6CBD"));
    const secondary = isStrong ? "083B7A" : (isVibrant ? "0078D4" : String(theme?.secondary || primary || "115EA3"));
    return {
      accent_primary: primary,
      accent_secondary: secondary,
      panel_fill: isStrong ? "FFE3F0FE" : (isVibrant ? "FFDDEBFF" : "FFEAF3FB"),
      card_fill: isStrong ? "FFEAF3FF" : (isVibrant ? "FFEAF2FF" : "FFF2F8FF"),
      border: isStrong ? "FFBFD3EA" : (isVibrant ? "FFB3CEEF" : "FFD6E4F5"),
      zebra: isStrong ? "FFF2F7FD" : (isVibrant ? "FFEFF5FF" : "FFF7FAFD"),
      chrome_fill: isStrong ? "F6FAFF" : (isVibrant ? "F2F7FF" : "FFFFFF"),
      chrome_transparency: isStrong ? 12 : (isVibrant ? 6 : 24),
      chrome_line: isStrong ? "C7DCF4" : (isVibrant ? "B8D3F2" : "DCEBFA"),
    };
  }

  function pickIllustrationImage(rows, options = {}) {
    const fromInputFiles = () => {
      const raw = options && options.input_files;
      if (!raw) return "";
      try {
        const arr = Array.isArray(raw) ? raw : JSON.parse(String(raw));
        if (!Array.isArray(arr)) return "";
        for (const p0 of arr) {
          const p = String(p0 || "");
          const ext = pathModule.extname(p).toLowerCase();
          if (![".png", ".jpg", ".jpeg", ".bmp", ".webp"].includes(ext)) continue;
          if (fs.existsSync(p)) return p;
        }
      } catch {}
      return "";
    };

    const viaInput = fromInputFiles();
    if (viaInput) return viaInput;

    for (const r of rows || []) {
      const p = String(r.source_file || "");
      if (!p) continue;
      const ext = pathModule.extname(p).toLowerCase();
      if (![".png", ".jpg", ".jpeg", ".bmp", ".webp"].includes(ext)) continue;
      if (fs.existsSync(p)) return p;
    }
    return "";
  }

  function resolveBundledFluentChartImage() {
    const cands = [
      pathModule.join(__dirname, "assets", "fluent_dashboard_chart.png"),
      pathModule.join(__dirname, "..", "assets", "fluent_dashboard_chart.png"),
      pathModule.join(__dirname, "..", "..", "apps", "dify-desktop", "assets", "fluent_dashboard_chart.png"),
    ];
    for (const p of cands) {
      try {
        if (fs.existsSync(p)) return p;
      } catch {}
    }
    return "";
  }

  function unionColumns(rows) {
    const pref = ["source_file", "source_type", "row_no", "id", "text", "amount"];
    const set = new Set();
    rows.forEach((r) => Object.keys(r).forEach((k) => set.add(k)));
    const tail = Array.from(set).filter((k) => !pref.includes(k)).sort();
    return [...pref.filter((k) => set.has(k)), ...tail];
  }

  function isDebateStyleTemplate(options = {}) {
    const t = String(options.cleaning_template || "").trim().toLowerCase();
    return t === "debate_evidence_v1";
  }

  function parsePriorityList(input) {
    return String(input || "")
      .split(/[,\n]/)
      .map((x) => x.trim())
      .filter(Boolean);
  }

  function parseBattlefieldRules(input) {
    const lines = String(input || "")
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter(Boolean);
    const out = [];
    lines.forEach((line) => {
      const idx = line.indexOf("=");
      if (idx <= 0) return;
      const keyword = line.slice(0, idx).trim();
      const label = line.slice(idx + 1).trim();
      if (!keyword || !label) return;
      out.push({ keyword, label });
    });
    return out;
  }

  function parseClaimRules(input) {
    const lines = String(input || "")
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter(Boolean);
    const out = [];
    lines.forEach((line) => {
      const idx = line.indexOf("=");
      if (idx <= 0) return;
      const keyword = line.slice(0, idx).trim();
      const claim = line.slice(idx + 1).trim();
      if (!keyword || !claim) return;
      out.push({ keyword, claim });
    });
    return out;
  }

  function pickSourceUrl(row, options = {}) {
    const cands = [
      ...parsePriorityList(options.debate_source_priority_url),
      "source_url", "url", "link", "source_link", "ref_url",
    ];
    for (const k of cands) {
      const v = String(row && row[k] !== undefined ? row[k] : "").trim();
      if (v) return v;
    }
    return "";
  }

  function pickSourceOrg(row, options = {}) {
    const cands = [
      ...parsePriorityList(options.debate_source_priority),
      "source_org", "publisher", "source", "author", "source_type",
    ];
    for (const k of cands) {
      const v = String(row && row[k] !== undefined ? row[k] : "").trim();
      if (v) return v;
    }
    return pathModule.basename(String(row?.source_file || "")) || "未标注";
  }

  function inferBattlefield(text, options = {}) {
    const s = String(text || "");
    const rules = parseBattlefieldRules(options.debate_battlefield_rules);
    for (const r of rules) {
      if (s.includes(r.keyword)) return r.label;
    }
    const m = s.match(/^[\[【](.{2,20})[\]】]/);
    if (m) return m[1].trim();
    if (/[谣言|真假|信息]/.test(s)) return "内容差";
    if (/[成瘾|沉迷|自制力]/.test(s)) return "能力减损";
    if (/[社交|交流|表达]/.test(s)) return "社交与表达";
    return "论点支撑";
  }

  function inferConclusion(text) {
    const s = normalizeLineText(text || "", 220);
    if (!s) return "结论待补充";
    const m = s.match(/^(.{8,42}?)[，。；;！!？?]/);
    return (m && m[1]) ? m[1] : s.slice(0, 34);
  }

  function inferExample(text) {
    const s = normalizeLineText(text || "", 420);
    if (!s) return "";
    const parts = s.split(/[。；;!?！？]/).map((x) => x.trim()).filter(Boolean);
    if (parts.length <= 1) return s.slice(0, 180);
    return parts.slice(1, 3).join("；").slice(0, 220);
  }

  function inferEvidenceType(text) {
    const s = String(text || "");
    if (/%|样本|调查|数据显示|研究显示|统计|回归|显著|p值|实验|数据/.test(s)) return "实证";
    if (/理论|模型|机制|框架|范式|原理|假设|推导/.test(s)) return "理论";
    if (/案例|事件|平台|通报|法院|判决|某年|某地|视频/.test(s)) return "案例";
    if (/应当|应该|规范|原则|伦理|法律|制度|价值/.test(s)) return "规范";
    return "实证";
  }

  function inferSupportClaim(text, battlefield, options = {}) {
    const claimRules = parseClaimRules(options.debate_claim_rules);
    for (const r of claimRules) {
      if (String(text || "").includes(r.keyword)) return r.claim;
    }
    const bf = String(battlefield || "").trim();
    if (bf && bf !== "论点支撑") return bf;
    return String(options.debate_default_claim || "核心论点").trim() || "核心论点";
  }

  function computeRelevanceScore(text, supportClaim, evidenceType, options = {}) {
    const t = String(text || "");
    const claim = String(supportClaim || "");
    let score = 40;
    const nums = t.match(/[-+]?\d+(?:\.\d+)?%?/g) || [];
    const numSignal = Math.min(20, nums.length * 3);
    score += numSignal;
    if (claim) {
      const segs = claim.split(/[、，,；;\s]+/).map((x) => x.trim()).filter((x) => x.length >= 2);
      let hit = 0;
      segs.forEach((k) => { if (t.includes(k)) hit += 1; });
      score += Math.min(25, hit * 8);
    }
    if (evidenceType === "实证") score += 10;
    else if (evidenceType === "理论") score += 8;
    else if (evidenceType === "案例") score += 6;
    else score += 5;
    const sourceWeight = Number(options.debate_source_weight || 0);
    if (Number.isFinite(sourceWeight)) score += Math.max(0, Math.min(5, sourceWeight));
    return Math.max(0, Math.min(100, Math.round(score)));
  }

  function buildDebateTableRows(rows = [], options = {}) {
    const out = [];
    const used = new Set();
    for (const r of rows || []) {
      const text = normalizeLineText(rowTextForQuality(r), 560);
      if (!text || text.length < 16) continue;
      const key = text.slice(0, 160).toLowerCase();
      if (used.has(key)) continue;
      used.add(key);
      const battlefield = inferBattlefield(text, options);
      const evidenceType = inferEvidenceType(text);
      const supportClaim = inferSupportClaim(text, battlefield, options);
      const relevanceScore = computeRelevanceScore(text, supportClaim, evidenceType, options);
      out.push({
        battlefield,
        support_claim: supportClaim,
        evidence_type: evidenceType,
        relevance_score: relevanceScore,
        conclusion: inferConclusion(text),
        content: text,
        source_org: pickSourceOrg(r, options),
        source_url: pickSourceUrl(r, options),
        example: inferExample(text),
      });
      if (out.length >= 300) break;
    }
    out.sort((a, b) => {
      const c = String(a.support_claim || "").localeCompare(String(b.support_claim || ""), "zh-Hans-CN");
      if (c !== 0) return c;
      const t = String(a.evidence_type || "").localeCompare(String(b.evidence_type || ""), "zh-Hans-CN");
      if (t !== 0) return t;
      return Number(b.relevance_score || 0) - Number(a.relevance_score || 0);
    });
    return out;
  }

  function groupDebateRowsByBattlefield(rows = []) {
    const groups = new Map();
    for (const r of rows || []) {
      const k = String(r?.battlefield || "论点支撑").trim() || "论点支撑";
      if (!groups.has(k)) groups.set(k, []);
      groups.get(k).push(r);
    }
    return Array.from(groups.entries()).map(([battlefield, items]) => ({ battlefield, items }));
  }

  function buildDebatePreview(rows = [], options = {}, limit = 10) {
    const n = Math.max(1, Math.min(30, Number(limit || 10)));
    const blocks = buildDebateTableRows(rows, options);
    const groups = groupDebateRowsByBattlefield(blocks).map((g) => ({
      battlefield: g.battlefield,
      count: g.items.length,
    }));
    return {
      total: blocks.length,
      groups,
      rows: blocks.slice(0, n),
    };
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
      .filter((r) => !isLikelyCorruptedText(rowTextForQuality(r)))
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
      const text = cleanOfficeText(rowTextForQuality(r), 240);
      if (text.length < 40) continue;
      if (isLikelyCorruptedText(text)) continue;
      if (isQuestionMarkHeavy(text)) continue;
      if (looksLikeReferenceEntry(text)) continue;
      const key = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
      if (!key || key.length < 20 || seen.has(key)) continue;
      const src = pathModule.basename(String(r.source_file || ""));
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
        const text = cleanOfficeText(rowTextForQuality(r), 240);
        if (text.length < 40 || isLikelyCorruptedText(text) || looksLikeReferenceEntry(text)) continue;
        if (isQuestionMarkHeavy(text)) continue;
        const key = text.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!key || key.length < 20 || seen.has(key)) continue;
        seen.add(key);
        out.push({
          source: pathModule.basename(String(r.source_file || "")),
          text: text.length > 180 ? `${text.slice(0, 180)}...` : text,
        });
        if (out.length >= maxItems) break;
      }
    }
    return out;
  }

  return {
    isQuestionMarkHeavy,
    filterRowsForOffice,
    looksLikeTitleLine,
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
    parsePriorityList,
    parseBattlefieldRules,
    parseClaimRules,
    pickSourceUrl,
    pickSourceOrg,
    inferBattlefield,
    inferConclusion,
    inferExample,
    inferEvidenceType,
    inferSupportClaim,
    computeRelevanceScore,
    buildDebateTableRows,
    groupDebateRowsByBattlefield,
    buildDebatePreview,
    buildDataQualityInsights,
    buildEvidenceHighlights,
  };
}

module.exports = {
  createOfflineOutputAnalysis,
};
