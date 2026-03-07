const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");
const {
  normalizeLineText,
  isLikelyCorruptedText,
  looksLikeReferenceEntry,
  rowTextForQuality,
} = require("./offline_paper");

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
const getImageSizeModule = createLazyModuleLoader("image-size");

function createOfflineOutputs({ resolveOfficeTheme, resolveOfficeFont, resolveOfficeLayout }) {
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
          const ext = path.extname(p).toLowerCase();
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
      const ext = path.extname(p).toLowerCase();
      if (![".png", ".jpg", ".jpeg", ".bmp", ".webp"].includes(ext)) continue;
      if (fs.existsSync(p)) return p;
    }
    return "";
  }

  function resolveBundledFluentChartImage() {
    const cands = [
      path.join(__dirname, "assets", "fluent_dashboard_chart.png"),
      path.join(__dirname, "..", "assets", "fluent_dashboard_chart.png"),
      path.join(__dirname, "..", "..", "apps", "dify-desktop", "assets", "fluent_dashboard_chart.png"),
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
    return path.basename(String(row?.source_file || "")) || "未标注";
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
        const text = cleanOfficeText(rowTextForQuality(r), 240);
        if (text.length < 40 || isLikelyCorruptedText(text) || looksLikeReferenceEntry(text)) continue;
        if (isQuestionMarkHeavy(text)) continue;
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

  function toFiniteNumber(v) {
    if (v === null || v === undefined) return null;
    const s = String(v).replace(/[,\s，￥¥$]/g, "").trim();
    if (!s) return null;
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }

  function buildNumericColumnStats(rows = [], maxCols = 4) {
    const list = Array.isArray(rows) ? rows : [];
    if (!list.length) return [];
    const keys = unionColumns(list);
    const out = [];
    for (const k of keys) {
      if (/^(text|content|summary|title|source_file|source_type|id|url|link)$/i.test(k)) continue;
      const vals = [];
      for (const r of list) {
        const n = toFiniteNumber(r?.[k]);
        if (n !== null) vals.push(n);
      }
      if (vals.length < Math.max(6, Math.ceil(list.length * 0.15))) continue;
      const min = Math.min(...vals);
      const max = Math.max(...vals);
      const avg = vals.reduce((a, b) => a + b, 0) / vals.length;
      out.push({
        key: k,
        count: vals.length,
        min: Number(min.toFixed(4)),
        max: Number(max.toFixed(4)),
        avg: Number(avg.toFixed(4)),
      });
    }
    out.sort((a, b) => b.count - a.count);
    return out.slice(0, Math.max(1, Math.min(8, Number(maxCols || 4))));
  }

  function buildNarrativeBullets(rows = [], maxItems = 8) {
    const out = [];
    const seen = new Set();
    const keys = unionColumns(Array.isArray(rows) ? rows : []);
    const textKeys = ["summary", "abstract", "conclusion", "claim", "point", "analysis", "text", "content"]
      .filter((k) => keys.includes(k));
    const scanKeys = textKeys.length ? textKeys : keys.filter((k) => /(summary|abstract|conclusion|claim|point|analysis|text|content|note|remark|观点|结论|摘要)/i.test(k));
    for (const r of rows || []) {
      for (const k of scanKeys) {
        const t = cleanOfficeText(r?.[k] || "", 180);
        if (!t || t.length < 20) continue;
        if (looksLikeReferenceEntry(t) || isLikelyCorruptedText(t) || isQuestionMarkHeavy(t)) continue;
        const sig = t.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!sig || sig.length < 20 || seen.has(sig)) continue;
        seen.add(sig);
        out.push(t);
        if (out.length >= maxItems) return out;
      }
    }
    if (out.length < maxItems) {
      const fallback = buildEvidenceHighlights(rows, maxItems);
      for (const x of fallback) {
        const t = cleanOfficeText(x.text || "", 180);
        if (!t) continue;
        const sig = t.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (!sig || seen.has(sig)) continue;
        seen.add(sig);
        out.push(t);
        if (out.length >= maxItems) break;
      }
    }
    return out;
  }

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
    const src = Array.isArray(gate.source_files) ? gate.source_files.slice(0, 2).map((p) => path.basename(String(p || ""))).filter(Boolean).join(", ") : "";
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
      blocks.forEach((b) => {
        const et = String(b.evidence_type || "实证");
        typeMap.set(et, (typeMap.get(et) || 0) + 1);
        const cl = String(b.support_claim || "核心论点");
        claimMap.set(cl, (claimMap.get(cl) || 0) + 1);
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
      const tocLines = groups.length ? groups.map((g, i) => `${i + 1}. ${g.battlefield}（${g.items.length}）`) : ["1. 论点支撑（0）"];
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

      groups.forEach((g, idx) => {
        const s = pptx.addSlide();
        addFluentChrome(s, `${idx + 1}. ${g.battlefield}`);
        const picks = g.items.slice(0, 4);
        const left = [];
        const right = [];
        picks.forEach((it, i) => {
          left.push(`观点${i + 1}: ${cleanOfficeText(it.support_claim || it.conclusion, 44)}`);
          left.push(`类型/相关性: ${it.evidence_type || "实证"} / ${Number(it.relevance_score || 0)}`);
          left.push(`证据: ${cleanOfficeText(it.content, 90)}`);
          left.push("");
          const srcLine = cleanOfficeText(it.source_org || "未标注", 48);
          const urlLine = cleanOfficeText(it.source_url || "", 88);
          right.push(`${i + 1}) ${srcLine}${urlLine ? `\n${urlLine}` : ""}`);
          right.push("");
        });
        s.addShape(pptx.ShapeType.roundRect, { x: 0.55, y: 1.05, w: 8.15, h: 5.75, fill: { color: "FFFFFF" }, line: { color: "DDE7F2" } });
        s.addShape(pptx.ShapeType.roundRect, { x: 8.9, y: 1.05, w: 3.9, h: 5.75, fill: { color: "FFFFFF" }, line: { color: "DDE7F2" } });
        s.addText(left.join("\n"), { x: 0.78, y: 1.3, w: 7.7, h: 5.25, fontSize: 12, color: "1F2D3D", fontFace, breakLine: true, valign: "top" });
        s.addText(`出处\n\n${right.join("\n")}`, { x: 9.1, y: 1.3, w: 3.45, h: 5.25, fontSize: 11.5, color: "3A4A5E", fontFace, breakLine: true, valign: "top" });
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
    rows.forEach((r) => {
      const k = String(r.source_type || "unknown");
      sourceCount2[k] = (sourceCount2[k] || 0) + 1;
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
    const narr = (narrativeBullets.length ? narrativeBullets : highlights.map((h) => h.text)).slice(0, 6);
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
          numericStats.slice(0, 4).map((s) => [String(s.key), String(s.avg), String(s.min), String(s.max)])
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
    rows.slice(0, sampleRows).forEach((r) => tableRows.push(cols.map((c) => cleanOfficeText(String(r[c] ?? ""), 44))));
    s3.addTable(tableRows, { x: 0.6, y: 1.05, w: 12.0, h: 4.6, border: { pt: 1, color: "DDE7F2" }, fontFace, fontSize: 11 });

    const s4 = pptx.addSlide();
    addFluentChrome(s4, "关键发现");
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

    const evBullets = (highlights.length ? highlights : [{ source: "N/A", text: "未提取到高质量证据摘录，请检查源文件可读性。" }])
      .map((x) => `[${cleanOfficeText(x.source, 40)}] ${cleanOfficeText(x.text, 150)}`);
    const evPages = paginateBullets(evBullets, Math.max(4, Math.min(7, Number(layout.pptx_evidence_page_size || 6))));
    evPages.forEach((page, idx) => {
      const sEvidence = pptx.addSlide();
      addFluentChrome(sEvidence, `证据摘录${evPages.length > 1 ? ` (${idx + 1}/${evPages.length})` : ""}`);
      sEvidence.addText(toBulletList(page).join("\n"), {
        x: 0.6, y: 1.0, w: 12.0, h: 5.7, fontSize: 13.5, color: "1F2D3D", fontFace, breakLine: true, valign: "top",
      });
    });

    if (warnings.length > 0) {
      const warnPages = paginateBullets(warnings.map((w) => cleanOfficeText(w, 140)), 8);
      warnPages.forEach((page, idx) => {
        const sWarn = pptx.addSlide();
        addFluentChrome(sWarn, `告警${warnPages.length > 1 ? ` (${idx + 1}/${warnPages.length})` : ""}`);
        sWarn.addText(`告警${warnPages.length > 1 ? ` (${idx + 1}/${warnPages.length})` : ""}`, { x: 0.5, y: 0.4, w: 4, h: 0.5, fontSize: 22, bold: true, color: "B42318", fontFace });
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
        .map((r) => normalizeLineText(rowTextForQuality(r)))
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
      list.forEach((r, i) => {
        const src = path.basename(String(r?.source_file || ""));
        const no = String(r?.row_no ?? "");
        const text = normalizeLineText(rowTextForQuality(r)).slice(0, 300);
        lines.push(`## ${i + 1}. ${src}${no ? `#${no}` : ""}`);
        lines.push(`- source_type: ${String(r?.source_type || "")}`);
        lines.push(`- text: ${text}`);
        lines.push("");
      });
    }
    fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
  }

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


