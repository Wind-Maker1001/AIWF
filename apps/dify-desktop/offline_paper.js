const fs = require("fs");
const path = require("path");
const crypto = require("crypto");

function loadPaperCleanProfile() {
  const defaults = {
    stop_section_titles: [
      "references",
      "bibliography",
      "works cited",
      "acknowledgments",
      "appendix",
      "notes",
      "参考文献",
      "引用文献",
      "文献目录",
      "致谢",
      "附录",
      "注释",
      "脚注",
    ],
    noise_prefixes: ["doi:", "收稿日期", "基金项目", "通讯作者"],
    inline_citation_patterns: [
      "\\[\\s*\\d+(?:\\s*[-,，]\\s*\\d+)*\\s*\\]",
      "（[^（）]{0,48}\\d{4}[a-z]?(?:[;；，,][^（）]{0,48})?）",
      "\\([^()]{0,48}\\d{4}[a-z]?(?:[;,][^()]{0,48})?\\)",
    ],
  };
  const candidate = path.join(__dirname, "..", "..", "rules", "templates", "paper_clean_profile.json");
  try {
    if (!fs.existsSync(candidate)) return defaults;
    const raw = JSON.parse(fs.readFileSync(candidate, "utf8"));
    if (!raw || typeof raw !== "object") return defaults;
    const out = { ...defaults };
    if (Array.isArray(raw.stop_section_titles)) {
      out.stop_section_titles = raw.stop_section_titles
        .map((x) => String(x || "").toLowerCase().trim())
        .filter(Boolean);
    }
    if (Array.isArray(raw.noise_prefixes)) {
      out.noise_prefixes = raw.noise_prefixes
        .map((x) => String(x || "").trim())
        .filter(Boolean);
    }
    if (Array.isArray(raw.inline_citation_patterns)) {
      out.inline_citation_patterns = raw.inline_citation_patterns
        .map((x) => String(x || ""))
        .filter(Boolean);
    }
    return out;
  } catch {
    return defaults;
  }
}

const PAPER_CLEAN_PROFILE = loadPaperCleanProfile();

function normalizeLineText(s) {
  return String(s || "").replace(/\s+/g, " ").trim();
}

function isLikelyNoiseLine(line) {
  const s = normalizeLineText(line);
  if (!s) return true;
  if (s.length <= 1) return true;
  if (/^\d{1,4}$/.test(s)) return true;
  if (/^(page|页)\s*\d+$/i.test(s)) return true;
  if (/^--\s*\d+\s*of\s*\d+\s*--$/i.test(s)) return true;
  if (/^doi[:\s]/i.test(s)) return true;
  if (PAPER_CLEAN_PROFILE.noise_prefixes.some((p) => p && s.startsWith(p))) return true;
  const letters = (s.match(/[A-Za-z\u4e00-\u9fff]/g) || []).length;
  const symbols = (s.match(/[^A-Za-z0-9\u4e00-\u9fff\s]/g) || []).length;
  if (letters === 0 && symbols > 0) return true;
  if (symbols > letters * 2 && s.length < 24) return true;
  return false;
}

function splitPdfTextToEvidence(text) {
  const lines = String(text || "")
    .split(/\r?\n/)
    .map(normalizeLineText)
    .filter((x) => x && !isLikelyNoiseLine(x));

  const paras = [];
  let buf = "";
  const flush = () => {
    const out = normalizeLineText(buf);
    buf = "";
    if (!out) return;
    if (out.length < 18) return;
    paras.push(out);
  };

  for (const ln of lines) {
    const joiner = /[\u4e00-\u9fff]$/.test(buf) || /^[\u4e00-\u9fff]/.test(ln) ? "" : " ";
    buf = buf ? `${buf}${joiner}${ln}` : ln;
    if (/[。！？!?；;.]$/.test(ln) || buf.length > 260) flush();
  }
  flush();

  const uniq = [];
  const seen = new Set();
  for (const p of paras) {
    const key = p.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
    if (!key || key.length < 10) continue;
    if (seen.has(key)) continue;
    seen.add(key);
    uniq.push(p);
  }
  return uniq.slice(0, 5000);
}

function textCorruptionStats(text) {
  const s = String(text || "");
  const total = s.length || 1;
  const cjk = (s.match(/[\u4e00-\u9fff]/g) || []).length;
  const latin = (s.match(/[A-Za-z]/g) || []).length;
  const digits = (s.match(/[0-9]/g) || []).length;
  const ctrl = (s.match(/[\x00-\x08\x0B-\x1F\x7F-\x9F]/g) || []).length;
  const replacement = (s.match(/\uFFFD/g) || []).length;
  const mojibake = (s.match(/[ÃÂ¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]/g) || []).length;
  const weirdHan = (s.match(/[聙聛聜聝聞聟聠聡聢聣聤聥聦聧聨聩聪]/g) || []).length;
  const symbol = (s.match(/[^A-Za-z0-9\u4e00-\u9fff\s]/g) || []).length;
  const readable = cjk + latin + Math.floor(digits * 0.2);
  return {
    total,
    cjk,
    latin,
    digits,
    ctrl,
    replacement,
    mojibake,
    weirdHan,
    symbol,
    readable,
    readableRatio: readable / total,
    ctrlRatio: ctrl / total,
    noiseRatio: (replacement + mojibake + weirdHan + Math.floor(symbol * 0.2)) / total,
  };
}

function isLikelyCorruptedText(text) {
  const st = textCorruptionStats(text);
  if (!String(text || "").trim()) return true;
  if (st.ctrlRatio > 0.002) return true;
  if (st.noiseRatio > 0.22 && st.readableRatio < 0.3) return true;
  if (st.readableRatio < 0.14 && st.symbol > st.readable * 1.2) return true;
  return false;
}

function isReferenceSectionTitle(line) {
  const s = normalizeLineText(line).toLowerCase();
  if (!s) return false;
  if (PAPER_CLEAN_PROFILE.stop_section_titles.includes(s)) return true;
  return /^(references?|bibliography|works cited)(\b|[:：\s].*)$/.test(s)
    || /^(参考文献|引用文献|文献目录).*$/.test(s);
}

function isNonBodySectionTitle(line) {
  const s = normalizeLineText(line).toLowerCase();
  if (!s) return false;
  if (PAPER_CLEAN_PROFILE.stop_section_titles.includes(s)) return true;
  return /^(acknowledg?ments?|appendix|notes)(\b|[:：\s].*)$/.test(s)
    || /^(致谢|附录|注释|脚注)([:：\s].*)?$/.test(s);
}

function looksLikeReferenceEntry(line) {
  const s = normalizeLineText(line);
  if (!s) return false;
  if (isReferenceSectionTitle(s)) return true;
  if (/^\[\d{1,3}([,-]\d{1,3})?\]/.test(s)) return true;
  if (/^\(?\d{1,3}\)?[.)]\s/.test(s) && /\b\d{4}[a-z]?\b/.test(s)) return true;
  if (/\bdoi[:\s]/i.test(s) || /https?:\/\//i.test(s)) return true;
  if (/\b\d{4}[a-z]?\b/.test(s) && /(journal|vol\.?|no\.?|pp\.?|出版社|学报|期刊|会议|press|大学出版社)/i.test(s)) return true;
  if (/(et al\.?|等)\s*,?\s*\d{4}/i.test(s)) return true;
  return false;
}

function stripInlineCitations(text) {
  let s = String(text || "");
  for (const patt of PAPER_CLEAN_PROFILE.inline_citation_patterns) {
    try {
      s = s.replace(new RegExp(patt, "g"), "");
    } catch {}
  }
  return normalizeLineText(s);
}

function normalizeAcademicLine(text) {
  let s = normalizeLineText(text);
  // Remove standalone footnote markers and common superscript-like numeric tails.
  s = s.replace(/\s*[†‡*]+\s*$/g, "");
  s = s.replace(/\s*\(\s*\d{1,2}\s*\)\s*$/g, "");
  s = s.replace(/\s*\[\s*\d{1,2}\s*\]\s*$/g, "");
  // Repair simple line-wrap hyphenation in OCR/PDF extraction.
  s = s.replace(/([A-Za-z])-\s+([A-Za-z])/g, "$1$2");
  // Drop obvious editorial noise.
  s = s.replace(/\b(preprint|accepted manuscript|all rights reserved)\b/gi, "");
  return normalizeLineText(s);
}

function cleanAcademicChunksDetailed(chunks, filePath) {
  const srcRaw = Array.isArray(chunks) ? chunks : [];
  const src = [];
  // Merge line-broken paragraphs produced by PDF/OCR extraction.
  for (let i = 0; i < srcRaw.length; i += 1) {
    let cur = normalizeAcademicLine(srcRaw[i]);
    if (!cur) continue;
    while (i + 1 < srcRaw.length) {
      const next = normalizeAcademicLine(srcRaw[i + 1]);
      if (!next) {
        i += 1;
        continue;
      }
      const curLooksHeading = isReferenceSectionTitle(cur) || isNonBodySectionTitle(cur) || /^(\d+(\.\d+){0,3}\s+|[（(]?[一二三四五六七八九十]+[)）、.]\s*)/.test(cur);
      const nextLooksHeading = isReferenceSectionTitle(next) || isNonBodySectionTitle(next) || /^(\d+(\.\d+){0,3}\s+|[（(]?[一二三四五六七八九十]+[)）、.]\s*)/.test(next);
      const curEnds = /[。！？!?；;:]$/.test(cur);
      const shouldJoin = !curLooksHeading && !nextLooksHeading && !curEnds && cur.length <= 140 && next.length <= 140;
      if (!shouldJoin) break;
      cur = normalizeLineText(`${cur} ${next}`);
      i += 1;
    }
    src.push(cur);
  }
  const cleaned = [];
  const stats = {
    source_file: filePath,
    raw_chunks: src.length,
    kept_chunks: 0,
    removed_reference_lines: 0,
    removed_noise_lines: 0,
    removed_short_lines: 0,
    stop_section_hit: false,
    inline_citation_cleaned_lines: 0,
  };
  let stopBody = false;
  for (const raw of src) {
    if (stopBody) break;
    const line = normalizeAcademicLine(raw);
    if (!line) continue;
    if (isReferenceSectionTitle(line) || isNonBodySectionTitle(line)) {
      stats.stop_section_hit = true;
      stopBody = true;
      continue;
    }
    if (looksLikeReferenceEntry(line)) {
      stats.removed_reference_lines += 1;
      continue;
    }
    const stripped = normalizeAcademicLine(stripInlineCitations(line));
    if (stripped !== line) stats.inline_citation_cleaned_lines += 1;
    if (!stripped) continue;
    if (isLikelyNoiseLine(stripped)) {
      stats.removed_noise_lines += 1;
      continue;
    }
    if (stripped.length < 10) {
      stats.removed_short_lines += 1;
      continue;
    }
    cleaned.push(stripped);
  }
  const dedup = [];
  const seen = new Set();
  for (const x of cleaned) {
    const key = x.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
    if (!key || key.length < 12 || seen.has(key)) continue;
    seen.add(key);
    dedup.push(x);
  }
  if (dedup.length > 0) {
    stats.kept_chunks = dedup.length;
    return { chunks: dedup, stats };
  }
  const fallback = src
    .map((x) => normalizeAcademicLine(stripInlineCitations(x)))
    .filter((x) => x && x.length >= 10)
    .slice(0, 5000);
  stats.kept_chunks = fallback.length;
  return { chunks: fallback, stats };
}

function cleanAcademicChunks(chunks, filePath) {
  return cleanAcademicChunksDetailed(chunks, filePath).chunks;
}

function pickPaperTitle(chunks, filePath) {
  const fallback = path.basename(filePath, path.extname(filePath));
  const cands = [];
  (chunks || []).slice(0, 40).forEach((raw, i) => {
    const s = normalizeLineText(raw);
    if (!s) return;
    if (isLikelyNoiseLine(s) || looksLikeReferenceEntry(s)) return;
    if (s.length < 6 || s.length > 120) return;
    if (/^(摘要|abstract|关键词|keywords|introduction|引言)[:：]?$/i.test(s)) return;
    const stats = textCorruptionStats(s);
    if (isLikelyCorruptedText(s)) return;
    let score = 0;
    if (i <= 3) score += 5;
    if (s.length >= 10 && s.length <= 48) score += 4;
    if (!/[。！？!?;；]$/.test(s)) score += 2;
    if (/幸福|研究|subjective|well-being|emotion|psychology|心理/i.test(s)) score += 2;
    score += Math.min(2, stats.readableRatio * 2);
    cands.push({ s, score });
  });
  cands.sort((a, b) => b.score - a.score);
  if (cands[0] && cands[0].score >= 5) return cands[0].s;
  for (const raw of chunks || []) {
    const s = normalizeLineText(raw);
    if (s && s.length >= 8 && s.length <= 80 && !isLikelyCorruptedText(s)) return s;
  }
  return fallback;
}

function scorePdfExtractText(text) {
  const s = String(text || "");
  if (!s.trim()) return -999;
  const st = textCorruptionStats(s);
  const pageMarks = (s.match(/--\s*\d+\s*of\s*\d+\s*--/gi) || []).length;
  let score = st.readableRatio * 100;
  score -= pageMarks * 1.6;
  score -= st.ctrl * 10;
  score -= st.replacement * 6;
  score -= st.mojibake * 2;
  score -= st.weirdHan * 1.5;
  return score;
}

function shortHash(input) {
  return crypto.createHash("md5").update(String(input || "")).digest("hex").slice(0, 10);
}

function safeFileStem(name) {
  return String(name || "")
    .replace(/[<>:"/\\|?*\x00-\x1f]/g, "_")
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_")
    .replace(/^_+|_+$/g, "")
    .slice(0, 80) || "pdf";
}

function buildPaperMarkdown(filePath, sourceType, chunks) {
  const body = cleanAcademicChunks(chunks, filePath);
  const title = pickPaperTitle(body, filePath);
  const detectHeadingLevel = (line) => {
    const s = normalizeLineText(line);
    if (!s || s.length > 80) return 0;
    if (/^(摘要|abstract|引言|introduction|结论|conclusion|讨论|discussion|方法|method|results?|结果|背景|background)[:：]?$/i.test(s)) return 2;
    if (/^\d+(\.\d+){0,3}\s+/.test(s)) {
      const depth = (s.match(/\./g) || []).length + 2;
      return Math.min(4, Math.max(2, depth));
    }
    if (/^[（(]?[一二三四五六七八九十]+[)）、.]\s*/.test(s)) return 3;
    if (/^[A-Z][A-Z\s]{4,40}$/.test(s)) return 2;
    return 0;
  };
  const lines = [];
  lines.push(`# ${title}`);
  lines.push("");
  lines.push(`- source_file: ${filePath}`);
  lines.push(`- source_type: ${sourceType}`);
  lines.push(`- chunk_count: ${body.length}`);
  lines.push("");
  lines.push("## Body");
  let prevHeading = false;
  for (const c of body) {
    const t = normalizeLineText(c);
    if (!t) continue;
    const lvl = detectHeadingLevel(t);
    if (lvl > 0) {
      lines.push(`${"#".repeat(lvl)} ${t.replace(/^(\d+(\.\d+){0,3}\s+|[（(]?[一二三四五六七八九十]+[)）、.]\s*)/, "").trim()}`);
      lines.push("");
      prevHeading = true;
      continue;
    }
    if (prevHeading) {
      lines.push(t);
      lines.push("");
      prevHeading = false;
      continue;
    }
    lines.push(`- ${t}`);
  }
  lines.push("");
  return `\uFEFF${lines.join("\n")}\n`;
}

function chunksFromMarkdown(mdText) {
  return String(mdText || "")
    .replace(/^\uFEFF/, "")
    .split(/\r?\n/)
    .map((line) => line.replace(/^\s*[-*]\s+/, "").replace(/^#{1,6}\s+/, "").trim())
    .filter((line) => line && !/^source_(file|type):/i.test(line) && !/^chunk_count:/i.test(line) && !/^body$/i.test(line))
    .map(normalizeLineText)
    .filter((x) => x.length >= 6);
}

function materializePaperMarkdown(filePath, sourceType, chunks, runtime, params = {}) {
  const enabled = params.paper_markdown_enabled !== false && params.pdf_markdown_enabled !== false;
  const detail = cleanAcademicChunksDetailed(chunks, filePath);
  const cleanedChunks = detail.chunks;
  if (!enabled || !runtime || !runtime.paperMdDir) return cleanedChunks;
  fs.mkdirSync(runtime.paperMdDir, { recursive: true });
  const base = safeFileStem(path.basename(filePath, path.extname(filePath)));
  const mdName = `${base}_${shortHash(filePath)}.md`;
  const mdPath = path.join(runtime.paperMdDir, mdName);
  const md = buildPaperMarkdown(filePath, sourceType, cleanedChunks);
  fs.writeFileSync(mdPath, md, "utf8");

  if (Array.isArray(runtime.paperMdRecords)) {
    runtime.paperMdRecords.push({
      source_file: filePath,
      source_type: sourceType,
      chunk_count: chunksFromMarkdown(md).length,
      raw_chunk_count: Number(detail?.stats?.raw_chunks || 0),
      removed_reference_lines: Number(detail?.stats?.removed_reference_lines || 0),
      removed_noise_lines: Number(detail?.stats?.removed_noise_lines || 0),
      inline_citation_cleaned_lines: Number(detail?.stats?.inline_citation_cleaned_lines || 0),
      stop_section_hit: !!detail?.stats?.stop_section_hit,
      path: mdPath,
    });
  }

  return chunksFromMarkdown(md);
}

function writePaperMarkdownIndex(filePath, records) {
  const lines = [];
  lines.push("# 论文 Markdown 中间产物索引（PDF/DOCX）");
  lines.push("");
  if (!records || records.length === 0) {
    lines.push("- 本次任务未生成论文 Markdown 中间产物。");
  } else {
    records.forEach((r, i) => {
      lines.push(`## ${i + 1}. ${path.basename(String(r.source_file || ""))}`);
      lines.push(`- source_type: ${r.source_type}`);
      lines.push(`- chunk_count: ${r.chunk_count}`);
      lines.push(`- markdown: ${r.path}`);
      lines.push("");
    });
  }
  fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
}

function writeAiCorpusMarkdown(filePath, records) {
  const lines = [];
  lines.push("# AI 备用语料（论文正文）");
  lines.push("");
  if (!Array.isArray(records) || records.length === 0) {
    lines.push("- 无论文 Markdown 数据。");
    fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
    return;
  }
  for (const r of records) {
    const src = String(r.path || "");
    if (!src || !fs.existsSync(src)) continue;
    const raw = fs.readFileSync(src, "utf8").replace(/^\uFEFF/, "");
    const fileName = path.basename(String(r.source_file || ""));
    lines.push(`## ${fileName}`);
    lines.push("");
    raw.split(/\r?\n/).forEach((ln) => {
      const s = String(ln || "").trimEnd();
      if (!s) return;
      if (s.startsWith("- source_file:")) return;
      if (s.startsWith("- source_type:")) return;
      if (s.startsWith("- chunk_count:")) return;
      lines.push(s);
    });
    lines.push("");
  }
  fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
}

function writeQualityReport(filePath, rows, warnings, records, options = {}) {
  const paperRecords = Array.isArray(options?.paperRecords) ? options.paperRecords : [];
  const fidelity = options?.fidelity || {};
  const qualityScore = options?.qualityScore && typeof options.qualityScore === "object"
    ? options.qualityScore
    : null;
  const contentQuality = options?.contentQuality && typeof options.contentQuality === "object"
    ? options.contentQuality
    : null;
  const outputGateMeta = options?.outputGateMeta && typeof options.outputGateMeta === "object"
    ? options.outputGateMeta
    : null;
  const totalRows = Array.isArray(rows) ? rows.length : 0;
  const badRows = (rows || []).filter((r) => isLikelyCorruptedText(String(r.text || "")));
  const gibberishRatio = totalRows > 0 ? badRows.length / totalRows : 0;
  const rawChunks = paperRecords.reduce((a, r) => a + Number(r.raw_chunk_count || 0), 0);
  const keptChunks = paperRecords.reduce((a, r) => a + Number(r.chunk_count || 0), 0);
  const refRemoved = paperRecords.reduce((a, r) => a + Number(r.removed_reference_lines || 0), 0);
  const citeCleaned = paperRecords.reduce((a, r) => a + Number(r.inline_citation_cleaned_lines || 0), 0);
  const stopHitCount = paperRecords.reduce((a, r) => a + (r.stop_section_hit ? 1 : 0), 0);
  const extractionRate = rawChunks > 0 ? keptChunks / rawChunks : 1;
  const referencePruneRate = rawChunks > 0 ? refRemoved / rawChunks : 0;
  const sectionIntegrityRate = paperRecords.length > 0 ? stopHitCount / paperRecords.length : 1;

  const lines = [];
  lines.push("# 质量体检报告");
  lines.push("");
  lines.push(`- 行数: ${totalRows}`);
  lines.push(`- 告警数: ${Array.isArray(warnings) ? warnings.length : 0}`);
  lines.push(`- 模式: ${fidelity.enabled ? "text_fidelity" : "standard"}`);
  if (qualityScore) {
    lines.push(`- 总体质量分(0-100): ${Number(qualityScore.score || 0).toFixed(1)} (${String(qualityScore.level || "unknown")})`);
  }
  if (contentQuality) {
    lines.push(`- 内容质量分(0-100): ${Number(contentQuality.score || 0).toFixed(1)} (${String(contentQuality.level || "unknown")})`);
    lines.push(`- 内容门禁: ${contentQuality.pass ? "通过" : "未通过"}`);
  }
  if (outputGateMeta && Number.isFinite(Number(outputGateMeta.filtered_question_mark_rows || 0))) {
    lines.push(`- 问号密集过滤行: ${Number(outputGateMeta.filtered_question_mark_rows || 0)}`);
  }
  if (fidelity.enabled && Array.isArray(fidelity.reasons) && fidelity.reasons.length > 0) {
    lines.push(`- 降级原因: ${fidelity.reasons.join("; ")}`);
  }
  lines.push("");
  lines.push("## Markdown 质量指标");
  lines.push(`- 抽取率(extraction_rate): ${(extractionRate * 100).toFixed(1)}%`);
  lines.push(`- 乱码疑似率(gibberish_ratio): ${(gibberishRatio * 100).toFixed(1)}%`);
  lines.push(`- 参考/注释剔除率(reference_prune_rate): ${(referencePruneRate * 100).toFixed(1)}%`);
  lines.push(`- 引文清洗触发率(citation_cleaned_rate): ${rawChunks > 0 ? ((citeCleaned / rawChunks) * 100).toFixed(1) : "0.0"}%`);
  lines.push(`- 章节完整度(section_integrity_rate): ${(sectionIntegrityRate * 100).toFixed(1)}%`);
  if (contentQuality && contentQuality.metrics) {
    lines.push(`- 标题覆盖率(title_coverage): ${(Number(contentQuality.metrics.title_coverage || 0) * 100).toFixed(1)}%`);
    lines.push(`- 段落覆盖率(paragraph_coverage): ${(Number(contentQuality.metrics.paragraph_coverage || 0) * 100).toFixed(1)}%`);
    lines.push(`- 段落连贯率(coherent_paragraph_ratio): ${(Number(contentQuality.metrics.coherent_paragraph_ratio || 0) * 100).toFixed(1)}%`);
    lines.push(`- 数字一致性(numeric_consistency): ${(Number(contentQuality.metrics.numeric_consistency || 0) * 100).toFixed(1)}%`);
  }
  lines.push("");
  lines.push("## 文件级检查");
  const hasLayerRecords = Array.isArray(records) && records.length > 0;
  const hasPaperRecords = Array.isArray(paperRecords) && paperRecords.length > 0;
  if (!hasLayerRecords && !hasPaperRecords) {
    lines.push("- 无文件级质量记录。");
  } else {
    if (hasLayerRecords) {
      records.forEach((r, i) => {
        lines.push(`### ${i + 1}. ${path.basename(String(r.source_file || ""))} (${r.source_type || "unknown"})`);
        lines.push(`- quality_score: ${r.quality_score}`);
        lines.push(`- text_chars: ${r.text_chars}`);
        lines.push(`- chunks: ${r.chunks}`);
        lines.push(`- corrupted: ${r.corrupted ? "yes" : "no"}`);
        lines.push("");
      });
    }
    if (hasPaperRecords) {
      paperRecords.forEach((r, i) => {
        const raw = Number(r.raw_chunk_count || 0);
        const kept = Number(r.chunk_count || 0);
        const keepRate = raw > 0 ? (kept / raw) : 1;
        lines.push(`### ${i + 1}. ${path.basename(String(r.source_file || ""))} (paper_markdown)`);
        lines.push(`- raw_chunks: ${raw}`);
        lines.push(`- kept_chunks: ${kept}`);
        lines.push(`- keep_rate: ${(keepRate * 100).toFixed(1)}%`);
        lines.push(`- removed_reference_lines: ${Number(r.removed_reference_lines || 0)}`);
        lines.push(`- removed_noise_lines: ${Number(r.removed_noise_lines || 0)}`);
        lines.push(`- inline_citation_cleaned_lines: ${Number(r.inline_citation_cleaned_lines || 0)}`);
        lines.push(`- stop_section_hit: ${r.stop_section_hit ? "yes" : "no"}`);
        lines.push(`- markdown: ${String(r.path || "")}`);
        lines.push("");
      });
    }
  }
  lines.push("## 行级异常");
  lines.push(`- 乱码疑似行数: ${badRows.length}`);
  badRows.slice(0, 20).forEach((r, i) => {
    lines.push(`${i + 1}. [${path.basename(String(r.source_file || ""))}] ${normalizeLineText(String(r.text || "")).slice(0, 200)}`);
  });
  lines.push("");
  fs.writeFileSync(filePath, `\uFEFF${lines.join("\n")}\n`, "utf8");
}

module.exports = {
  normalizeLineText,
  splitPdfTextToEvidence,
  scorePdfExtractText,
  isLikelyCorruptedText,
  looksLikeReferenceEntry,
  materializePaperMarkdown,
  writePaperMarkdownIndex,
  writeAiCorpusMarkdown,
  writeQualityReport,
};
