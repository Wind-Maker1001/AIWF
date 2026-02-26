const fs = require("fs");
const path = require("path");
const { spawnSync } = require("child_process");

function runPythonJson(script, args = []) {
  const out = spawnSync("python", ["-c", script, ...args], {
    encoding: "utf8",
    windowsHide: true,
    timeout: 20000,
  });
  if (out.status !== 0) {
    const err = String(out.stderr || out.stdout || "").trim();
    throw new Error(`python_failed: ${err || `exit_${out.status}`}`);
  }
  const txt = String(out.stdout || "").trim();
  const line = txt.split(/\r?\n/).map((x) => x.trim()).filter(Boolean).pop() || "{}";
  return JSON.parse(line);
}

function auditOfficeArtifact(filePath) {
  const abs = path.resolve(filePath);
  const ext = path.extname(abs).toLowerCase();
  if (!fs.existsSync(abs)) {
    return { ok: false, path: abs, kind: ext.replace(".", ""), error: "file_not_found" };
  }
  if (![".xlsx", ".docx", ".pptx"].includes(ext)) {
    return { ok: false, path: abs, kind: ext.replace(".", ""), error: "unsupported_kind" };
  }
  const script = [
    "import json, os, re, sys, zipfile",
    "fp = sys.argv[1]",
    "ext = os.path.splitext(fp)[1].lower()",
    "res = {'ok': True, 'path': fp, 'kind': ext.replace('.', ''), 'size': os.path.getsize(fp)}",
    "with zipfile.ZipFile(fp, 'r') as z:",
    "  names = z.namelist()",
    "  def read(name):",
    "    try:",
    "      return z.read(name).decode('utf-8', errors='ignore')",
    "    except Exception:",
    "      return ''",
    "  if ext == '.xlsx':",
    "    wb = read('xl/workbook.xml')",
    "    sheets = re.findall(r'name=\"([^\"]+)\"', wb)",
    "    res['sheet_names'] = sheets",
    "    res['sheet_count'] = len(sheets)",
    "    res['chart_count'] = sum(1 for n in names if n.startswith('xl/charts/chart') and n.endswith('.xml'))",
    "    res['has_cleaned'] = 'cleaned' in sheets",
    "    res['has_chart_data'] = 'chart_data' in sheets",
    "    res['has_dashboard'] = 'dashboard' in sheets",
    "  elif ext == '.docx':",
    "    doc = read('word/document.xml')",
    "    res['heading_count'] = len(re.findall(r'<w:pStyle[^>]*w:val=\"Heading', doc))",
    "    res['table_count'] = len(re.findall(r'<w:tbl[ >]', doc))",
    "    res['image_count'] = len(re.findall(r'<w:drawing[ >]', doc))",
    "    res['paragraph_count'] = len(re.findall(r'<w:p[ >]', doc))",
    "    text = re.sub(r'<[^>]+>', ' ', doc)",
    "    text = re.sub(r'\\s+', ' ', text).strip()",
    "    res['text_len'] = len(text)",
    "    res['bad_char_count'] = text.count('\\ufffd')",
    "    res['qmark_count'] = text.count('?') + text.count('？')",
    "    cjk = len(re.findall(r'[\\u4e00-\\u9fff]', text))",
    "    res['cjk_count'] = cjk",
    "    res['cjk_ratio'] = (float(cjk) / max(1, len(text)))",
    "  elif ext == '.pptx':",
    "    res['slide_count'] = sum(1 for n in names if n.startswith('ppt/slides/slide') and n.endswith('.xml'))",
    "    res['image_count'] = sum(1 for n in names if n.startswith('ppt/media/'))",
    "    res['chart_count'] = sum(1 for n in names if n.startswith('ppt/charts/chart') and n.endswith('.xml'))",
    "print(json.dumps(res, ensure_ascii=False))",
  ].join("\n");
  try {
    return runPythonJson(script, [abs]);
  } catch (e) {
    return {
      ok: false,
      path: abs,
      kind: ext.replace(".", ""),
      size: Number(fs.statSync(abs).size || 0),
      error: String(e && e.message ? e.message : e),
    };
  }
}

function gateThresholds() {
  return {
    xlsx_min_size: Number(process.env.AIWF_GATE_XLSX_MIN_SIZE || 8 * 1024),
    xlsx_min_charts: Number(process.env.AIWF_GATE_XLSX_MIN_CHARTS || 2),
    docx_min_size: Number(process.env.AIWF_GATE_DOCX_MIN_SIZE || 6 * 1024),
    docx_min_headings: Number(process.env.AIWF_GATE_DOCX_MIN_HEADINGS || 2),
    docx_min_tables: Number(process.env.AIWF_GATE_DOCX_MIN_TABLES || 1),
    docx_max_bad_char_ratio: Number(process.env.AIWF_GATE_DOCX_MAX_BAD_CHAR_RATIO || 0.01),
    docx_max_qmark_ratio: Number(process.env.AIWF_GATE_DOCX_MAX_QMARK_RATIO || 0.08),
    docx_min_cjk_ratio: Number(process.env.AIWF_GATE_DOCX_MIN_CJK_RATIO || 0.1),
    pptx_min_size: Number(process.env.AIWF_GATE_PPTX_MIN_SIZE || 10 * 1024),
    pptx_min_slides: Number(process.env.AIWF_GATE_PPTX_MIN_SLIDES || 4),
    pptx_min_images_or_charts: Number(process.env.AIWF_GATE_PPTX_MIN_VISUALS || 1),
  };
}

function evaluateOfficeArtifacts(audits = [], thresholds = gateThresholds()) {
  const fails = [];
  const byKind = new Map();
  for (const a of audits || []) {
    byKind.set(String(a.kind || "").toLowerCase(), a);
  }
  const xlsx = byKind.get("xlsx");
  const docx = byKind.get("docx");
  const pptx = byKind.get("pptx");
  if (!xlsx) fails.push("missing_xlsx");
  if (!docx) fails.push("missing_docx");
  if (!pptx) fails.push("missing_pptx");
  if (xlsx) {
    if (Number(xlsx.size || 0) < thresholds.xlsx_min_size) fails.push(`xlsx_size_lt_${thresholds.xlsx_min_size}`);
    if (Number(xlsx.chart_count || 0) < thresholds.xlsx_min_charts) fails.push(`xlsx_chart_count_lt_${thresholds.xlsx_min_charts}`);
    if (!xlsx.has_cleaned) fails.push("xlsx_missing_cleaned_sheet");
    if (!xlsx.has_chart_data) fails.push("xlsx_missing_chart_data_sheet");
  }
  if (docx) {
    if (Number(docx.size || 0) < thresholds.docx_min_size) fails.push(`docx_size_lt_${thresholds.docx_min_size}`);
    if (Number(docx.heading_count || 0) < thresholds.docx_min_headings) fails.push(`docx_heading_count_lt_${thresholds.docx_min_headings}`);
    if (Number(docx.table_count || 0) < thresholds.docx_min_tables) fails.push(`docx_table_count_lt_${thresholds.docx_min_tables}`);
    const textLen = Math.max(1, Number(docx.text_len || 0));
    const badRatio = Number(docx.bad_char_count || 0) / textLen;
    const qmarkRatio = Number(docx.qmark_count || 0) / textLen;
    const cjkRatio = Number(docx.cjk_ratio || 0);
    if (badRatio > thresholds.docx_max_bad_char_ratio) fails.push(`docx_bad_char_ratio_gt_${thresholds.docx_max_bad_char_ratio}`);
    if (qmarkRatio > thresholds.docx_max_qmark_ratio) fails.push(`docx_qmark_ratio_gt_${thresholds.docx_max_qmark_ratio}`);
    if (cjkRatio < thresholds.docx_min_cjk_ratio) fails.push(`docx_cjk_ratio_lt_${thresholds.docx_min_cjk_ratio}`);
  }
  if (pptx) {
    if (Number(pptx.size || 0) < thresholds.pptx_min_size) fails.push(`pptx_size_lt_${thresholds.pptx_min_size}`);
    if (Number(pptx.slide_count || 0) < thresholds.pptx_min_slides) fails.push(`pptx_slide_count_lt_${thresholds.pptx_min_slides}`);
    const visuals = Number(pptx.image_count || 0) + Number(pptx.chart_count || 0);
    if (visuals < thresholds.pptx_min_images_or_charts) fails.push(`pptx_visual_count_lt_${thresholds.pptx_min_images_or_charts}`);
  }
  return {
    ok: fails.length === 0,
    thresholds,
    failures: fails,
  };
}

module.exports = {
  auditOfficeArtifact,
  gateThresholds,
  evaluateOfficeArtifacts,
};
