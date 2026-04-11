const test = require("node:test");
const assert = require("node:assert/strict");
const { createOfflineIngestParsers } = require("../offline_ingest_parsers");

function baseDeps(overrides = {}) {
  return {
    fs: { existsSync: () => true, readFileSync: () => Buffer.from("") },
    path: {
      extname: (value) => {
        const text = String(value || "");
        const idx = text.lastIndexOf(".");
        return idx >= 0 ? text.slice(idx).toLowerCase() : "";
      },
      basename: (value) => String(value || "").split(/[\\/]/).pop(),
    },
    Readable: { from: () => ({}) },
    ExcelJS: { Workbook: function Workbook() { throw new Error("local xlsx parser should not run"); } },
    mammoth: { extractRawText: async () => ({ value: "" }) },
    imageSize: { imageSize: () => ({ width: 10, height: 20 }) },
    readTextFileSmart: () => "",
    runImageOcr: () => ({ ok: false, reason: "tesseract_missing" }),
    runPdfOcr: () => ({ ok: false, reason: "disabled" }),
    splitPdfTextToEvidence: () => [],
    scorePdfExtractText: () => 10,
    isLikelyCorruptedText: () => false,
    materializePaperMarkdown: (_filePath, _kind, chunks) => chunks,
    extractPdfTextFromBuffer: async () => "",
    normalizeCell: (value) => String(value ?? "").trim(),
    IMG_EXT: new Set([".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"]),
    splitTextToChunks: (value) => [String(value || "")].filter(Boolean),
    rowFromText: (filePath, sourceType, txt, idx) => ({ source_file: filePath, source_type: sourceType, row_no: idx + 1, text: txt }),
    toRowsFromWorksheet: () => [{ source_type: "table", text: "fallback" }],
    fetch: async () => ({ ok: true, json: async () => ({ ok: true, rows: [] }) }),
    glueSidecarUrl: () => "http://127.0.0.1:18081",
    ...overrides,
  };
}

test("offline ingest parsers prefer glue sidecar for xlsx", async () => {
  const warnings = [];
  const deps = baseDeps({
    fetch: async (url, init) => {
      assert.equal(url, "http://127.0.0.1:18081/ingest/extract");
      assert.equal(init.method, "POST");
      const body = JSON.parse(String(init.body || "{}"));
      assert.equal(body.input_path, "D:/sample.xlsx");
      return {
        ok: true,
        json: async () => ({
          ok: true,
          rows: [{ source_type: "xlsx", sheet_name: "S1", amount: "10" }],
          quality_blocked: false,
          file_results: [],
        }),
      };
    },
  });
  const parsers = createOfflineIngestParsers(deps);
  const rows = await parsers.parseOneFile("D:/sample.xlsx", warnings, { glue_sidecar_url: "http://127.0.0.1:18081" }, {});
  assert.equal(rows.length, 1);
  assert.equal(rows[0].sheet_name, "S1");
  assert.equal(warnings.length, 0);
});

test("offline ingest parsers block image rows when glue sidecar reports quality gate failure", async () => {
  const warnings = [];
  const deps = baseDeps({
    fetch: async () => ({
      ok: true,
      json: async () => ({
        ok: true,
        rows: [],
        quality_blocked: true,
        blocked_inputs: [{ error: "ocr_confidence_avg below threshold" }],
      }),
    }),
  });
  const parsers = createOfflineIngestParsers(deps);
  const rows = await parsers.parseOneFile("D:/sample.png", warnings, { glue_sidecar_url: "http://127.0.0.1:18081" }, {});
  assert.deepEqual(rows, []);
  assert.ok(warnings.some((item) => String(item).includes("输入质量门禁阻断")));
});

test("offline ingest parsers local xlsx fallback reads all sheets when sidecar is unavailable", async () => {
  const warnings = [];
  function Workbook() {
    this.worksheets = [{ name: "S1", rows: [1] }, { name: "S2", rows: [1] }];
    this.xlsx = { readFile: async () => {} };
    this.csv = { read: async () => {} };
  }
  const deps = baseDeps({
    ExcelJS: { Workbook },
    fetch: async () => {
      throw new Error("sidecar down");
    },
    toRowsFromWorksheet: (ws, filePath) => [{ source_file: filePath, source_type: "table", sheet_name: ws.name }],
  });
  const parsers = createOfflineIngestParsers(deps);
  const rows = await parsers.parseOneFile("D:/sample.xlsx", warnings, { glue_sidecar_url: "http://127.0.0.1:18081" }, {});
  assert.equal(rows.length, 2);
  assert.deepEqual(rows.map((item) => item.sheet_name), ["S1", "S2"]);
  assert.ok(warnings.some((item) => String(item).includes("glue sidecar 不可用")));
});

test("offline ingest parsers preserve sidecar recommendation payload for downstream precheck", async () => {
  const warnings = [];
  const runtime = {};
  const deps = baseDeps({
    fetch: async () => ({
      ok: true,
      json: async () => ({
        ok: true,
        rows: [{ source_type: "xlsx", amount: "10" }],
        quality_blocked: false,
        candidate_profiles: [{ profile: "finance_statement", recommended: true, recommended_template_id: "finance_report_v1", score: 0.96 }],
        blocked_reason_codes: ["header_low_confidence"],
      }),
    }),
  });
  const parsers = createOfflineIngestParsers(deps);
  await parsers.parseOneFile("D:/sample.xlsx", warnings, { glue_sidecar_url: "http://127.0.0.1:18081" }, runtime);
  assert.equal(Array.isArray(runtime.sidecarExtractResults), true);
  assert.equal(runtime.sidecarExtractResults.length, 1);
  assert.equal(runtime.sidecarExtractResults[0].payload.candidate_profiles[0].recommended_template_id, "finance_report_v1");
  assert.equal(runtime.sidecarExtractResults[0].payload.blocked_reason_codes[0], "header_low_confidence");
});
