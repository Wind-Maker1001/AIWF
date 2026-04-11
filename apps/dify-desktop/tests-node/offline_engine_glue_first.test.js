const test = require("node:test");
const assert = require("node:assert/strict");
const Module = require("module");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

function purgeModules(modulePaths) {
  for (const modulePath of modulePaths) {
    try {
      delete require.cache[require.resolve(modulePath)];
    } catch {}
  }
}

test("runOfflineCleaning prefers glue-first result for mixed input when available", async () => {
  const originalLoad = Module._load;
  const originalFetch = global.fetch;
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-glue-first-"));
  const artifactDir = path.join(tempRoot, "job-test", "artifacts");
  fs.mkdirSync(artifactDir, { recursive: true });
  const xlsxPath = path.join(artifactDir, "fin.xlsx");
  const docxPath = path.join(artifactDir, "audit.docx");
  const pptxPath = path.join(artifactDir, "deck.pptx");
  [xlsxPath, docxPath, pptxPath].forEach((fp) => fs.writeFileSync(fp, "BIN"));

  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === "./offline_text") {
      return { readTextFileSmart: (fp) => fs.readFileSync(fp, "utf8") };
    }
    if (request === "./offline_runtime") {
      return { runImageOcr: () => ({ ok: false }), runPdfOcr: () => ({ ok: false }) };
    }
    if (request === "./offline_paper") {
      return {
        splitPdfTextToEvidence: () => [],
        scorePdfExtractText: () => 10,
        isLikelyCorruptedText: () => false,
        rowTextForQuality: (row) => String(row?.text || ""),
        materializePaperMarkdown: () => [],
        writePaperMarkdownIndex: async () => {},
        writeAiCorpusMarkdown: async () => {},
        writeQualityReport: async () => {},
      };
    }
    if (request === "./offline_outputs") {
      return {
        createOfflineOutputs: () => ({
          buildDataQualityInsights: () => [],
          buildEvidenceHighlights: () => [],
          filterRowsForOffice: () => ({ rows: [], filtered: 0, removedRows: [] }),
          computeOfficeQualityScore: () => ({ score: 100, pass: true, level: "good", thresholds: { min_score: 60 }, metrics: {} }),
          writeMarkdown: async () => {},
        }),
      };
    }
    if (request === "./offline_ingest") {
      return {
        createOfflineIngest: () => ({
          readInputRows: async () => {
            throw new Error("local readInputRows should not be used");
          },
          cleanRows: () => {
            throw new Error("local cleanRows should not be used");
          },
          applyQualityGates: () => ({ evaluated: true, passed: true }),
          buildFidelityRows: () => [],
          precheckRows: () => ({ ok: true }),
        }),
      };
    }
    if (request === "./offline_engine_config") {
      return {
        createOfflineEngineConfig: () => ({
          listCleaningTemplates: () => ({ ok: true, templates: [] }),
          normalizeReportTitle: (title) => String(title || "report"),
          resolveCleaningTemplateParams: (params) => ({ ...(params || {}) }),
          resolveOfficeLayout: () => ({}),
          resolveOfficeTheme: () => ({}),
        }),
      };
    }
    if (request === "./offline_engine_runtime") {
      return {
        ExcelJS: {},
        IMG_EXT: new Set(),
        ensureDir: (p) => fs.mkdirSync(p, { recursive: true }),
        extractPdfTextFromBuffer: async () => "",
        imageSize: {},
        isMissingNodeModuleError: () => false,
        makeJobId: () => "job-test",
        mammoth: {},
        normalizeAmount: (value) => Number(value),
        normalizeCell: (value) => String(value ?? ""),
        resolveOfficeFont: () => "",
        sha256File: () => "sha",
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  global.fetch = async (url, options = {}) => {
    if (String(url).endsWith("/health")) {
      return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true }) };
    }
    if (String(url).includes("/ingest/extract")) {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({ ok: true, rows: [{ id: "1", amount: "10", text: "claim" }] }),
      };
    }
    if (String(url).includes("/run/cleaning")) {
      const body = JSON.parse(String(options.body || "{}"));
      assert.equal(body.params.local_standalone, true);
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          ok: true,
          job_id: "job-test",
          seconds: 0.2,
          artifacts: [
            { artifact_id: "xlsx_fin_001", kind: "xlsx", path: xlsxPath, sha256: "sha-xlsx" },
            { artifact_id: "docx_audit_001", kind: "docx", path: docxPath, sha256: "sha-docx" },
            { artifact_id: "pptx_deck_001", kind: "pptx", path: pptxPath, sha256: "sha-pptx" },
          ],
          profile: {
            quality: { input_rows: 1, output_rows: 1, invalid_rows: 0, filtered_rows: 0, duplicate_rows_removed: 0, rust_v2_used: true },
            quality_gate: { evaluated: true, passed: true },
          },
          execution: {
            execution_mode: "rust_v2",
            execution_audit: { schema: "transform_rows_v2.audit.v1" },
            eligibility_reason: "eligible",
            requested_rust_v2_mode: "default",
            effective_rust_v2_mode: "default",
            verify_on_default: true,
            shadow_compare: { status: "matched", matched: true, mismatch_count: 0, mismatches: [], skipped_reason: "", compare_fields: ["rows", "quality", "reason_counts"] },
          },
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };

  try {
    purgeModules(["../offline_engine"]);
    const { runOfflineCleaning } = require("../offline_engine");
    const out = await runOfflineCleaning({
      output_root: tempRoot,
      glue_url: "http://127.0.0.1:18081",
      params: {
        input_files: JSON.stringify(["D:/sample.txt"]),
        office_lang: "zh",
      },
    });
    assert.equal(out.execution.execution_mode, "rust_v2");
    assert.equal(out.execution.verify_on_default, true);
    assert.equal(out.execution.shadow_compare.status, "matched");
    assert.equal(out.artifacts_dir, artifactDir);
    assert.equal(Array.isArray(out.artifacts), true);
    assert.equal(out.artifacts.length, 3);
  } finally {
    global.fetch = originalFetch;
    Module._load = originalLoad;
    purgeModules(["../offline_engine"]);
  }
});

test("runOfflineCleaning falls back to local cleaner on glue mismatch", async () => {
  const originalLoad = Module._load;
  const originalFetch = global.fetch;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === "./offline_text") {
      return { readTextFileSmart: () => "" };
    }
    if (request === "./offline_runtime") {
      return { runImageOcr: () => ({ ok: false }), runPdfOcr: () => ({ ok: false }) };
    }
    if (request === "./offline_paper") {
      return {
        splitPdfTextToEvidence: () => [],
        scorePdfExtractText: () => 10,
        isLikelyCorruptedText: () => false,
        rowTextForQuality: (row) => String(row?.text || ""),
        materializePaperMarkdown: () => [],
        writePaperMarkdownIndex: async () => {},
        writeAiCorpusMarkdown: async () => {},
        writeQualityReport: async () => {},
      };
    }
    if (request === "./offline_outputs") {
      return {
        createOfflineOutputs: () => ({
          buildDataQualityInsights: () => [],
          buildEvidenceHighlights: () => [],
          filterRowsForOffice: () => ({ rows: [], filtered: 0, removedRows: [] }),
          computeOfficeQualityScore: () => ({ score: 90, pass: true, level: "good", thresholds: { min_score: 60 }, metrics: {} }),
          writeXlsx: async () => {},
          writeDocx: async () => {},
          writePptx: async () => {},
          writeMarkdown: async () => {},
        }),
      };
    }
    if (request === "./offline_ingest") {
      return {
        createOfflineIngest: () => ({
          readInputRows: async () => [{ id: "1", amount: "10", text: "claim" }],
          cleanRows: () => ({
            rows: [{ id: "1", amount: 10, text: "claim" }],
            quality: { input_rows: 1, output_rows: 1, invalid_rows: 0, filtered_rows: 0, duplicate_rows_removed: 0 },
            execution_mode: "python_legacy",
            execution_audit: { schema: "desktop_cleaning.audit.v1" },
            eligibility_reason: "eligible",
            requested_rust_v2_mode: "off",
            effective_rust_v2_mode: "off",
            verify_on_default: false,
            shadow_compare: { status: "skipped", matched: false, mismatch_count: 0, mismatches: [], skipped_reason: "mode_off", compare_fields: ["rows", "quality", "reason_counts"] },
          }),
          applyQualityGates: () => ({ evaluated: true, passed: true }),
          buildFidelityRows: () => [],
          precheckRows: () => ({ ok: true }),
        }),
      };
    }
    if (request === "./offline_engine_config") {
      return {
        createOfflineEngineConfig: () => ({
          listCleaningTemplates: () => ({ ok: true, templates: [] }),
          normalizeReportTitle: (title) => String(title || "report"),
          resolveCleaningTemplateParams: (params) => ({ ...(params || {}) }),
          resolveOfficeLayout: () => ({}),
          resolveOfficeTheme: () => ({}),
        }),
      };
    }
    if (request === "./offline_engine_runtime") {
      return {
        ExcelJS: {},
        IMG_EXT: new Set(),
        ensureDir: () => {},
        extractPdfTextFromBuffer: async () => "",
        imageSize: {},
        isMissingNodeModuleError: () => false,
        makeJobId: () => "job-test",
        mammoth: {},
        normalizeAmount: (value) => Number(value),
        normalizeCell: (value) => String(value ?? ""),
        resolveOfficeFont: () => "",
        sha256File: () => "sha",
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  global.fetch = async (url) => {
    if (String(url).endsWith("/health")) {
      return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true }) };
    }
    if (String(url).includes("/ingest/extract")) {
      return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true, rows: [{ id: "1", amount: "10", text: "claim" }] }) };
    }
    if (String(url).includes("/run/cleaning")) {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({
          ok: true,
          execution: {
            execution_mode: "python_legacy",
            execution_audit: { schema: "python_cleaning.audit.v1" },
            eligibility_reason: "shadow_compare_mismatch",
            requested_rust_v2_mode: "default",
            effective_rust_v2_mode: "default",
            verify_on_default: true,
            shadow_compare: { status: "mismatched", matched: false, mismatch_count: 1, mismatches: ["rows mismatch"], skipped_reason: "", compare_fields: ["rows", "quality", "reason_counts"] },
          },
          profile: { quality: { rust_v2_used: false }, quality_gate: { evaluated: true, passed: true } },
          artifacts: [],
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };

  try {
    purgeModules(["../offline_engine"]);
    const { runOfflineCleaning } = require("../offline_engine");
    const out = await runOfflineCleaning({
      output_root: "D:/tmp",
      glue_url: "http://127.0.0.1:18081",
      params: {
        input_files: JSON.stringify(["D:/sample.txt"]),
      },
    });
    assert.equal(out.execution.execution_mode, "python_legacy");
    assert.equal(out.execution.requested_rust_v2_mode, "off");
    assert.ok(Array.isArray(out.warnings));
    assert.ok(out.warnings.some((item) => String(item).includes("shadow_compare_mismatch")));
  } finally {
    global.fetch = originalFetch;
    Module._load = originalLoad;
    purgeModules(["../offline_engine"]);
  }
});

test("runOfflineCleaning surfaces structured guardrail block without local fallback", async () => {
  const originalLoad = Module._load;
  const originalFetch = global.fetch;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (request === "./offline_text") {
      return { readTextFileSmart: () => "" };
    }
    if (request === "./offline_runtime") {
      return { runImageOcr: () => ({ ok: false }), runPdfOcr: () => ({ ok: false }) };
    }
    if (request === "./offline_paper") {
      return {
        splitPdfTextToEvidence: () => [],
        scorePdfExtractText: () => 10,
        isLikelyCorruptedText: () => false,
        rowTextForQuality: (row) => String(row?.text || ""),
        materializePaperMarkdown: () => [],
        writePaperMarkdownIndex: async () => {},
        writeAiCorpusMarkdown: async () => {},
        writeQualityReport: async () => {},
      };
    }
    if (request === "./offline_outputs") {
      return {
        createOfflineOutputs: () => ({
          buildDataQualityInsights: () => [],
          buildEvidenceHighlights: () => [],
          filterRowsForOffice: () => ({ rows: [], filtered: 0, removedRows: [] }),
          computeOfficeQualityScore: () => ({ score: 90, pass: true, level: "good", thresholds: { min_score: 60 }, metrics: {} }),
          writeMarkdown: async () => {},
        }),
      };
    }
    if (request === "./offline_ingest") {
      return {
        createOfflineIngest: () => ({
          readInputRows: async () => {
            throw new Error("local readInputRows should not be used after structured block");
          },
          cleanRows: () => {
            throw new Error("local cleanRows should not be used after structured block");
          },
          applyQualityGates: () => ({ evaluated: true, passed: true }),
          buildFidelityRows: () => [],
          precheckRows: () => ({ ok: true }),
        }),
      };
    }
    if (request === "./offline_engine_config") {
      return {
        createOfflineEngineConfig: () => ({
          listCleaningTemplates: () => ({ ok: true, templates: [] }),
          normalizeReportTitle: (title) => String(title || "report"),
          resolveCleaningTemplateParams: (params) => ({ ...(params || {}) }),
          resolveOfficeLayout: () => ({}),
          resolveOfficeTheme: () => ({}),
        }),
      };
    }
    if (request === "./offline_engine_runtime") {
      return {
        ExcelJS: {},
        IMG_EXT: new Set(),
        ensureDir: () => {},
        extractPdfTextFromBuffer: async () => "",
        imageSize: {},
        isMissingNodeModuleError: () => false,
        makeJobId: () => "job-test",
        mammoth: {},
        normalizeAmount: (value) => Number(value),
        normalizeCell: (value) => String(value ?? ""),
        resolveOfficeFont: () => "",
        sha256File: () => "sha",
      };
    }
    return originalLoad.call(this, request, parent, isMain);
  };

  global.fetch = async (url) => {
    if (String(url).endsWith("/health")) {
      return { ok: true, status: 200, text: async () => JSON.stringify({ ok: true }) };
    }
    if (String(url).includes("/ingest/extract")) {
      return {
        ok: true,
        status: 200,
        text: async () => JSON.stringify({ ok: true, rows: [{ claim_text: "claim", speaker: "alice" }] }),
      };
    }
    if (String(url).includes("/run/cleaning")) {
      return {
        ok: false,
        status: 400,
        text: async () => JSON.stringify({
          ok: false,
          error: "profile mismatch blocked",
          error_code: "profile_mismatch_blocked",
          reason_codes: ["profile_mismatch_blocked"],
          requested_profile: "finance_statement",
          recommended_profile: "debate_evidence",
          profile_confidence: 0.93,
          required_field_coverage: 0.0,
          template_id: "finance_report_v1",
          template_expected_profile: "finance_statement",
          blocking_reason_codes: ["profile_mismatch", "profile_mismatch_blocked"],
          blank_output_expected: false,
          zero_output_unexpected: false,
        }),
      };
    }
    throw new Error(`unexpected fetch: ${url}`);
  };

  try {
    purgeModules(["../offline_engine"]);
    const { runOfflineCleaning } = require("../offline_engine");
    const out = await runOfflineCleaning({
      output_root: "D:/tmp",
      glue_url: "http://127.0.0.1:18081",
      params: {
        input_files: JSON.stringify(["D:/sample.txt"]),
        cleaning_template: "finance_report_v1",
      },
    });
    assert.equal(out.ok, false);
    assert.equal(out.blocked, true);
    assert.equal(out.job_id, "job-test");
    assert.equal(out.error_code, "profile_mismatch_blocked");
    assert.equal(out.requested_profile, "finance_statement");
    assert.equal(out.recommended_profile, "debate_evidence");
    assert.deepEqual(out.reason_codes, ["profile_mismatch_blocked"]);
    assert.deepEqual(out.blocking_reason_codes, ["profile_mismatch", "profile_mismatch_blocked"]);
    assert.equal(out.quality_summary.requested_profile, "finance_statement");
    assert.equal(out.quality_summary.recommended_profile, "debate_evidence");
    assert.equal(out.execution.execution_mode, "guardrail_blocked");
  } finally {
    global.fetch = originalFetch;
    Module._load = originalLoad;
    purgeModules(["../offline_engine"]);
  }
});
