const test = require("node:test");
const assert = require("node:assert/strict");
const Module = require("module");

function purgeModules(modulePaths) {
  for (const modulePath of modulePaths) {
    try {
      delete require.cache[require.resolve(modulePath)];
    } catch {}
  }
}

test("runOfflineCleaning prefers unified execution report when available", async () => {
  const originalLoad = Module._load;
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
          computeOfficeQualityScore: () => ({ score: 100, pass: true, level: "good", thresholds: { min_score: 60 }, metrics: {} }),
          writeMarkdown: async () => {},
        }),
      };
    }
    if (request === "./offline_ingest") {
      return {
        createOfflineIngest: () => ({
          readInputRows: async () => [{ id: "1", amount: "10", text: "claim" }],
          cleanRows: () => ({
            rows: [{ id: "1", amount: "10", text: "claim" }],
            quality: { input_rows: 1, output_rows: 1, invalid_rows: 0, filtered_rows: 0, duplicate_rows_removed: 0 },
            execution_mode: "rust_v2",
            execution_audit: { schema: "transform_rows_v2.audit.v1" },
            eligibility_reason: "eligible",
            requested_rust_v2_mode: "default",
            effective_rust_v2_mode: "default",
            verify_on_default: false,
            shadow_compare: {
              status: "skipped",
              matched: false,
              mismatch_count: 0,
              mismatches: [],
              skipped_reason: "default_without_verify",
              compare_fields: ["rows", "quality", "reason_counts"],
            },
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

  try {
    purgeModules(["../offline_engine"]);
    const { runOfflineCleaning } = require("../offline_engine");
    const out = await runOfflineCleaning({
      output_root: "D:/tmp",
      params: {
        md_only: true,
        input_files: "D:/sample.txt",
      },
    });
    assert.equal(out.execution.execution_mode, "rust_v2");
    assert.equal(out.execution.eligibility_reason, "eligible");
    assert.equal(out.execution.execution_audit.schema, "transform_rows_v2.audit.v1");
    assert.equal(out.execution.requested_rust_v2_mode, "default");
    assert.equal(out.execution.effective_rust_v2_mode, "default");
    assert.equal(out.execution.verify_on_default, false);
    assert.equal(out.execution.shadow_compare.status, "skipped");
  } finally {
    Module._load = originalLoad;
    purgeModules(["../offline_engine"]);
  }
});
