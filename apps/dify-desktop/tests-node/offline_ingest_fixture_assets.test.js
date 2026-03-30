const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { Readable } = require("node:stream");

const { createOfflineIngestParsers } = require("../offline_ingest_parsers");
const { readTextFileSmart } = require("../offline_text");

let runtime = null;
let runtimeReady = false;
try {
  runtime = require("../offline_engine_runtime");
  void runtime.ExcelJS.Workbook;
  runtimeReady = true;
} catch {
  runtime = null;
  runtimeReady = false;
}

test("offline ingest parsers can read real xlsx regression fixture across sheets when sidecar is unavailable", { skip: runtimeReady === false }, async () => {
  const fixture = path.resolve(__dirname, "..", "tests", "fixtures", "regression_xlsx", "customer_multi_sheet.xlsx");
  assert.equal(fs.existsSync(fixture), true);
  const { ExcelJS, IMG_EXT, mammoth, imageSize, extractPdfTextFromBuffer, normalizeCell } = runtime;

  const parsers = createOfflineIngestParsers({
    fs,
    path,
    Readable,
    ExcelJS,
    mammoth,
    imageSize,
    readTextFileSmart,
    runImageOcr: () => ({ ok: false, reason: "tesseract_missing" }),
    runPdfOcr: () => ({ ok: false, reason: "disabled" }),
    splitPdfTextToEvidence: () => [],
    scorePdfExtractText: () => 10,
    isLikelyCorruptedText: () => false,
    materializePaperMarkdown: (_filePath, _kind, chunks) => chunks,
    extractPdfTextFromBuffer,
    normalizeCell,
    IMG_EXT,
    splitTextToChunks: (value) => [String(value || "")].filter(Boolean),
    rowFromText: (filePath, sourceType, txt, idx) => ({ source_file: filePath, source_type: sourceType, row_no: idx + 1, text: txt }),
    toRowsFromWorksheet: (ws, filePath) => {
      const headers = [];
      ws.getRow(2).eachCell((cell, idx) => {
        headers[idx - 1] = normalizeCell(cell.value) || `col_${idx}`;
      });
      const rows = [];
      for (let r = 3; r <= ws.rowCount; r += 1) {
        const row = ws.getRow(r);
        const item = { source_file: filePath, source_type: "table", sheet_name: ws.name, row_no: r - 2 };
        let nonEmpty = 0;
        for (let c = 1; c <= headers.length; c += 1) {
          const value = normalizeCell(row.getCell(c).value);
          item[headers[c - 1]] = value;
          if (value !== "") nonEmpty += 1;
        }
        if (nonEmpty > 0) rows.push(item);
      }
      return rows;
    },
    fetch: async () => {
      throw new Error("sidecar down");
    },
    glueSidecarUrl: () => "http://127.0.0.1:18081",
  });

  const warnings = [];
  const rows = await parsers.parseOneFile(fixture, warnings, { xlsx_all_sheets: true, glue_sidecar_url: "http://127.0.0.1:18081" }, {});
  assert.equal(rows.length, 3);
  assert.deepEqual(rows.map((row) => row.sheet_name), ["North", "North", "South"]);
  assert.ok(warnings.some((item) => String(item).includes("glue sidecar 不可用")));
});

test("desktop regression image fixtures are present", async () => {
  const blank = path.resolve(__dirname, "..", "tests", "fixtures", "regression_images", "blank_blocked.png");
  const textSample = path.resolve(__dirname, "..", "tests", "fixtures", "regression_images", "text_sample.png");
  assert.equal(fs.existsSync(blank), true);
  assert.equal(fs.existsSync(textSample), true);
});
