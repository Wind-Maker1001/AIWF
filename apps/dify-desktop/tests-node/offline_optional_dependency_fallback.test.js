const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const Module = require("node:module");

function purgeModules(modulePaths) {
  for (const modulePath of modulePaths) {
    try {
      delete require.cache[require.resolve(modulePath)];
    } catch {}
  }
}

async function withMissingModules(moduleNames, work) {
  const missing = new Set(moduleNames);
  const originalLoad = Module._load;
  Module._load = function patchedLoad(request, parent, isMain) {
    if (missing.has(request)) {
      const err = new Error(`Cannot find module '${request}'`);
      err.code = "MODULE_NOT_FOUND";
      throw err;
    }
    return originalLoad.call(this, request, parent, isMain);
  };
  try {
    return await work();
  } finally {
    Module._load = originalLoad;
  }
}

test("runOfflineCleaning falls back to markdown when office generators are unavailable", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-offline-fallback-"));
  const source = path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples", "sample_01_ai_jobs.txt");

  await withMissingModules(["exceljs"], async () => {
    purgeModules(["../offline_engine", "../offline_outputs"]);
    const { runOfflineCleaning } = require("../offline_engine");
    const out = await runOfflineCleaning({
      output_root: tempRoot,
      glue_url: "http://127.0.0.1:1",
      params: {
        report_title: "fallback_markdown_only",
        input_files: source,
        office_lang: "zh",
        strict_output_gate: false,
        content_quality_gate_enabled: false,
        office_quality_gate_enabled: false,
      },
    });

    assert.equal(!!out?.ok, true);
    const warnings = Array.isArray(out?.warnings) ? out.warnings : [];
    assert.ok(warnings.some((warning) => String(warning).includes("Office \u751f\u6210\u4f9d\u8d56\u7f3a\u5931")));

    const artifacts = Array.isArray(out?.artifacts) ? out.artifacts : [];
    assert.ok(artifacts.length >= 4);
    assert.ok(artifacts.every((item) => String(item?.kind || "") === "md"));
    assert.ok(artifacts.some((item) => fs.existsSync(String(item?.path || ""))));
    assert.equal(artifacts.some((item) => ["xlsx", "docx", "pptx"].includes(String(item?.kind || ""))), false);
  });

  purgeModules(["../offline_engine", "../offline_outputs"]);
});

test("readTextFileSmart remains usable when iconv-lite is unavailable", async () => {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-offline-text-"));
  const sample = "\u4e2d\u6587\u5185\u5bb9";
  const file = path.join(tempRoot, "utf8.txt");
  fs.writeFileSync(file, sample, "utf8");

  await withMissingModules(["iconv-lite"], async () => {
    purgeModules(["../offline_text"]);
    const { decodeBufferWithEncoding, readTextFileSmart } = require("../offline_text");
    assert.equal(decodeBufferWithEncoding(Buffer.from(sample, "utf8"), "gb18030"), sample);
    assert.equal(readTextFileSmart(file), sample);
  });

  purgeModules(["../offline_text"]);
});
