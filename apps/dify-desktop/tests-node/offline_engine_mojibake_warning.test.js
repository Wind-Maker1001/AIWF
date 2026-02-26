const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { runOfflineCleaning } = require("../offline_engine");

test("runOfflineCleaning warns on medium gibberish ratio without forcing fidelity fallback", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-mojibake-"));
  const src = path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples_dirty", "dirty_03_mojibake.txt");

  const out = await runOfflineCleaning({
    output_root: tmp,
    params: {
      report_title: "mojibake_warning",
      input_files: src,
      office_lang: "zh",
      strict_output_gate: false,
      content_quality_gate_enabled: false,
      office_quality_gate_enabled: false,
    },
  });

  assert.equal(!!out?.ok, true);
  const warnings = Array.isArray(out?.warnings) ? out.warnings : [];
  assert.ok(warnings.some((w) => String(w).includes("疑似乱码率偏高") || String(w).includes("疑似乱码率过高")));
  const qr = String(out?.quality_score?.gibberish_ratio || 0);
  assert.ok(Number(qr) >= 0.2);
});
