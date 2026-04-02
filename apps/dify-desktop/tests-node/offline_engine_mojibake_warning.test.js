const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { runOfflineCleaning } = require("../offline_engine");

test("runOfflineCleaning warns on medium gibberish ratio without forcing fidelity fallback", async () => {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-mojibake-"));
  const src = path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples_dirty", "dirty_03_mojibake.txt");

  const out = await runOfflineCleaning({
    output_root: tmp,
    glue_url: "http://127.0.0.1:1",
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
  assert.ok(
    warnings.some((warning) => {
      const text = String(warning);
      return text.includes("\u7591\u4f3c\u4e71\u7801\u7387\u504f\u9ad8") || text.includes("\u7591\u4f3c\u4e71\u7801\u7387\u8fc7\u9ad8");
    })
  );
  const qr = String(out?.quality_score?.gibberish_ratio || 0);
  assert.ok(Number(qr) >= 0.2);
});
