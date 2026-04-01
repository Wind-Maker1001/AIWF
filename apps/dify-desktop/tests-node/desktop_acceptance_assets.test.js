const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("desktop real sample acceptance keeps office output gates disabled explicitly", () => {
  const psAcceptance = readText("ops/scripts/acceptance_desktop_real_sample.ps1");
  const financeAcceptance = readText("ops/scripts/acceptance_desktop_finance_template.ps1");
  const jsAcceptance = readText("apps/dify-desktop/scripts/acceptance_real_samples.js");

  for (const pattern of [
    /md_only = \$false/i,
    /strict_output_gate = \$false/i,
    /content_quality_gate_enabled = \$false/i,
    /office_quality_gate_enabled = \$false/i,
    /xlsx_embed_charts = \$true/i,
  ]) {
    assert.match(psAcceptance, pattern);
  }

  for (const pattern of [
    /md_only:\s*false/i,
    /strict_output_gate:\s*false/i,
    /content_quality_gate_enabled:\s*false/i,
    /office_quality_gate_enabled:\s*false/i,
    /xlsx_embed_charts:\s*true/i,
  ]) {
    assert.match(jsAcceptance, pattern);
  }

  for (const text of [psAcceptance, financeAcceptance]) {
    assert.match(text, /Assert-CleaningShadowDependencies/i);
    assert.match(text, /Invoke-GlueRunCleaningAcceptance/i);
    assert.match(text, /Assert-ShadowCompareMatched/i);
    assert.match(text, /local_standalone/i);
    assert.match(text, /run_mode_audit\.jsonl/i);
    assert.match(text, /cleaning_shadow_rollout\.json/i);
    assert.match(text, /cleaning_result\.json/i);
  }

  assert.doesNotMatch(psAcceptance, /runOfflineCleaning\s*\(/);
  assert.doesNotMatch(financeAcceptance, /runOfflineCleaning\s*\(/);
  assert.doesNotMatch(psAcceptance, /New-AiwfBaseJob/);
  assert.doesNotMatch(financeAcceptance, /New-AiwfBaseJob/);
});
