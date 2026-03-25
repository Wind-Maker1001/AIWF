const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("frontend launch scripts ensure local governance bridge before startup", () => {
  const ensureScript = readText("ops/scripts/ensure_local_governance_bridge.ps1");
  const runFrontend = readText("ops/scripts/run_aiwf_frontend.ps1");
  const runWinUi = readText("ops/scripts/run_dify_native_winui.ps1");
  const runDesktop = readText("ops/scripts/run_dify_desktop.ps1");
  const quickstart = readText("docs/quickstart_native_winui.md");

  assert.match(ensureScript, /run_glue_python\.ps1/);
  assert.match(ensureScript, /\/health/);
  assert.match(ensureScript, /StartIfMissing/);

  assert.match(runFrontend, /SkipEnsureGlueBridge/);
  assert.match(runFrontend, /publish_native_winui\.ps1/);
  assert.match(runFrontend, /package_native_winui_bundle\.ps1/);
  assert.match(runFrontend, /publishing primary frontend app: WinUI/);
  assert.match(runFrontend, /packaging primary frontend installer bundle: WinUI/);
  assert.match(runFrontend, /ReleaseChannel = "dev"/);
  assert.match(runWinUi, /ensure_local_governance_bridge\.ps1/);
  assert.match(runWinUi, /SkipEnsureGlueBridge/);
  assert.match(runWinUi, /AIWF_MANUAL_REVIEW_PROVIDER/);
  assert.match(runWinUi, /AIWF_QUALITY_RULE_SET_PROVIDER/);
  assert.match(runWinUi, /AIWF_WORKFLOW_APP_REGISTRY_PROVIDER/);
  assert.match(runWinUi, /AIWF_WORKFLOW_VERSION_PROVIDER/);
  assert.match(runWinUi, /AIWF_WORKFLOW_RUN_AUDIT_PROVIDER/);
  assert.match(runWinUi, /AIWF_RUN_BASELINE_PROVIDER/);
  assert.match(runDesktop, /ensure_local_governance_bridge\.ps1/);
  assert.match(runDesktop, /SkipEnsureGlueBridge/);
  assert.match(runDesktop, /AIWF_MANUAL_REVIEW_PROVIDER/);
  assert.match(runDesktop, /AIWF_QUALITY_RULE_SET_PROVIDER/);
  assert.match(runDesktop, /AIWF_WORKFLOW_APP_REGISTRY_PROVIDER/);
  assert.match(runDesktop, /AIWF_WORKFLOW_VERSION_PROVIDER/);
  assert.match(runDesktop, /AIWF_WORKFLOW_RUN_AUDIT_PROVIDER/);
  assert.match(runDesktop, /AIWF_RUN_BASELINE_PROVIDER/);

  assert.match(quickstart, /glue-python governance bridge/i);
  assert.match(quickstart, /SkipEnsureGlueBridge/);
  assert.match(quickstart, /run_aiwf_frontend\.ps1 -BuildWin/);
  assert.match(quickstart, /run_aiwf_frontend\.ps1 -BuildInstaller/);
});
