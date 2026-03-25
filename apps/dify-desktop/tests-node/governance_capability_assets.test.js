const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("governance capability manifest and generated assets stay aligned", () => {
  const manifest = readText("contracts/governance/governance_capabilities.v1.json");
  const exportScript = readText("ops/scripts/export_governance_capabilities.ps1");
  const supportScript = readText("ops/scripts/governance_capability_support.js");
  const desktopGenerated = readText("apps/dify-desktop/workflow_governance_capabilities.generated.js");
  const winUiGenerated = readText("apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/GovernanceCapabilities.Generated.cs");
  const workflowGovernance = readText("apps/dify-desktop/workflow_governance.js");

  assert.match(manifest, /governance_capabilities\.v1/);
  assert.match(manifest, /quality_rule_sets/);
  assert.match(manifest, /workflow_run_audit/);
  assert.match(manifest, /run_baselines/);

  assert.match(exportScript, /governance_capabilities\.v1\.json/);
  assert.match(exportScript, /workflow_governance_capabilities\.generated\.js/);
  assert.match(exportScript, /GovernanceCapabilities\.Generated\.cs/);
  assert.match(exportScript, /governance_surface\.py/);
  assert.match(exportScript, /AIWF_GOVERNANCE_CAPABILITY_SURFACE_EXPORT_JSON/);

  assert.match(supportScript, /loadGovernanceCapabilityManifest/);
  assert.match(supportScript, /buildGovernanceCapabilityDataFromSurfaceExport/);
  assert.match(supportScript, /capabilityToConstant/);
  assert.match(supportScript, /renderManifestJson/);
  assert.match(supportScript, /renderDesktopModule/);
  assert.match(supportScript, /renderWinUiModule/);

  assert.match(desktopGenerated, /GOVERNANCE_CAPABILITIES/);
  assert.match(desktopGenerated, /GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS/);
  assert.match(desktopGenerated, /WORKFLOW_RUN_AUDIT/);
  assert.match(desktopGenerated, /RUN_BASELINES/);
  assert.match(desktopGenerated, /WORKFLOW_AUDIT_EVENTS/);

  assert.match(winUiGenerated, /public static class GovernanceCapabilitiesGenerated/);
  assert.match(winUiGenerated, /public const string WORKFLOW_RUN_AUDIT/);
  assert.match(winUiGenerated, /public const string QUALITY_RULE_SETS/);
  assert.match(winUiGenerated, /WORKFLOW_RUN_AUDIT_WORKFLOW_AUDIT_EVENTS_ROUTE_PREFIX/);
  assert.match(winUiGenerated, /WORKFLOW_SANDBOX_RULES_RULE_VERSIONS_ROUTE_PREFIX/);

  assert.match(workflowGovernance, /workflow_governance_capabilities\.generated\.js/);
  assert.match(workflowGovernance, /GOVERNANCE_CAPABILITIES/);
  assert.match(workflowGovernance, /GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS/);
});
