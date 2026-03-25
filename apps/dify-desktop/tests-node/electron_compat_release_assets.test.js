const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("electron compatibility release wrapper is explicit and secondary", () => {
  const wrapper = readText("ops/scripts/release_electron_compatibility.ps1");
  const frontendRelease = readText("ops/scripts/release_frontend_productize.ps1");
  const legacy = readText("ops/scripts/release_productize.ps1");
  const desktopDoc = readText("docs/dify_desktop_app.md");
  const retirementDoc = readText("docs/electron_compatibility_retirement_plan_20260321.md");
  const minimalDelivery = readText("docs/offline_delivery_minimal.md");

  assert.match(wrapper, /Electron compatibility release path invoked/i);
  assert.match(wrapper, /release_productize\.ps1/);
  assert.match(frontendRelease, /release_electron_compatibility\.ps1/);
  assert.match(legacy, /frontend_verification/);
  assert.match(legacy, /architecture_scorecard/);
  assert.match(legacy, /architecture_scorecard_release_ready_latest\.json/);
  assert.match(legacy, /architecture_scorecard_release_ready_latest\.md/);
  assert.match(legacy, /release blocked by architecture scorecard gate/i);
  assert.match(legacy, /architecture scorecard release gate passed/i);
  assert.match(legacy, /frontend_primary_verification_latest\.json/);
  assert.match(legacy, /frontend_compatibility_verification_latest\.json/);
  assert.match(legacy, /governance_store_schema_versions/);
  assert.match(legacy, /governance_control_plane_boundary/);
  assert.match(legacy, /governance_capability_export/);
  assert.match(legacy, /local_workflow_store_schema_versions/);
  assert.match(legacy, /template_pack_contract_sync/);
  assert.match(legacy, /local_template_storage_contract_sync/);
  assert.match(legacy, /offline_template_catalog_sync/);
  assert.match(legacy, /legacy Electron compatibility release path/i);
  assert.match(desktopDoc, /release_electron_compatibility\.ps1/);
  assert.match(retirementDoc, /2026-06-18/);
  assert.match(retirementDoc, /secondary compatibility frontend/i);
  assert.match(minimalDelivery, /release_gate_audit_<version>\.json/);
  assert.match(minimalDelivery, /frontend_verification\.compatibility/);
  assert.match(minimalDelivery, /architecture_scorecard/i);
  assert.match(minimalDelivery, /architecture_scorecard_release_ready_latest\.json/i);
  assert.match(minimalDelivery, /overall_status = passed/i);
  assert.match(minimalDelivery, /ci_check\.ps1 -CiProfile Quick/i);
  assert.match(minimalDelivery, /ci_check\.ps1 -CiProfile Compatibility/);
});
