const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("native winui release wrapper enforces trusted msix on stable by default", () => {
  const releaseWrapper = readText("ops/scripts/release_frontend_productize.ps1");
  const deliveryDoc = readText("docs/offline_delivery_native_winui.md");

  assert.match(releaseWrapper, /frontend_verification/);
  assert.match(releaseWrapper, /architecture_scorecard/);
  assert.match(releaseWrapper, /architecture_scorecard_release_ready_latest\.json/);
  assert.match(releaseWrapper, /architecture_scorecard_release_ready_latest\.md/);
  assert.match(releaseWrapper, /release blocked by architecture scorecard gate/i);
  assert.match(releaseWrapper, /architecture scorecard release gate passed/i);
  assert.match(releaseWrapper, /frontend_primary_verification_latest\.json/);
  assert.match(releaseWrapper, /frontend_compatibility_verification_latest\.json/);
  assert.match(releaseWrapper, /governance_store_schema_versions/);
  assert.match(releaseWrapper, /governance_control_plane_boundary/);
  assert.match(releaseWrapper, /governance_capability_export/);
  assert.match(releaseWrapper, /export_governance_capabilities\.ps1/);
  assert.match(releaseWrapper, /local_workflow_store_schema_versions/);
  assert.match(releaseWrapper, /template_pack_contract_sync/);
  assert.match(releaseWrapper, /local_template_storage_contract_sync/);
  assert.match(releaseWrapper, /offline_template_catalog_sync/);
  assert.match(releaseWrapper, /ReleaseAudience/);
  assert.match(releaseWrapper, /ManagedTrusted/);
  assert.match(releaseWrapper, /PersonalSideload/);
  assert.match(releaseWrapper, /ManagedTrusted distribution cannot ship preview or personal sideload signing/i);
  assert.match(releaseWrapper, /ManagedTrusted distribution must also generate an appinstaller/i);
  assert.match(releaseWrapper, /MsixSigningMode/);
  assert.match(releaseWrapper, /AllowPreviewMsixOnStable/);
  assert.match(releaseWrapper, /GenerateAppInstaller/);

  assert.match(deliveryDoc, /ProvidedPfx/);
  assert.match(deliveryDoc, /PersonalSideload/);
  assert.match(deliveryDoc, /ManagedTrusted/);
  assert.match(deliveryDoc, /AllowPreviewMsixOnStable/);
  assert.match(deliveryDoc, /ManagedTrusted.*appinstaller/i);
  assert.match(deliveryDoc, /release_frontend_audit_<version>\.json/);
  assert.match(deliveryDoc, /frontend_verification\.primary/);
  assert.match(deliveryDoc, /frontend_compatibility_verification_latest\.json/);
  assert.match(deliveryDoc, /architecture_scorecard/i);
  assert.match(deliveryDoc, /architecture_scorecard_release_ready_latest\.json/i);
  assert.match(deliveryDoc, /overall_status = passed/i);
  assert.match(deliveryDoc, /ci_check\.ps1 -CiProfile Compatibility/i);
});
