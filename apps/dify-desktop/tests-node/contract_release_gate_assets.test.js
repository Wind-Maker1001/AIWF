const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("new contract gates are wired into package and release scripts", () => {
  const packageOffline = readText("ops/scripts/package_offline_bundle.ps1");
  const packageWinUi = readText("ops/scripts/package_native_winui_bundle.ps1");
  const packageMsix = readText("ops/scripts/package_native_winui_msix.ps1");
  const releaseLegacy = readText("ops/scripts/release_productize.ps1");
  const releaseFrontend = readText("ops/scripts/release_frontend_productize.ps1");
  const releaseElectron = readText("ops/scripts/release_electron_compatibility.ps1");

  for (const text of [packageOffline, packageWinUi, packageMsix, releaseLegacy, releaseFrontend]) {
    assert.match(text, /export_governance_capabilities\.ps1/);
    assert.match(text, /SkipGovernanceControlPlaneBoundaryGate/);
    assert.match(text, /SkipGovernanceStoreSchemaVersionsGate/);
    assert.match(text, /SkipLocalWorkflowStoreSchemaVersionsGate/);
    assert.match(text, /SkipTemplatePackContractSyncGate/);
    assert.match(text, /SkipLocalTemplateStorageContractSyncGate/);
    assert.match(text, /SkipOfflineTemplateCatalogSyncGate/);
  }

  for (const text of [packageOffline, packageWinUi, packageMsix, releaseLegacy, releaseFrontend]) {
    assert.match(text, /check_governance_control_plane_boundary\.ps1/);
    assert.match(text, /check_governance_store_schema_versions\.ps1/);
    assert.match(text, /check_local_workflow_store_schema_versions\.ps1/);
    assert.match(text, /check_template_pack_contract_sync\.ps1/);
    assert.match(text, /check_local_template_storage_contract_sync\.ps1/);
    assert.match(text, /check_offline_template_catalog_sync\.ps1/);
  }

  assert.match(releaseLegacy, /governance_store_schema_versions/);
  assert.match(releaseLegacy, /governance_control_plane_boundary/);
  assert.match(releaseLegacy, /governance_capability_export/);
  assert.match(releaseLegacy, /local_workflow_store_schema_versions/);
  assert.match(releaseLegacy, /template_pack_contract_sync/);
  assert.match(releaseLegacy, /local_template_storage_contract_sync/);
  assert.match(releaseLegacy, /offline_template_catalog_sync/);

  assert.match(releaseFrontend, /governance_store_schema_versions/);
  assert.match(releaseFrontend, /governance_control_plane_boundary/);
  assert.match(releaseFrontend, /governance_capability_export/);
  assert.match(releaseFrontend, /local_workflow_store_schema_versions/);
  assert.match(releaseFrontend, /template_pack_contract_sync/);
  assert.match(releaseFrontend, /local_template_storage_contract_sync/);
  assert.match(releaseFrontend, /offline_template_catalog_sync/);

  assert.match(releaseElectron, /SkipGovernanceControlPlaneBoundaryGate/);
  assert.match(releaseElectron, /SkipGovernanceStoreSchemaVersionsGate/);
  assert.match(releaseElectron, /SkipLocalWorkflowStoreSchemaVersionsGate/);
  assert.match(releaseElectron, /SkipTemplatePackContractSyncGate/);
  assert.match(releaseElectron, /SkipLocalTemplateStorageContractSyncGate/);
  assert.match(releaseElectron, /SkipOfflineTemplateCatalogSyncGate/);
});
