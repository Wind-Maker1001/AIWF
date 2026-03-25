const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("native winui msix packaging assets stay wired", () => {
  const msixScript = readText("ops/scripts/package_native_winui_msix.ps1");
  const quickstart = readText("docs/quickstart_native_winui.md");

  assert.match(msixScript, /makeappx\.exe/i);
  assert.match(msixScript, /signtool\.exe/i);
  assert.match(msixScript, /PersonalSideloadCert/);
  assert.match(msixScript, /New-SelfSignedCertificate/);
  assert.match(msixScript, /ProvidedPfx/);
  assert.match(msixScript, /StoreThumbprint/);
  assert.match(msixScript, /SigningThumbprint/);
  assert.match(msixScript, /GenerateAppInstaller/);
  assert.match(msixScript, /AppInstaller/);
  assert.match(msixScript, /Install_AIWF_Native_WinUI_MSIX\.ps1/);
  assert.match(msixScript, /Add-AppxPackage/);
  assert.match(msixScript, /SkipFallbackGovernanceGate/);
  assert.match(msixScript, /SkipGovernanceControlPlaneBoundaryGate/);
  assert.match(msixScript, /export_governance_capabilities\.ps1/);
  assert.match(msixScript, /SkipGovernanceStoreSchemaVersionsGate/);
  assert.match(msixScript, /SkipLocalWorkflowStoreSchemaVersionsGate/);
  assert.match(msixScript, /SkipTemplatePackContractSyncGate/);
  assert.match(msixScript, /SkipLocalTemplateStorageContractSyncGate/);
  assert.match(msixScript, /SkipOfflineTemplateCatalogSyncGate/);
  assert.match(msixScript, /check_governance_control_plane_boundary\.ps1/);
  assert.match(msixScript, /check_offline_template_catalog_sync\.ps1/);

  assert.match(quickstart, /PersonalSideloadCert/i);
  assert.match(quickstart, /ensure_personal_sideload_certificate/i);
});
