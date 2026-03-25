const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("native winui installer assets stay wired into bundle packaging", () => {
  const packageScript = readText("ops/scripts/package_native_winui_bundle.ps1");
  const installScript = readText("ops/scripts/install_native_winui_bundle.ps1");
  const uninstallScript = readText("ops/scripts/uninstall_native_winui_bundle.ps1");
  const deliveryDoc = readText("docs/offline_delivery_native_winui.md");

  assert.match(packageScript, /Install_AIWF_Native_WinUI\.ps1/);
  assert.match(packageScript, /Uninstall_AIWF_Native_WinUI\.ps1/);
  assert.match(packageScript, /Install_AIWF_Native_WinUI\.cmd/);
  assert.match(packageScript, /install_manifest\.json/);
  assert.match(packageScript, /contracts\\desktop/);
  assert.match(packageScript, /contracts\\workflow/);
  assert.match(packageScript, /contracts\\rust/);
  assert.match(packageScript, /contracts\\governance/);
  assert.match(packageScript, /template_pack_artifact\.schema\.json/);
  assert.match(packageScript, /local_template_storage\.schema\.json/);
  assert.match(packageScript, /office_theme_catalog\.schema\.json/);
  assert.match(packageScript, /office_layout_catalog\.schema\.json/);
  assert.match(packageScript, /cleaning_template_registry\.schema\.json/);
  assert.match(packageScript, /offline_template_catalog_pack_manifest\.schema\.json/);
  assert.match(packageScript, /workflow\.schema\.json/);
  assert.match(packageScript, /operators_manifest\.v1\.json/);
  assert.match(packageScript, /governance_capabilities\.v1\.json/);
  assert.match(packageScript, /contract_schemas/);
  assert.match(packageScript, /workflow_contracts/);
  assert.match(packageScript, /rust_contracts/);
  assert.match(packageScript, /governance_contracts/);
  assert.match(packageScript, /gates = \[ordered\]@/);
  assert.match(packageScript, /governance_control_plane_boundary/);
  assert.match(packageScript, /governance_capability_export/);
  assert.match(packageScript, /export_governance_capabilities\.ps1/);
  assert.match(packageScript, /governance_store_schema_versions/);
  assert.match(packageScript, /local_workflow_store_schema_versions/);
  assert.match(packageScript, /template_pack_contract_sync/);
  assert.match(packageScript, /local_template_storage_contract_sync/);
  assert.match(packageScript, /offline_template_catalog_sync/);

  assert.match(installScript, /AIWF Native WinUI/);
  assert.match(installScript, /CurrentVersion\\Uninstall\\AIWF\.Native\.WinUI/);
  assert.match(uninstallScript, /native winui uninstall scheduled/i);

  assert.match(deliveryDoc, /Install_AIWF_Native_WinUI/);
  assert.match(deliveryDoc, /contracts\/desktop/i);
  assert.match(deliveryDoc, /contracts\/workflow/i);
  assert.match(deliveryDoc, /contracts\/rust/i);
  assert.match(deliveryDoc, /contracts\/governance/i);
});
