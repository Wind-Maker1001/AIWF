const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("offline bundle includes desktop contract schemas in script and docs", () => {
  const packageOffline = readText("ops/scripts/package_offline_bundle.ps1");
  const deliveryDoc = readText("docs/offline_delivery_minimal.md");
  const quickstartDoc = readText("docs/quickstart_desktop_offline.md");

  assert.match(packageOffline, /contracts\\desktop/);
  assert.match(packageOffline, /contracts\\workflow/);
  assert.match(packageOffline, /contracts\\rust/);
  assert.match(packageOffline, /contracts\\glue/);
  assert.match(packageOffline, /contracts\\governance/);
  assert.match(packageOffline, /template_pack_artifact\.schema\.json/);
  assert.match(packageOffline, /local_template_storage\.schema\.json/);
  assert.match(packageOffline, /office_theme_catalog\.schema\.json/);
  assert.match(packageOffline, /office_layout_catalog\.schema\.json/);
  assert.match(packageOffline, /cleaning_template_registry\.schema\.json/);
  assert.match(packageOffline, /offline_template_catalog_pack_manifest\.schema\.json/);
  assert.match(packageOffline, /workflow\.schema\.json/);
  assert.match(packageOffline, /operators_manifest\.v1\.json/);
  assert.match(packageOffline, /ingest_extract\.schema\.json/);
  assert.match(packageOffline, /governance_capabilities\.v1\.json/);
  assert.match(packageOffline, /contract_schemas/);
  assert.match(packageOffline, /workflow_contracts/);
  assert.match(packageOffline, /rust_contracts/);
  assert.match(packageOffline, /glue_contracts/);
  assert.match(packageOffline, /governance_contracts/);
  assert.match(packageOffline, /sidecar_regression_quality_report\.json/);
  assert.match(packageOffline, /sidecar_python_rust_consistency_report\.json/);

  assert.match(deliveryDoc, /contracts\/desktop/i);
  assert.match(deliveryDoc, /contracts\/workflow/i);
  assert.match(deliveryDoc, /contracts\/rust/i);
  assert.match(deliveryDoc, /contracts\/glue/i);
  assert.match(deliveryDoc, /contracts\/governance/i);
  assert.match(deliveryDoc, /template_pack_artifact\.schema\.json/i);
  assert.match(deliveryDoc, /local_template_storage\.schema\.json/i);
  assert.match(deliveryDoc, /office_theme_catalog\.schema\.json/i);
  assert.match(deliveryDoc, /office_layout_catalog\.schema\.json/i);
  assert.match(deliveryDoc, /cleaning_template_registry\.schema\.json/i);
  assert.match(deliveryDoc, /offline_template_catalog_pack_manifest\.schema\.json/i);
  assert.match(deliveryDoc, /workflow\.schema\.json/i);
  assert.match(deliveryDoc, /operators_manifest\.v1\.json/i);
  assert.match(deliveryDoc, /ingest_extract\.schema\.json/i);
  assert.match(deliveryDoc, /governance_capabilities\.v1\.json/i);
  assert.match(quickstartDoc, /contracts\/desktop/i);
  assert.match(quickstartDoc, /contracts\/workflow/i);
  assert.match(quickstartDoc, /contracts\/rust/i);
  assert.match(quickstartDoc, /contracts\/governance/i);
});
