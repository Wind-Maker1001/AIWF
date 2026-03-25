const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("native winui release cleanup script keeps personal and managed exemplars", () => {
  const cleanupScript = readText("ops/scripts/clean_native_winui_release_artifacts.ps1");

  assert.match(cleanupScript, /native_winui_bundle_2026\.03\.21-personal/);
  assert.match(cleanupScript, /native_winui_bundle_2026\.03\.21-managed/);
  assert.match(cleanupScript, /native_winui_msix_2026\.03\.21-personal/);
  assert.match(cleanupScript, /native_winui_msix_2026\.03\.21-managed/);
  assert.match(cleanupScript, /release_frontend_audit_2026\.03\.21-personal\.json/);
  assert.match(cleanupScript, /release_frontend_audit_2026\.03\.21-managed\.json/);
});
