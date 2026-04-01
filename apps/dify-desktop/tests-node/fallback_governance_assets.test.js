const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("fallback governance gate is wired into ci and release paths", () => {
  const ciCheck = readText("ops/scripts/ci_check.ps1");
  const releaseFrontend = readText("ops/scripts/release_frontend_productize.ps1");
  const packageOffline = readText("ops/scripts/package_offline_bundle.ps1");
  const releaseLegacy = readText("ops/scripts/release_productize.ps1");
  const releaseElectron = readText("ops/scripts/release_electron_compatibility.ps1");

  assert.match(ciCheck, /SkipFallbackGovernance/);
  assert.match(ciCheck, /check_fallback_governance\.ps1/);

  assert.match(releaseFrontend, /SkipFallbackGovernanceGate/);
  assert.match(releaseFrontend, /check_fallback_governance\.ps1/);
  assert.match(releaseFrontend, /fallback_governance/);

  assert.match(packageOffline, /SkipFallbackGovernanceGate/);
  assert.match(packageOffline, /check_fallback_governance\.ps1/);
  assert.match(packageOffline, /fallback_governance/);

  assert.match(releaseLegacy, /SkipFallbackGovernanceGate/);
  assert.match(releaseLegacy, /check_fallback_governance\.ps1/);
  assert.match(releaseLegacy, /fallback_governance/);

  assert.match(releaseElectron, /SkipFallbackGovernanceGate/);
});

test("local legacy providers declare fallback governance titles", () => {
  const root = path.resolve(__dirname, "..");
  const files = fs.readdirSync(root)
    .filter((name) => name.endsWith("_store.js"))
    .map((name) => path.join(root, name));

  const localLegacyFiles = files.filter((file) => /const LOCAL_PROVIDER = "local_legacy";/.test(fs.readFileSync(file, "utf8")));
  assert.equal(localLegacyFiles.length, 0);

  for (const file of localLegacyFiles) {
    const text = fs.readFileSync(file, "utf8");
    assert.match(text, /const FALLBACK_GOVERNANCE_TITLE = /, file);
  }
});

test("fallback governance inventory covers sandbox autofix mirror", () => {
  const gate = readText("ops/scripts/check_fallback_governance.ps1");
  const doc = readText("docs/fallback_governance_20260320.md");

  assert.match(gate, /FALLBACK_GOVERNANCE_TITLE/);
  assert.match(gate, /Get-ChildItem/);
  assert.match(doc, /### desktop workflow sandbox autofix local mirror/);
  assert.match(doc, /sandbox autofix/i);
  assert.match(doc, /retired_at/i);
  assert.match(doc, /workflowSandboxAutoFixProvider=local_legacy/i);
  assert.match(gate, /cleaning default rust_v2 fallback to python_legacy/i);
  assert.match(doc, /cleaning default rust_v2 fallback to python_legacy/i);
});
