const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("ci check wires sidecar python/rust consistency as a first-class gate", () => {
  const ciCheck = readText("ops/scripts/ci_check.ps1");
  const backendQuickstart = readText("docs/quickstart_backend.md");

  assert.match(ciCheck, /SkipSidecarPythonRustConsistency/);
  assert.match(ciCheck, /run_sidecar_python_rust_consistency\.ps1/);
  assert.match(ciCheck, /Ensure-AccelRustService/);
  assert.match(ciCheck, /Stop-AccelRustService/);
  assert.match(ciCheck, /-RequireAccel/);
  assert.match(ciCheck, /skip sidecar python\/rust consistency checks/i);
  assert.match(ciCheck, /node_modules\\exceljs\\package\.json/i);

  const quickSkipMatch = ciCheck.match(/\$quickSkipParams = @\(([\s\S]*?)\n  \)/);
  const compatibilitySkipMatch = ciCheck.match(/\$compatibilitySkipParams = @\(([\s\S]*?)\n  \)/);
  assert.ok(quickSkipMatch, "quick profile skip list not found");
  assert.ok(compatibilitySkipMatch, "compatibility profile skip list not found");
  assert.match(quickSkipMatch[1], /"SkipSidecarPythonRustConsistency"/);
  assert.match(compatibilitySkipMatch[1], /"SkipSidecarPythonRustConsistency"/);

  assert.match(backendQuickstart, /run_sidecar_python_rust_consistency\.ps1/i);
  assert.match(backendQuickstart, /-RequireEnhancedIngest/i);
});
