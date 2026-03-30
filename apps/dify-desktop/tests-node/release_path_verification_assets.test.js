const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("release and package scripts keep sidecar evidence as hard gates", () => {
  const packageOffline = readText("ops/scripts/package_offline_bundle.ps1");
  const nativeRelease = readText("ops/scripts/release_frontend_productize.ps1");
  const compatRelease = readText("ops/scripts/release_productize.ps1");

  assert.match(packageOffline, /Assert-SidecarReportReady/);
  assert.match(packageOffline, /sidecar regression package gate passed/i);
  assert.match(packageOffline, /sidecar python\/rust consistency package gate passed/i);
  assert.match(packageOffline, /sidecar_regression_quality_report\.json/i);
  assert.match(packageOffline, /sidecar_python_rust_consistency_report\.json/i);

  assert.match(nativeRelease, /Assert-SidecarReportReady/);
  assert.match(nativeRelease, /sidecar regression release gate passed/i);
  assert.match(nativeRelease, /sidecar python\/rust consistency release gate passed/i);

  assert.match(compatRelease, /Assert-SidecarReportReady/);
  assert.match(compatRelease, /sidecar regression release gate passed/i);
  assert.match(compatRelease, /sidecar python\/rust consistency release gate passed/i);
});

test("desktop fixture dependency guidance stays explicit in docs and scripts", () => {
  const depsScript = readText("ops/scripts/check_desktop_fixture_deps.ps1");
  const quickstart = readText("docs/quickstart_desktop_offline.md");
  const verification = readText("docs/verification.md");
  const regression = readText("docs/regression_quality.md");

  assert.match(depsScript, /exceljs/i);
  assert.match(depsScript, /desktop fixture dependency check failed/i);
  assert.match(quickstart, /check_desktop_fixture_deps\.ps1/i);
  assert.match(quickstart, /exceljs/i);
  assert.match(quickstart, /run_sidecar_python_rust_consistency\.ps1 -RequireAccel/i);
  assert.match(verification, /run_sidecar_regression_quality\.ps1/i);
  assert.match(verification, /run_sidecar_python_rust_consistency\.ps1 -RequireAccel/i);
  assert.match(verification, /check_desktop_fixture_deps\.ps1/i);
  assert.match(regression, /check_desktop_fixture_deps\.ps1/i);
});
