const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function parseJsonLine(output) {
  const lines = String(output || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (!line.startsWith("{") || !line.endsWith("}")) {
      continue;
    }
    return JSON.parse(line);
  }
  return null;
}

test("governance control plane boundary gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_governance_control_plane_boundary.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.equal(payload.schemaVersion, "governance_surface.v1");
  assert.equal(payload.controlPlaneStatus, "effective_second_control_plane");
  assert.equal(payload.controlPlaneRole, "governance_state");
  assert.equal(payload.governanceStateControlPlaneOwner, "glue-python");
  assert.equal(payload.jobLifecycleControlPlaneOwner, "base-java");
  assert.equal(payload.metaRoute, "/governance/meta/control-plane");
  assert.match(payload.manifestPath, /governance_capabilities\.v1\.json/i);
  assert.match(payload.desktopGeneratedPath, /workflow_governance_capabilities\.generated\.js/i);
  assert.match(payload.winUiGeneratedPath, /GovernanceCapabilities\.Generated\.cs/i);
  assert.ok(payload.surfaceCount >= 8);
  assert.ok(payload.governanceRouteCount >= payload.coveredGovernanceRouteCount);
  assert.deepEqual(payload.drift.uncoveredGovernanceRoutes, []);
  assert.deepEqual(payload.drift.duplicateOwnedRoutePrefixes, []);
  assert.deepEqual(payload.drift.invalidControlPlaneRoles, []);
  assert.deepEqual(payload.drift.lifecycleMutationAllowed, []);
  assert.deepEqual(payload.drift.manifestCapabilityDrift, []);
  assert.deepEqual(payload.drift.desktopGeneratedCapabilityDrift, []);
  assert.deepEqual(payload.drift.winUiGeneratedCapabilityDrift, []);
});

test("governance control plane boundary gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_governance_control_plane_boundary.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-RequireGovernanceRoutes",
    "__missing_governance_route__",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing a missing governance route should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.deepEqual(payload.drift.missingRequiredRoutes, ["__missing_governance_route__"]);
  assert.ok(Array.isArray(payload.drift.manifestCapabilityDrift));
  assert.ok(Array.isArray(payload.drift.desktopGeneratedCapabilityDrift));
  assert.ok(Array.isArray(payload.drift.winUiGeneratedCapabilityDrift));
  assert.match(payload.issues.join("\n"), /required governance routes missing/i);
});
