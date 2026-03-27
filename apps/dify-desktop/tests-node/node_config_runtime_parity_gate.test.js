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

test("node config runtime parity gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_node_config_runtime_parity.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.fixturePath, /node_config_contract_fixtures\.v1\.json/i);
  assert.match(payload.fixtureSchemaPath, /node_config_contract_fixtures\.schema\.json/i);
  assert.match(payload.errorContractPath, /node_config_validation_errors\.v1\.json/i);
  assert.match(payload.errorContractSchemaPath, /node_config_validation_errors\.schema\.json/i);
  assert.ok(payload.declaredErrorCodeCount >= 20);
  assert.equal(payload.fixtureTypeCount, 30);
  assert.equal(payload.validCaseCount, 30);
  assert.equal(payload.invalidCaseCount, 30);
  assert.equal(payload.caseCount, 60);
  assert.deepEqual(payload.drift.missingRequiredFixtureTypes, []);
  assert.deepEqual(payload.drift.fixtureTypesMissingFromContract, []);
  assert.deepEqual(payload.drift.fixtureUndeclaredErrorCodes, []);
  assert.deepEqual(payload.drift.jsUndeclaredErrorCodes, []);
  assert.deepEqual(payload.drift.pythonUndeclaredErrorCodes, []);
  assert.deepEqual(payload.drift.declaredMissingInJs, []);
  assert.deepEqual(payload.drift.declaredMissingInPython, []);
  assert.deepEqual(payload.drift.jsExpectationFailures, []);
  assert.deepEqual(payload.drift.pythonExpectationFailures, []);
  assert.deepEqual(payload.drift.runtimeStatusDrift, []);
  assert.deepEqual(payload.drift.runtimeErrorItemDrift, []);
  assert.deepEqual(payload.drift.runtimeErrorDrift, []);
});

test("node config runtime parity gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_node_config_runtime_parity.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-RequiredParityNodeTypes",
    "__missing_node_type__",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing a missing fixture node type should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.deepEqual(payload.drift.missingRequiredFixtureTypes, ["__missing_node_type__"]);
  assert.ok(Array.isArray(payload.drift.fixtureUndeclaredErrorCodes));
  assert.ok(Array.isArray(payload.drift.jsUndeclaredErrorCodes));
  assert.ok(Array.isArray(payload.drift.pythonUndeclaredErrorCodes));
  assert.ok(Array.isArray(payload.drift.declaredMissingInJs));
  assert.ok(Array.isArray(payload.drift.declaredMissingInPython));
  assert.ok(Array.isArray(payload.drift.runtimeStatusDrift));
  assert.ok(Array.isArray(payload.drift.runtimeErrorItemDrift));
  assert.ok(Array.isArray(payload.drift.runtimeErrorDrift));
  assert.match(payload.issues.join("\n"), /missing required parity fixture node types/i);
});
