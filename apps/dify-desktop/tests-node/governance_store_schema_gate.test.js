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

test("governance store schema version gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_governance_store_schema_versions.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.ok(payload.sourceModuleCount >= 7);
  assert.equal(payload.sourceSchemaVersionCount, payload.sourceModuleCount);
  assert.ok(payload.runtimeCheckCount >= 12);
  assert.equal(payload.runtimeSchemaVersionCount, payload.runtimeCheckCount);
  assert.deepEqual(payload.drift.missingSourceSchemaVersionModules, []);
  assert.deepEqual(payload.drift.missingRuntimeSchemaVersionOutputs, []);
});

test("governance store schema version gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_governance_store_schema_versions.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-RequireRuntimeOutputs",
    "__missing_runtime_output__",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing a missing runtime output should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.deepEqual(payload.drift.missingRequiredRuntimeOutputs, ["__missing_runtime_output__"]);
  assert.match(payload.issues.join("\n"), /required runtime outputs missing/i);
});
