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

test("node config schema coverage gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_node_config_schema_coverage.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.contractPath, /node_config_contracts\.v1\.json/i);
  assert.match(payload.contractCjsPath, /workflow_node_config_contract\.generated\.js/i);
  assert.match(payload.contractEsmPath, /node_config_contract\.generated\.js/i);
  assert.equal(payload.contractAuthority, "contracts/desktop/node_config_contracts.v1.json");
  assert.equal(payload.contractSchemaVersion, "node_config_contracts.v1");
  assert.equal(payload.targetCount, 31);
  assert.equal(payload.coveredCount, 31);
  assert.ok(payload.contractValidatorKindCount >= 20);
  assert.equal(payload.minimumNestedShapeConstrained, 19);
  assert.equal(payload.nestedShapeConstrainedCount, 20);
  assert.deepEqual(
    payload.requiredNestedNodeTypes,
    [
      "load_rows_v3",
      "quality_check_v3",
      "quality_check_v4",
      "office_slot_fill_v1",
      "optimizer_v1",
      "parquet_io_v2",
      "plugin_registry_v1",
      "transform_rows_v3",
      "lineage_v3",
      "rule_simulator_v1",
      "constraint_solver_v1",
      "udf_wasm_v2",
    ],
  );
});

test("node config schema coverage gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_node_config_schema_coverage.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-MinimumNestedShapeConstrained",
    "999",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing an impossible threshold should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.equal(payload.minimumNestedShapeConstrained, 999);
  assert.ok(Array.isArray(payload.requiredNestedNodeTypes));
  assert.ok(Array.isArray(payload.requiredNestedMissing));
  assert.ok(Array.isArray(payload.issues));
  assert.match(payload.issues.join("\n"), /nested_shape_constrained count too low/i);
  assert.equal(
    payload.nestedShapeConstrainedDeficit,
    payload.minimumNestedShapeConstrained - payload.nestedShapeConstrainedCount,
  );
  assert.ok(payload.drift && Array.isArray(payload.drift.missingFromCatalog));
  assert.ok(Array.isArray(payload.drift.contractModuleTypeDrift));
  assert.ok(Array.isArray(payload.drift.contractModuleQualityDrift));
  assert.ok(Array.isArray(payload.drift.requiredNestedTypesMissingFromContract));
});
