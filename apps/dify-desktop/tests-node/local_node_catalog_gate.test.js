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

test("local node catalog policy gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_local_node_catalog_policy.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.ok(payload.localNodeTypeCount > 0);
  assert.equal(payload.localCatalogCount, payload.localNodeTypeCount);
  assert.deepEqual(payload.drift.missingCatalogTypes, []);
  assert.deepEqual(payload.drift.catalogMetadataDrift, []);
});

test("local node catalog policy gate emits structured failure details", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_local_node_catalog_policy.ps1");
  const result = spawnSync("powershell", [
    "-ExecutionPolicy",
    "Bypass",
    "-File",
    script,
    "-RequiredLocalNodeTypes",
    "__missing_local_node__",
  ], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.notEqual(result.status, 0, "forcing a missing local node type should fail");
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "failed");
  assert.deepEqual(payload.requiredLocalNodeTypes, ["__missing_local_node__"]);
  assert.deepEqual(payload.drift.requiredLocalTypeMissing, ["__missing_local_node__"]);
  assert.match(payload.issues.join("\n"), /required local node types missing from policy truth/i);
});
