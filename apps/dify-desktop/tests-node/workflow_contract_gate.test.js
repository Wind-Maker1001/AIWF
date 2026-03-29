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

test("workflow contract sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_workflow_contract_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.deepEqual(payload.required, ["workflow_id", "version", "nodes", "edges"]);
  assert.equal(payload.defaultVersion, "1.0.0");
  assert.equal(payload.importMigrated, true);
  assert.equal(payload.importRejectedUnknownType, true);
  assert.equal(payload.payloadRejectedUnknownType, true);
  assert.equal(payload.authoringRejectedUnknownType, true);
  assert.equal(payload.preflightUnknownTypeGuided, true);
  assert.equal(payload.normalizedVersion, "1.0.0");
  assert.equal(payload.runPayloadDefersUnknownType, true);
  assert.equal(payload.engineUsesRustValidation, true);
  assert.equal(payload.preflightUsesRustWorkflowValidation, true);
  assert.equal(payload.flowIoAvoidsLocalAssert, true);
  assert.equal(payload.runPayloadAvoidsLocalAssert, true);
  assert.equal(payload.rustUnavailableFailsClosed, true);
});
