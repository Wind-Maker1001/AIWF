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

test("template pack contract sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_template_pack_contract_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.schemaPath, /contracts[\\\/]desktop[\\\/]template_pack_artifact\.schema\.json/i);
  assert.equal(payload.artifactSchemaVersion, "template_pack_artifact.v1");
  assert.equal(payload.marketplaceEntrySchemaVersion, "template_pack_entry.v1");
  assert.equal(payload.importMigrated, true);
  assert.equal(payload.installMigrated, true);
  assert.equal(payload.exportedArtifactSchemaVersion, "template_pack_artifact.v1");
  assert.equal(payload.templateCount, 1);
});
