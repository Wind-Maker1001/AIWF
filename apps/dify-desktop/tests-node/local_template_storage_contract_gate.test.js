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

test("local template storage contract sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_local_template_storage_contract_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.schemaPath, /contracts[\\\/]desktop[\\\/]local_template_storage\.schema\.json/i);
  assert.equal(payload.storageSchemaVersion, "local_template_storage.v1");
  assert.equal(payload.entrySchemaVersion, "local_template_entry.v1");
  assert.equal(payload.legacyStorageMigrated, true);
  assert.equal(payload.localStorageNormalizedOnLoad, true);
  assert.equal(payload.localSaveVersioned, true);
  assert.ok(payload.savedEntryCount >= 1);
});
