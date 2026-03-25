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

test("offline template catalog sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_offline_template_catalog_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.schemaPaths.theme, /office_theme_catalog\.schema\.json/i);
  assert.match(payload.schemaPaths.layout, /office_layout_catalog\.schema\.json/i);
  assert.match(payload.schemaPaths.registry, /cleaning_template_registry\.schema\.json/i);
  assert.match(payload.schemaPaths.pack_manifest, /offline_template_catalog_pack_manifest\.schema\.json/i);
  assert.equal(payload.schemaVersions.theme, "office_theme_catalog.v1");
  assert.equal(payload.schemaVersions.layout, "office_layout_catalog.v1");
  assert.equal(payload.schemaVersions.registry, "cleaning_template_registry.v1");
  assert.equal(payload.migratedLegacy.theme, true);
  assert.equal(payload.migratedLegacy.layout, true);
  assert.equal(payload.migratedLegacy.registry, true);
  assert.ok(payload.runtime.cleaningTemplateCount >= 2);
  assert.equal(payload.packManager.exported, true);
  assert.equal(payload.packManager.imported, true);
  assert.equal(payload.packManager.generic_template_roundtrip, true);
  assert.equal(payload.packManager.packManifestSchemaVersion, "offline_template_catalog_pack_manifest.v1");
});
