const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadModule(relPath) {
  const file = pathToFileURL(path.resolve(__dirname, relPath)).href;
  return import(file);
}

test("rust operator palette policy covers desktop-exposable operators with explicit sections", async () => {
  const [rendererManifest, presentations, palettePolicy] = await Promise.all([
    loadModule("../renderer/workflow/rust_operator_manifest.generated.js"),
    loadModule("../renderer/workflow/rust-operator-presentations.js"),
    loadModule("../renderer/workflow/rust-operator-palette-policy.js"),
  ]);

  const result = palettePolicy.buildRustOperatorPalettePolicy(
    rendererManifest.DESKTOP_RUST_OPERATOR_METADATA,
    presentations.RUST_OPERATOR_PRESENTATIONS,
  );
  assert.equal(result.ok, true, result.errors.join("\n"));
  assert.deepEqual(result.details.missingPaletteSectionDomains, []);
  assert.deepEqual(result.details.invalidSections, []);
  assert.deepEqual(result.details.stalePinnedOperators, []);
  assert.deepEqual(result.details.duplicatePinnedOperators, []);

  const types = result.entries.map((entry) => entry.type);
  const visibleRustTypes = [...rendererManifest.DESKTOP_RUST_OPERATOR_TYPES].filter(
    (type) => !rendererManifest.DESKTOP_RUST_OPERATOR_METADATA[type]?.palette_hidden,
  );
  assert.deepEqual(types.sort(), visibleRustTypes);
  assert.equal(types.includes("postprocess_rows_v1"), false);
  for (const entry of result.entries) {
    assert.ok(String(entry.group || "").trim(), `${entry.type} group must not be empty`);
    assert.ok(String(entry.policy_section || "").trim(), `${entry.type} policy_section must not be empty`);
    assert.equal(String(entry.policy_source || "").trim(), "rust_manifest");
  }
});
