const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadModule(relPath) {
  const file = pathToFileURL(path.resolve(__dirname, relPath)).href;
  return import(file);
}

test("local node palette policy covers local workflow nodes without stale entries", async () => {
  const [presentations, palettePolicy] = await Promise.all([
    loadModule("../renderer/workflow/local-node-presentations.js"),
    loadModule("../renderer/workflow/local-node-palette-policy.js"),
  ]);

  const result = palettePolicy.buildLocalNodePalettePolicy(presentations.LOCAL_NODE_PRESENTATIONS);
  assert.equal(result.ok, true, result.errors.join("\n"));
  assert.deepEqual(result.details.invalidSections, []);
  assert.deepEqual(result.details.missingSectionTypes, []);
  assert.deepEqual(result.details.missingPresentationTypes, []);
  assert.deepEqual(result.details.stalePresentationTypes, []);
  assert.deepEqual(result.details.invalidPresentationEntries, []);
  assert.deepEqual(result.details.duplicatePinnedTypes, []);
  assert.deepEqual(result.details.stalePinnedTypes, []);
  assert.deepEqual(
    result.entries.map((entry) => entry.type).sort(),
    [...palettePolicy.LOCAL_NODE_TYPES].sort(),
  );
  for (const entry of result.entries) {
    assert.ok(String(entry.group || "").trim(), `${entry.type} group must not be empty`);
    assert.ok(String(entry.policy_section || "").trim(), `${entry.type} policy_section must not be empty`);
    assert.equal(String(entry.policy_source || "").trim(), "local_policy");
  }
});
