const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadModule(relPath) {
  const file = pathToFileURL(path.resolve(__dirname, relPath)).href;
  return import(file);
}

test("rust operator presentations cover desktop-exposable renderer operators without stale entries", async () => {
  const [rendererManifest, presentations] = await Promise.all([
    loadModule("../renderer/workflow/rust_operator_manifest.generated.js"),
    loadModule("../renderer/workflow/rust-operator-presentations.js"),
  ]);

  const presentationMap = presentations.RUST_OPERATOR_PRESENTATIONS;
  const presentationTypes = Object.keys(presentationMap).sort();
  assert.deepEqual(presentationTypes, [...rendererManifest.DESKTOP_RUST_OPERATOR_TYPES]);

  for (const type of presentationTypes) {
    const item = presentationMap[type];
    assert.ok(item && typeof item === "object" && !Array.isArray(item), `${type} presentation must be an object`);
    assert.ok(String(item.name || "").trim(), `${type} presentation.name must not be empty`);
    assert.ok(String(item.desc || "").trim(), `${type} presentation.desc must not be empty`);
  }
});
