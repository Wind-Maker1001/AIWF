const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadModule(relPath) {
  const file = pathToFileURL(path.resolve(__dirname, relPath)).href;
  return import(file);
}

test("workflow defaults catalog keeps Rust palette surface aligned to renderer manifest", async () => {
  const [{ NODE_CATALOG }, rendererManifest, localPolicy] = await Promise.all([
    loadModule("../renderer/workflow/defaults-catalog.js"),
    loadModule("../renderer/workflow/rust_operator_manifest.generated.js"),
    loadModule("../renderer/workflow/local-node-palette-policy.js"),
  ]);

  const catalogTypes = NODE_CATALOG.map((item) => String(item?.type || "").trim()).filter(Boolean).sort();
  const catalogRustTypes = catalogTypes.filter((type) => rendererManifest.KNOWN_RUST_OPERATOR_TYPES.includes(type));
  const catalogLocalTypes = catalogTypes.filter((type) => localPolicy.LOCAL_NODE_TYPES.includes(type));
  const visibleRustTypes = [...rendererManifest.DESKTOP_RUST_OPERATOR_TYPES].filter(
    (type) => !rendererManifest.DESKTOP_RUST_OPERATOR_METADATA[type]?.palette_hidden,
  );
  assert.deepEqual(catalogRustTypes, visibleRustTypes);
  assert.deepEqual(catalogLocalTypes, [...localPolicy.LOCAL_NODE_TYPES].sort());
  assert.equal(rendererManifest.DESKTOP_RUST_OPERATOR_TYPES.includes("postprocess_rows_v1"), true);
  assert.equal(rendererManifest.DESKTOP_RUST_OPERATOR_METADATA.postprocess_rows_v1.palette_hidden, true);
  assert.equal(catalogRustTypes.includes("postprocess_rows_v1"), false);

  for (const entry of NODE_CATALOG.filter((item) => rendererManifest.KNOWN_RUST_OPERATOR_TYPES.includes(String(item?.type || "").trim()))) {
    assert.ok(String(entry.name || "").trim(), `rust catalog entry name missing for ${entry.type}`);
    assert.ok(String(entry.desc || "").trim(), `rust catalog entry desc missing for ${entry.type}`);
    assert.ok(String(entry.group || "").trim(), `rust catalog entry group missing for ${entry.type}`);
    assert.ok(String(entry.policy_section || "").trim(), `rust catalog entry policy_section missing for ${entry.type}`);
    assert.equal(String(entry.policy_source || "").trim(), "rust_manifest", `rust catalog entry policy_source missing for ${entry.type}`);
  }

  for (const entry of NODE_CATALOG.filter((item) => localPolicy.LOCAL_NODE_TYPES.includes(String(item?.type || "").trim()))) {
    assert.ok(String(entry.name || "").trim(), `local catalog entry name missing for ${entry.type}`);
    assert.ok(String(entry.desc || "").trim(), `local catalog entry desc missing for ${entry.type}`);
    assert.ok(String(entry.group || "").trim(), `local catalog entry group missing for ${entry.type}`);
    assert.ok(String(entry.policy_section || "").trim(), `local catalog entry policy_section missing for ${entry.type}`);
    assert.equal(String(entry.policy_source || "").trim(), "local_policy", `local catalog entry policy_source missing for ${entry.type}`);
  }
});
