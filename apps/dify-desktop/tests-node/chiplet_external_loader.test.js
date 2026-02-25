const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const { parsePluginDirs, loadExternalChiplets } = require("../workflow_chiplets/external_loader");

test("parsePluginDirs merges config and env", () => {
  const prev = process.env.AIWF_CHIPLET_PLUGIN_DIRS;
  process.env.AIWF_CHIPLET_PLUGIN_DIRS = "C:\\p1;C:\\p2";
  try {
    const dirs = parsePluginDirs({ chiplet_plugin_dirs: ["C:\\p3", "C:\\p2"] });
    assert.deepEqual(dirs, ["C:\\p3", "C:\\p2", "C:\\p1"]);
  } finally {
    if (typeof prev === "undefined") delete process.env.AIWF_CHIPLET_PLUGIN_DIRS;
    else process.env.AIWF_CHIPLET_PLUGIN_DIRS = prev;
  }
});

test("loadExternalChiplets loads plugin and registers node type", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-chiplet-"));
  const pluginsDir = path.join(root, "plugins");
  const pluginA = path.join(pluginsDir, "pluginA");
  fs.mkdirSync(pluginA, { recursive: true });
  fs.writeFileSync(path.join(pluginA, "manifest.json"), JSON.stringify({ entry: "index.js", enabled: true }), "utf8");
  fs.writeFileSync(
    path.join(pluginA, "index.js"),
    [
      "module.exports = {",
      "  register(registry) {",
      "    registry.register('ext_dummy_v1', {",
      "      id: 'chiplet.ext_dummy_v1',",
      "      async run() { return { ok: true }; },",
      "    });",
      "  },",
      "};",
      "",
    ].join("\n"),
    "utf8",
  );

  const registry = new WorkflowChipletRegistry();
  const out = loadExternalChiplets({
    fs,
    path,
    registry,
    config: { chiplet_plugin_dirs: [pluginsDir] },
  });
  assert.equal(out.scanned_plugins, 1);
  assert.equal(out.failed_plugins, 0);
  assert.equal(registry.has("ext_dummy_v1"), true);
});

