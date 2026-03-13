const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("fs");
const os = require("os");
const path = require("path");
const { WorkflowChipletRegistry } = require("../workflow_chiplets/registry");
const crypto = require("crypto");
const {
  parsePluginDirs,
  parsePluginCapabilityAllowlist,
  buildPluginSignatureBase,
  loadExternalChiplets,
} = require("../workflow_chiplets/external_loader");

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

test("parsePluginCapabilityAllowlist merges config and env", () => {
  const prev = process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST;
  process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST = "file.read;net.none";
  try {
    const caps = parsePluginCapabilityAllowlist({
      chiplet_plugin_capability_allowlist: ["workflow.read", "net.none"],
    });
    assert.deepEqual(caps, ["workflow.read", "net.none", "file.read"]);
  } finally {
    if (typeof prev === "undefined") delete process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST;
    else process.env.AIWF_CHIPLET_CAPABILITY_ALLOWLIST = prev;
  }
});

test("loadExternalChiplets loads plugin and registers node type", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-chiplet-"));
  const pluginsDir = path.join(root, "plugins");
  const pluginA = path.join(pluginsDir, "pluginA");
  fs.mkdirSync(pluginA, { recursive: true });
  fs.writeFileSync(path.join(pluginA, "manifest.json"), JSON.stringify({
    name: "pluginA",
    version: "1.0.0",
    api_version: "v1",
    entry: "index.js",
    enabled: true,
  }), "utf8");
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

test("loadExternalChiplets blocks capability not in allowlist", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-chiplet-"));
  const pluginsDir = path.join(root, "plugins");
  const pluginA = path.join(pluginsDir, "pluginA");
  fs.mkdirSync(pluginA, { recursive: true });
  fs.writeFileSync(path.join(pluginA, "manifest.json"), JSON.stringify({
    name: "pluginA",
    version: "1.0.0",
    api_version: "v1",
    capabilities: ["net.call_external"],
    entry: "index.js",
    enabled: true,
  }), "utf8");
  fs.writeFileSync(path.join(pluginA, "index.js"), "module.exports={register(){}};\n", "utf8");

  const registry = new WorkflowChipletRegistry();
  const out = loadExternalChiplets({
    fs,
    path,
    registry,
    config: {
      chiplet_plugin_dirs: [pluginsDir],
      chiplet_plugin_capability_allowlist: ["workflow.read"],
    },
  });
  assert.equal(out.failed_plugins, 1);
  assert.match(String(out.items[0]?.error || ""), /plugin_capability_not_allowed/i);
});

test("loadExternalChiplets validates plugin signature when secret is set", () => {
  const prev = process.env.AIWF_CHIPLET_SIGNING_SECRET;
  process.env.AIWF_CHIPLET_SIGNING_SECRET = "unit-secret";
  try {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-chiplet-"));
    const pluginsDir = path.join(root, "plugins");
    const pluginA = path.join(pluginsDir, "pluginA");
    fs.mkdirSync(pluginA, { recursive: true });
    const entrySrc = [
      "module.exports = {",
      "  register(registry) {",
      "    registry.register('ext_sig_v1', { id: 'chiplet.ext_sig_v1', async run(){ return { ok: true }; } });",
      "  },",
      "};",
      "",
    ].join("\n");
    fs.writeFileSync(path.join(pluginA, "index.js"), entrySrc, "utf8");
    const manifest = {
      name: "pluginSig",
      version: "1.0.0",
      api_version: "v1",
      entry: "index.js",
      enabled: true,
    };
    const base = buildPluginSignatureBase(manifest, entrySrc);
    manifest.signature_hmac_sha256 = crypto
      .createHmac("sha256", "unit-secret")
      .update(base, "utf8")
      .digest("hex");
    fs.writeFileSync(path.join(pluginA, "manifest.json"), JSON.stringify(manifest), "utf8");

    const registry = new WorkflowChipletRegistry();
    const out = loadExternalChiplets({
      fs,
      path,
      registry,
      config: { chiplet_plugin_dirs: [pluginsDir] },
    });
    assert.equal(out.failed_plugins, 0);
    assert.equal(registry.has("ext_sig_v1"), true);
  } finally {
    if (typeof prev === "undefined") delete process.env.AIWF_CHIPLET_SIGNING_SECRET;
    else process.env.AIWF_CHIPLET_SIGNING_SECRET = prev;
  }
});

test("loadExternalChiplets blocks plugin before executing module side effects when allowlist rejects it", () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-chiplet-"));
  const pluginsDir = path.join(root, "plugins");
  const pluginA = path.join(pluginsDir, "pluginA");
  const sideEffectFile = path.join(root, "side-effect.txt");
  fs.mkdirSync(pluginA, { recursive: true });
  fs.writeFileSync(path.join(pluginA, "manifest.json"), JSON.stringify({
    name: "pluginA",
    version: "1.0.0",
    api_version: "v1",
    entry: "index.js",
    enabled: true,
  }), "utf8");
  fs.writeFileSync(
    path.join(pluginA, "index.js"),
    [
      "const fs = require('node:fs');",
      `fs.writeFileSync(${JSON.stringify(sideEffectFile)}, 'executed', 'utf8');`,
      "module.exports = {",
      "  register(registry) {",
      "    registry.register('ext_blocked_v1', { id: 'chiplet.ext_blocked_v1', async run(){ return { ok: true }; } });",
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
    config: {
      chiplet_plugin_dirs: [pluginsDir],
      chiplet_plugin_allowlist: ["pluginB"],
    },
  });
  assert.equal(out.failed_plugins, 1);
  assert.match(String(out.items[0]?.error || ""), /plugin_not_in_allowlist/i);
  assert.equal(registry.has("ext_blocked_v1"), false);
  assert.equal(fs.existsSync(sideEffectFile), false);
});
