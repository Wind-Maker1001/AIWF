const {
  parsePluginCapabilityAllowlist,
  buildPluginSignatureBase,
  verifyPluginSignature,
  normalizeCapabilities,
  findBlockedCapabilities,
} = require("./domains/external_policy");

function parsePluginDirs(config = {}) {
  const fromCfg = Array.isArray(config?.chiplet_plugin_dirs) ? config.chiplet_plugin_dirs : [];
  const fromEnv = String(process.env.AIWF_CHIPLET_PLUGIN_DIRS || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const all = [...fromCfg, ...fromEnv]
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  return Array.from(new Set(all));
}
function parsePluginAllowlist(config = {}) {
  const fromCfg = Array.isArray(config?.chiplet_plugin_allowlist) ? config.chiplet_plugin_allowlist : [];
  const fromEnv = String(process.env.AIWF_CHIPLET_PLUGIN_ALLOWLIST || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const all = [...fromCfg, ...fromEnv]
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  return Array.from(new Set(all));
}

function safeReadJson(fs, fp) {
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function loadOnePlugin({ fs, path, registry, pluginRoot, capabilityAllowlist = [], signingSecret = "" }) {
  const manifestPath = path.join(pluginRoot, "manifest.json");
  const manifest = fs.existsSync(manifestPath) ? safeReadJson(fs, manifestPath) : null;
  if (!manifest || typeof manifest !== "object") {
    return { ok: false, plugin: pluginRoot, error: "manifest.json required" };
  }
  const name = String(manifest?.name || "").trim();
  const version = String(manifest?.version || "").trim();
  const apiVersion = String(manifest?.api_version || "").trim();
  if (!name || !version || !apiVersion) {
    return { ok: false, plugin: pluginRoot, error: "manifest fields name/version/api_version required" };
  }
  if (manifest && manifest.enabled === false) {
    return { ok: true, skipped: true, plugin: pluginRoot, reason: "disabled" };
  }
  const entry = manifest?.entry ? String(manifest.entry) : "index.js";
  const entryPath = path.resolve(pluginRoot, entry);
  if (!entryPath.startsWith(path.resolve(pluginRoot))) {
    return { ok: false, plugin: pluginRoot, error: "entry path escapes plugin root" };
  }
  if (!fs.existsSync(entryPath)) {
    return { ok: false, plugin: pluginRoot, error: `entry not found: ${entryPath}` };
  }
  const ext = String(path.extname(entryPath) || "").toLowerCase();
  if (![".js", ".cjs", ".mjs"].includes(ext)) {
    return { ok: false, plugin: pluginRoot, error: "entry extension must be .js/.cjs/.mjs" };
  }
  const entrySource = fs.readFileSync(entryPath, "utf8");
  const sigCheck = verifyPluginSignature(manifest, entrySource, signingSecret);
  if (!sigCheck.ok) {
    return { ok: false, plugin: pluginRoot, error: sigCheck.error };
  }
  const capabilities = normalizeCapabilities(manifest);
  const blocked = findBlockedCapabilities(capabilities, capabilityAllowlist);
  if (blocked.length > 0) {
    return { ok: false, plugin: pluginRoot, error: `plugin_capability_not_allowed:${blocked.join(",")}` };
  }
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const mod = require(entryPath);
  if (!mod || typeof mod.register !== "function") {
    return { ok: false, plugin: pluginRoot, error: "plugin must export register(registry, ctx)" };
  }
  const before = new Set(registry.list());
  const ctx = {
    plugin_root: pluginRoot,
    manifest: manifest || {},
    capabilities,
  };
  mod.register(registry, ctx);
  const after = registry.list();
  const added = after.filter((t) => !before.has(t));
  return {
    ok: true,
    plugin: pluginRoot,
    manifest: manifest || {},
    added_types: added,
    signature_verified: !sigCheck.skipped,
    capabilities,
  };
}

function collectPluginRoots(fs, path, pluginDirs = []) {
  const out = [];
  pluginDirs.forEach((dir) => {
    const root = String(dir || "").trim();
    if (!root || !fs.existsSync(root)) return;
    let entries = [];
    try {
      entries = fs.readdirSync(root, { withFileTypes: true });
    } catch {
      return;
    }
    entries
      .filter((d) => d && d.isDirectory && d.isDirectory())
      .forEach((d) => out.push(path.join(root, d.name)));
  });
  return out;
}

function loadExternalChiplets({ fs, path, registry, config = {} }) {
  const packaged = !!config.__app_is_packaged;
  const allowPackaged = String(process.env.AIWF_ALLOW_PACKAGED_EXTERNAL_CHIPLETS || "").trim() === "1";
  if (packaged && !allowPackaged) {
    return {
      plugin_dirs: [],
      scanned_plugins: 0,
      loaded_plugins: 0,
      failed_plugins: 0,
      items: [],
      disabled_reason: "packaged_external_plugins_disabled",
    };
  }
  const pluginDirs = parsePluginDirs(config);
  const allowlist = parsePluginAllowlist(config);
  const capabilityAllowlist = parsePluginCapabilityAllowlist(config);
  const signingSecret = String(process.env.AIWF_CHIPLET_SIGNING_SECRET || "").trim();
  const roots = collectPluginRoots(fs, path, pluginDirs);
  const items = roots.map((pluginRoot) => {
    try {
      const manifestPath = path.join(pluginRoot, "manifest.json");
      const manifest = fs.existsSync(manifestPath) ? safeReadJson(fs, manifestPath) : null;
      const pluginName = String(manifest?.name || "").trim();
      if (allowlist.length > 0 && pluginName && !allowlist.includes(pluginName)) {
        return { ok: false, plugin: pluginRoot, error: `plugin_not_in_allowlist:${pluginName}` };
      }
      const res = loadOnePlugin({
        fs,
        path,
        registry,
        pluginRoot,
        capabilityAllowlist,
        signingSecret,
      });
      return res;
    } catch (e) {
      return { ok: false, plugin: pluginRoot, error: String(e) };
    }
  });
  return {
    plugin_dirs: pluginDirs,
    capability_allowlist: capabilityAllowlist,
    scanned_plugins: roots.length,
    loaded_plugins: items.filter((x) => x.ok && !x.skipped).length,
    failed_plugins: items.filter((x) => !x.ok).length,
    items,
  };
}

module.exports = {
  parsePluginDirs,
  parsePluginAllowlist,
  parsePluginCapabilityAllowlist,
  buildPluginSignatureBase,
  loadExternalChiplets,
};
