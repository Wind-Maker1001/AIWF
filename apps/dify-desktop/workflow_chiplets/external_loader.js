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

function safeReadJson(fs, fp) {
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function loadOnePlugin({ fs, path, registry, pluginRoot }) {
  const manifestPath = path.join(pluginRoot, "manifest.json");
  const manifest = fs.existsSync(manifestPath) ? safeReadJson(fs, manifestPath) : null;
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
  // eslint-disable-next-line global-require, import/no-dynamic-require
  const mod = require(entryPath);
  if (!mod || typeof mod.register !== "function") {
    return { ok: false, plugin: pluginRoot, error: "plugin must export register(registry, ctx)" };
  }
  const before = new Set(registry.list());
  const ctx = {
    plugin_root: pluginRoot,
    manifest: manifest || {},
  };
  mod.register(registry, ctx);
  const after = registry.list();
  const added = after.filter((t) => !before.has(t));
  return {
    ok: true,
    plugin: pluginRoot,
    manifest: manifest || {},
    added_types: added,
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
  const pluginDirs = parsePluginDirs(config);
  const roots = collectPluginRoots(fs, path, pluginDirs);
  const items = roots.map((pluginRoot) => {
    try {
      return loadOnePlugin({ fs, path, registry, pluginRoot });
    } catch (e) {
      return { ok: false, plugin: pluginRoot, error: String(e) };
    }
  });
  return {
    plugin_dirs: pluginDirs,
    scanned_plugins: roots.length,
    loaded_plugins: items.filter((x) => x.ok && !x.skipped).length,
    failed_plugins: items.filter((x) => !x.ok).length,
    items,
  };
}

module.exports = {
  parsePluginDirs,
  loadExternalChiplets,
};

