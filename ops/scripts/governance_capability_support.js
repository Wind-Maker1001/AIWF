const fs = require("fs");

function uniqueInOrder(values) {
  const out = [];
  const seen = new Set();
  for (const value of Array.isArray(values) ? values : []) {
    const normalized = String(value || "").trim();
    if (!normalized || seen.has(normalized)) continue;
    seen.add(normalized);
    out.push(normalized);
  }
  return out;
}

function loadGovernanceCapabilityManifest(manifestPath) {
  const payload = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const normalizedItems = items.map((item) => ({
    constant: String(item?.constant || "").trim(),
    capability: String(item?.capability || "").trim(),
    route_prefix: String(item?.route_prefix || "").trim(),
    owned_route_prefixes: uniqueInOrder(item?.owned_route_prefixes || []),
  })).filter((item) => item.constant && item.capability && item.route_prefix);

  const constantMap = {};
  for (const item of normalizedItems) {
    constantMap[item.constant] = {
      capability: item.capability,
      route_prefix: item.route_prefix,
      owned_route_prefixes: item.owned_route_prefixes,
    };
  }

  return {
    schemaVersion: String(payload?.schema_version || "").trim(),
    sourceAuthority: String(payload?.source_authority || "").trim(),
    items: normalizedItems,
    constantMap,
  };
}

function capabilityToConstant(capability) {
  return String(capability || "")
    .trim()
    .replace(/[^A-Za-z0-9]+/g, "_")
    .replace(/^_+|_+$/g, "")
    .toUpperCase();
}

function routeLeafToConstant(routePrefix) {
  const leaf = String(routePrefix || "")
    .trim()
    .replace(/^\/+|\/+$/g, "")
    .split("/")
    .pop() || "";
  return capabilityToConstant(leaf);
}

function buildGovernanceCapabilityDataFromSurfaceExport(surfaceExport) {
  const payload = surfaceExport && typeof surfaceExport === "object" ? surfaceExport : {};
  const items = Array.isArray(payload?.items) ? payload.items : [];
  const normalizedItems = items.map((item) => {
    const capability = String(item?.capability || "").trim();
    const routePrefix = String(item?.route_prefix || "").trim();
    const ownedRoutePrefixes = uniqueInOrder(item?.owned_route_prefixes || []);
    const routeConstantMap = { PRIMARY: routePrefix };
    for (const ownedRoutePrefix of ownedRoutePrefixes) {
      const key = routeLeafToConstant(ownedRoutePrefix);
      if (key && !routeConstantMap[key]) {
        routeConstantMap[key] = ownedRoutePrefix;
      }
    }
    return {
      constant: capabilityToConstant(capability),
      capability,
      route_prefix: routePrefix,
      owned_route_prefixes: ownedRoutePrefixes,
      route_constant_map: routeConstantMap,
    };
  }).filter((item) => item.constant && item.capability && item.route_prefix);

  const constantMap = {};
  const routeConstantsByConstant = {};
  for (const item of normalizedItems) {
    constantMap[item.constant] = {
      capability: item.capability,
      route_prefix: item.route_prefix,
      owned_route_prefixes: item.owned_route_prefixes,
    };
    routeConstantsByConstant[item.constant] = item.route_constant_map;
  }

  return {
    schemaVersion: String(payload?.schema_version || "").trim() || "governance_capabilities.v1",
    sourceAuthority: String(payload?.source_authority || "").trim() || "apps/glue-python/aiwf/governance_surface.py",
    items: normalizedItems,
    constantMap,
    routeConstantsByConstant,
  };
}

function renderManifestJson(data) {
  return `${JSON.stringify({
    schema_version: data.schemaVersion,
    source_authority: data.sourceAuthority,
    items: data.items.map((item) => ({
      constant: item.constant,
      capability: item.capability,
      route_prefix: item.route_prefix,
      owned_route_prefixes: item.owned_route_prefixes,
    })),
  }, null, 2)}\n`;
}

function renderDesktopModule(data) {
  return [
    `const GOVERNANCE_CAPABILITY_SCHEMA_VERSION = ${JSON.stringify(data.schemaVersion)};`,
    `const GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY = ${JSON.stringify(data.sourceAuthority)};`,
    `const GOVERNANCE_CAPABILITY_ITEMS = Object.freeze(${JSON.stringify(data.items, null, 2)});`,
    `const GOVERNANCE_CAPABILITIES = Object.freeze(${JSON.stringify(data.constantMap, null, 2)});`,
    `const GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS = Object.freeze(${JSON.stringify(data.routeConstantsByConstant, null, 2)});`,
    "",
    "module.exports = {",
    "  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,",
    "  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,",
    "  GOVERNANCE_CAPABILITY_ITEMS,",
    "  GOVERNANCE_CAPABILITIES,",
    "  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,",
    "};",
    "",
  ].join("\n");
}

function renderWinUiModule(data) {
  const lines = [];
  lines.push("namespace AIWF.Native.Runtime;");
  lines.push("");
  lines.push("public static class GovernanceCapabilitiesGenerated");
  lines.push("{");
  lines.push(`    public const string SchemaVersion = ${JSON.stringify(data.schemaVersion)};`);
  lines.push(`    public const string SourceAuthority = ${JSON.stringify(data.sourceAuthority)};`);
  lines.push("");
  for (const item of data.items) {
    lines.push(`    public const string ${item.constant} = ${JSON.stringify(item.capability)};`);
    lines.push(`    public const string ${item.constant}_ROUTE_PREFIX = ${JSON.stringify(item.route_prefix)};`);
    for (const [routeKey, routeValue] of Object.entries(item.route_constant_map || {})) {
      if (routeKey === "PRIMARY") {
        continue;
      }
      lines.push(`    public const string ${item.constant}_${routeKey}_ROUTE_PREFIX = ${JSON.stringify(routeValue)};`);
    }
  }
  lines.push("}");
  lines.push("");
  return lines.join("\n");
}

module.exports = {
  buildGovernanceCapabilityDataFromSurfaceExport,
  capabilityToConstant,
  loadGovernanceCapabilityManifest,
  routeLeafToConstant,
  renderManifestJson,
  renderDesktopModule,
  renderWinUiModule,
};
