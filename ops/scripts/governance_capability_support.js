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
  const capabilityByName = {};
  for (const item of data.items) {
    capabilityByName[item.capability] = {
      constant: item.constant,
      capability: item.capability,
      route_prefix: item.route_prefix,
      owned_route_prefixes: item.owned_route_prefixes,
      route_constant_map: item.route_constant_map,
    };
  }
  return [
    `const GOVERNANCE_CAPABILITY_SCHEMA_VERSION = ${JSON.stringify(data.schemaVersion)};`,
    `const GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY = ${JSON.stringify(data.sourceAuthority)};`,
    `const GOVERNANCE_CAPABILITY_ITEMS = Object.freeze(${JSON.stringify(data.items, null, 2)});`,
    `const GOVERNANCE_CAPABILITIES = Object.freeze(${JSON.stringify(data.constantMap, null, 2)});`,
    `const GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS = Object.freeze(${JSON.stringify(data.routeConstantsByConstant, null, 2)});`,
    `const GOVERNANCE_CAPABILITY_BY_NAME = Object.freeze(${JSON.stringify(capabilityByName, null, 2)});`,
    "",
    "function normalizeGovernanceCapability(capability) {",
    "  return String(capability || \"\").trim();",
    "}",
    "",
    "function getGovernanceCapabilityItem(capability) {",
    "  const normalizedCapability = normalizeGovernanceCapability(capability);",
    "  return normalizedCapability ? (GOVERNANCE_CAPABILITY_BY_NAME[normalizedCapability] || null) : null;",
    "}",
    "",
    "function resolveGovernanceCapabilityRoutePrefix(capability, preferredOwnedPrefix = \"\") {",
    "  const item = getGovernanceCapabilityItem(capability);",
    "  if (!item) return \"\";",
    "  const preferred = String(preferredOwnedPrefix || \"\").trim();",
    "  const primary = String(item.route_prefix || \"\").trim();",
    "  if (preferred) {",
    "    if (primary === preferred) return primary;",
    "    const owned = Array.isArray(item.owned_route_prefixes) ? item.owned_route_prefixes : [];",
    "    return owned.find((entry) => String(entry || \"\").trim() === preferred) || \"\";",
    "  }",
    "  return primary;",
    "}",
    "",
    "function governanceCapabilityOwnsRoutePrefix(capability, routePrefix) {",
    "  const item = getGovernanceCapabilityItem(capability);",
    "  const normalizedRoutePrefix = String(routePrefix || \"\").trim();",
    "  if (!item || !normalizedRoutePrefix) return false;",
    "  if (String(item.route_prefix || \"\").trim() === normalizedRoutePrefix) return true;",
    "  const owned = Array.isArray(item.owned_route_prefixes) ? item.owned_route_prefixes : [];",
    "  return owned.some((entry) => String(entry || \"\").trim() === normalizedRoutePrefix);",
    "}",
    "",
    "module.exports = {",
    "  GOVERNANCE_CAPABILITY_SCHEMA_VERSION,",
    "  GOVERNANCE_CAPABILITY_SOURCE_AUTHORITY,",
    "  GOVERNANCE_CAPABILITY_ITEMS,",
    "  GOVERNANCE_CAPABILITIES,",
    "  GOVERNANCE_CAPABILITY_ROUTE_CONSTANTS,",
    "  GOVERNANCE_CAPABILITY_BY_NAME,",
    "  getGovernanceCapabilityItem,",
    "  resolveGovernanceCapabilityRoutePrefix,",
    "  governanceCapabilityOwnsRoutePrefix,",
    "};",
    "",
  ].join("\n");
}

function renderWinUiModule(data) {
  const lines = [];
  lines.push("using System;");
  lines.push("");
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
  lines.push("");
  lines.push("    public static string ResolveRoutePrefix(string capability, string? preferredOwnedPrefix = null)");
  lines.push("    {");
  lines.push("        var normalizedCapability = (capability ?? string.Empty).Trim();");
  lines.push("        return normalizedCapability switch");
  lines.push("        {");
  for (const item of data.items) {
    const ownedLiterals = item.owned_route_prefixes
      .filter((routePrefix) => String(routePrefix || "").trim() && String(routePrefix || "").trim() !== item.route_prefix)
      .map((routePrefix) => JSON.stringify(routePrefix))
      .join(", ");
    const ownedArgument = ownedLiterals ? `, ${ownedLiterals}` : "";
    lines.push(`            ${item.constant} => ResolvePreferredOrPrimary(${item.constant}_ROUTE_PREFIX, preferredOwnedPrefix${ownedArgument}),`);
  }
  lines.push("            _ => string.Empty,");
  lines.push("        };");
  lines.push("    }");
  lines.push("");
  lines.push("    public static bool CapabilityOwnsRoutePrefix(string capability, string routePrefix)");
  lines.push("    {");
  lines.push("        var normalizedCapability = (capability ?? string.Empty).Trim();");
  lines.push("        return normalizedCapability switch");
  lines.push("        {");
  for (const item of data.items) {
    const ownedLiterals = item.owned_route_prefixes
      .filter((routePrefix) => String(routePrefix || "").trim() && String(routePrefix || "").trim() !== item.route_prefix)
      .map((routePrefix) => JSON.stringify(routePrefix))
      .join(", ");
    const ownedArgument = ownedLiterals ? `, ${ownedLiterals}` : "";
    lines.push(`            ${item.constant} => RouteBelongsToCapability(routePrefix, ${item.constant}_ROUTE_PREFIX${ownedArgument}),`);
  }
  lines.push("            _ => false,");
  lines.push("        };");
  lines.push("    }");
  lines.push("");
  lines.push("    private static string ResolvePreferredOrPrimary(string primaryRoutePrefix, string? preferredOwnedPrefix, params string[] ownedRoutePrefixes)");
  lines.push("    {");
  lines.push("        var preferred = (preferredOwnedPrefix ?? string.Empty).Trim();");
  lines.push("        if (string.IsNullOrWhiteSpace(preferred))");
  lines.push("        {");
  lines.push("            return primaryRoutePrefix;");
  lines.push("        }");
  lines.push("");
  lines.push("        if (string.Equals(primaryRoutePrefix, preferred, StringComparison.Ordinal))");
  lines.push("        {");
  lines.push("            return primaryRoutePrefix;");
  lines.push("        }");
  lines.push("");
  lines.push("        foreach (var routePrefix in ownedRoutePrefixes)");
  lines.push("        {");
  lines.push("            if (string.Equals(routePrefix, preferred, StringComparison.Ordinal))");
  lines.push("            {");
  lines.push("                return routePrefix;");
  lines.push("            }");
  lines.push("        }");
  lines.push("");
  lines.push("        return string.Empty;");
  lines.push("    }");
  lines.push("");
  lines.push("    private static bool RouteBelongsToCapability(string routePrefix, string primaryRoutePrefix, params string[] ownedRoutePrefixes)");
  lines.push("    {");
  lines.push("        var normalizedRoutePrefix = (routePrefix ?? string.Empty).Trim();");
  lines.push("        if (string.IsNullOrWhiteSpace(normalizedRoutePrefix))");
  lines.push("        {");
  lines.push("            return false;");
  lines.push("        }");
  lines.push("");
  lines.push("        if (string.Equals(primaryRoutePrefix, normalizedRoutePrefix, StringComparison.Ordinal))");
  lines.push("        {");
  lines.push("            return true;");
  lines.push("        }");
  lines.push("");
  lines.push("        foreach (var ownedRoutePrefix in ownedRoutePrefixes)");
  lines.push("        {");
  lines.push("            if (string.Equals(ownedRoutePrefix, normalizedRoutePrefix, StringComparison.Ordinal))");
  lines.push("            {");
  lines.push("                return true;");
  lines.push("            }");
  lines.push("        }");
  lines.push("");
  lines.push("        return false;");
  lines.push("    }");
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
