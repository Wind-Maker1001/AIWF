"use strict";

const fs = require("fs");
const path = require("path");

const DEFAULT_MANIFEST_RELATIVE_PATH = path.join("contracts", "rust", "operators_manifest.v1.json");
const DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH = path.join("contracts", "rust", "operators_manifest.schema.json");
const DEFAULT_DESKTOP_MODULE_RELATIVE_PATH = path.join("apps", "dify-desktop", "workflow_chiplets", "domains", "rust_operator_manifest.generated.js");
const DEFAULT_RENDERER_MODULE_RELATIVE_PATH = path.join("apps", "dify-desktop", "renderer", "workflow", "rust_operator_manifest.generated.js");
const OPERATOR_CONTRACT_OVERRIDES = Object.freeze({
  transform_rows_v2: Object.freeze({
    request_schema: "#/components/schemas/TransformRowsReq",
    response_schema: "#/components/schemas/TransformRowsResp",
    rules_schema: "#/components/schemas/TransformRowsRuleSet",
    schema_hint_schema: "#/components/schemas/TransformRowsSchemaHint",
    audit_schema: "#/components/schemas/TransformRowsAudit",
  }),
});

function readText(filePath) {
  return fs.readFileSync(filePath, "utf8");
}

function uniqueSorted(values) {
  return Array.from(new Set(
    values
      .map((value) => String(value || "").trim())
      .filter(Boolean),
  )).sort();
}

function parseQuotedStrings(text) {
  return [...String(text || "").matchAll(/"([^"]+)"/g)].map((match) => match[1]);
}

function inferVersion(operator) {
  const match = String(operator || "").trim().match(/_v(\d+)$/i);
  return match ? `v${match[1]}` : null;
}

function parseBooleanMatchFunction(sourceText, functionName) {
  const regex = new RegExp(
    String.raw`pub\(super\)\s+fn\s+${functionName}\s*\([^)]*\)\s*->\s*bool\s*\{\s*matches!\(\s*operator,\s*([\s\S]*?)\)\s*\}`,
    "m",
  );
  const match = sourceText.match(regex);
  if (!match) {
    throw new Error(`unable to parse ${functionName} from operator_catalog_data.rs`);
  }
  return new Set(uniqueSorted(parseQuotedStrings(match[1])));
}

function parsePublishedEntries(sourceText) {
  const regex = /published_entry\("([^"]+)",\s*(true|false),\s*(true|false),\s*(true|false),\s*(true|false)\)/g;
  const entries = new Map();
  for (const match of sourceText.matchAll(regex)) {
    entries.set(match[1], {
      streaming: match[2] === "true",
      cache: match[3] === "true",
      checkpoint: match[4] === "true",
      io_contract: match[5] === "true",
    });
  }
  if (entries.size === 0) {
    throw new Error("published operator catalog parse returned 0 operators");
  }
  return entries;
}

function parseDomainCatalog(sourceText) {
  const bodyMatch = sourceText.match(
    /pub\(super\)\s+fn\s+infer_domain_catalog\s*\([^)]*\)\s*->\s*Option<\(&'static str, &'static str\)>\s*\{\s*match operator \{([\s\S]*?)\n\s*_\s*=>\s*None,\s*\n\s*\}\s*\}/m,
  );
  if (!bodyMatch) {
    throw new Error("unable to parse infer_domain_catalog from operator_catalog_data.rs");
  }

  const entries = new Map();
  const armRegex = /((?:"[^"]+"\s*(?:\|\s*"[^"]+"\s*)*))=>\s*(?:\{\s*)?Some\(\("([^"]+)",\s*"([^"]+)"\)\)(?:\s*\})?/g;
  for (const match of bodyMatch[1].matchAll(armRegex)) {
    const operators = uniqueSorted(parseQuotedStrings(match[1]));
    for (const operator of operators) {
      entries.set(operator, {
        domain: match[2],
        source_module: match[3],
      });
    }
  }
  if (entries.size === 0) {
    throw new Error("domain catalog parse returned 0 operators");
  }
  return entries;
}

function parseWorkflowOperators(engineText) {
  const operators = uniqueSorted(
    [...engineText.matchAll(/=>\s*"([^"]+)"/g)].map((match) => match[1]),
  );
  if (operators.length === 0) {
    throw new Error("rust workflow operator parse returned 0 operators");
  }
  return new Set(operators);
}

function normalizeManifest(manifest) {
  const operators = Array.isArray(manifest?.operators) ? manifest.operators : [];
  const normalizedOperators = operators
    .map((item) => ({
      operator: String(item?.operator || "").trim(),
      version: item?.version ? String(item.version) : null,
      domain: String(item?.domain || "").trim(),
      source_module: String(item?.source_module || "").trim(),
      published: Boolean(item?.published),
      workflow_exposable: Boolean(item?.workflow_exposable),
      desktop_exposable: Boolean(item?.desktop_exposable),
      palette_hidden: Boolean(item?.palette_hidden),
      direct_http_only: Boolean(item?.direct_http_only),
      internal_only: Boolean(item?.internal_only),
      contracts: item?.contracts && typeof item.contracts === "object" && !Array.isArray(item.contracts)
        ? {
            request_schema: item.contracts.request_schema ? String(item.contracts.request_schema) : undefined,
            response_schema: item.contracts.response_schema ? String(item.contracts.response_schema) : undefined,
            rules_schema: item.contracts.rules_schema ? String(item.contracts.rules_schema) : undefined,
            schema_hint_schema: item.contracts.schema_hint_schema ? String(item.contracts.schema_hint_schema) : undefined,
            audit_schema: item.contracts.audit_schema ? String(item.contracts.audit_schema) : undefined,
          }
        : undefined,
      capabilities: {
        streaming: Boolean(item?.capabilities?.streaming),
        cache: Boolean(item?.capabilities?.cache),
        checkpoint: Boolean(item?.capabilities?.checkpoint),
        io_contract: Boolean(item?.capabilities?.io_contract),
      },
    }))
    .filter((item) => item.operator)
    .sort((a, b) => a.operator.localeCompare(b.operator));

  return {
    manifest_version: String(manifest?.manifest_version || "v1"),
    authority: String(manifest?.authority || "rust_operator_catalog"),
    owner: String(manifest?.owner || "apps/accel-rust"),
    source_inputs: uniqueSorted(Array.isArray(manifest?.source_inputs) ? manifest.source_inputs : []),
    operators: normalizedOperators,
    summary: {
      known_operator_count: normalizedOperators.length,
      published_operator_count: normalizedOperators.filter((item) => item.published).length,
      workflow_exposable_count: normalizedOperators.filter((item) => item.workflow_exposable).length,
      desktop_exposable_count: normalizedOperators.filter((item) => item.desktop_exposable).length,
      direct_http_only_count: normalizedOperators.filter((item) => item.direct_http_only).length,
      internal_only_count: normalizedOperators.filter((item) => item.internal_only).length,
    },
  };
}

function validateOperatorManifest(manifest) {
  const normalized = normalizeManifest(manifest);
  const errors = [];

  function push(message) {
    errors.push(String(message));
  }

  if (normalized.manifest_version !== "v1") {
    push(`manifest.manifest_version must be "v1"`);
  }
  if (normalized.authority !== "rust_operator_catalog") {
    push(`manifest.authority must be "rust_operator_catalog"`);
  }
  if (!String(normalized.owner || "").trim()) {
    push("manifest.owner must not be empty");
  }

  if (!Array.isArray(normalized.source_inputs)) {
    push("manifest.source_inputs must be an array");
  } else {
    normalized.source_inputs.forEach((item, index) => {
      if (!String(item || "").trim()) {
        push(`manifest.source_inputs[${index}] must not be empty`);
      }
    });
  }

  const operatorIds = new Set();
  normalized.operators.forEach((item, index) => {
    const prefix = `manifest.operators[${index}]`;
    if (!item.operator) {
      push(`${prefix}.operator must not be empty`);
    }
    if (operatorIds.has(item.operator)) {
      push(`${prefix}.operator duplicates ${item.operator}`);
    }
    operatorIds.add(item.operator);
    if (item.version !== null && !String(item.version || "").trim()) {
      push(`${prefix}.version must be null or a non-empty string`);
    }
    if (!item.domain) {
      push(`${prefix}.domain must not be empty`);
    }
    if (!item.source_module) {
      push(`${prefix}.source_module must not be empty`);
    }
    if (item.desktop_exposable && !item.workflow_exposable) {
      push(`${prefix}.desktop_exposable requires workflow_exposable`);
    }
    if (item.palette_hidden && !item.desktop_exposable) {
      push(`${prefix}.palette_hidden requires desktop_exposable`);
    }
    if (item.direct_http_only && item.workflow_exposable) {
      push(`${prefix}.direct_http_only cannot be true when workflow_exposable is true`);
    }
    if (item.direct_http_only && item.internal_only) {
      push(`${prefix}.direct_http_only and internal_only cannot both be true`);
    }
    if (item.internal_only && item.desktop_exposable) {
      push(`${prefix}.internal_only cannot be true when desktop_exposable is true`);
    }
    if (item.internal_only && item.published) {
      push(`${prefix}.internal_only cannot be true when published is true`);
    }
    if (item.contracts !== undefined) {
      if (!item.contracts || typeof item.contracts !== "object" || Array.isArray(item.contracts)) {
        push(`${prefix}.contracts must be an object when provided`);
      } else {
        for (const contractName of ["request_schema", "response_schema", "rules_schema", "schema_hint_schema", "audit_schema"]) {
          const value = item.contracts[contractName];
          if (value !== undefined && !String(value || "").trim()) {
            push(`${prefix}.contracts.${contractName} must not be empty when provided`);
          }
        }
      }
    }
    if (!item.capabilities || typeof item.capabilities !== "object" || Array.isArray(item.capabilities)) {
      push(`${prefix}.capabilities must be an object`);
      return;
    }
    for (const capabilityName of ["streaming", "cache", "checkpoint", "io_contract"]) {
      if (typeof item.capabilities[capabilityName] !== "boolean") {
        push(`${prefix}.capabilities.${capabilityName} must be a boolean`);
      }
    }
  });

  const computedSummary = {
    known_operator_count: normalized.operators.length,
    published_operator_count: normalized.operators.filter((item) => item.published).length,
    workflow_exposable_count: normalized.operators.filter((item) => item.workflow_exposable).length,
    desktop_exposable_count: normalized.operators.filter((item) => item.desktop_exposable).length,
    direct_http_only_count: normalized.operators.filter((item) => item.direct_http_only).length,
    internal_only_count: normalized.operators.filter((item) => item.internal_only).length,
  };
  for (const [key, value] of Object.entries(computedSummary)) {
    if (Number(normalized.summary[key]) !== value) {
      push(`manifest.summary.${key} mismatch: expected ${value}, got ${normalized.summary[key]}`);
    }
  }

  return {
    ok: errors.length === 0,
    manifest: normalized,
    errors,
  };
}

function buildDesktopRustOperatorModuleSource(manifest) {
  const result = validateOperatorManifest(manifest);
  if (!result.ok) {
    throw new Error(result.errors.join("; "));
  }
  const normalized = result.manifest;
  const knownOperators = normalized.operators
    .sort((a, b) => a.operator.localeCompare(b.operator));
  const desktopOperators = normalized.operators
    .filter((item) => item.desktop_exposable)
    .sort((a, b) => a.operator.localeCompare(b.operator));
  const knownMetadataByOperator = Object.fromEntries(
    knownOperators.map((item) => [item.operator, item]),
  );
  const metadataByOperator = Object.fromEntries(
    desktopOperators.map((item) => [item.operator, item]),
  );

  return `"use strict";

// Generated by ops/scripts/export_operator_manifest.ps1. Do not edit manually.

function deepFreeze(value) {
  if (!value || typeof value !== "object") return value;
  Object.getOwnPropertyNames(value).forEach((name) => {
    const next = value[name];
    if (next && typeof next === "object") {
      deepFreeze(next);
    }
  });
  return Object.freeze(value);
}

const DESKTOP_RUST_OPERATOR_MANIFEST_VERSION = ${JSON.stringify(normalized.manifest_version)};
const DESKTOP_RUST_OPERATOR_MANIFEST_AUTHORITY = ${JSON.stringify(normalized.authority)};
const DESKTOP_RUST_OPERATOR_SCHEMA_PATH = ${JSON.stringify(DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH.replace(/\\\\/g, "/"))};
const DESKTOP_RUST_OPERATOR_SOURCE_INPUTS = Object.freeze(${JSON.stringify(normalized.source_inputs)});
const KNOWN_RUST_OPERATOR_METADATA = deepFreeze(${JSON.stringify(knownMetadataByOperator, null, 2)});
const KNOWN_RUST_OPERATOR_TYPES = Object.freeze(Object.keys(KNOWN_RUST_OPERATOR_METADATA).sort());
const KNOWN_RUST_OPERATOR_COUNT = KNOWN_RUST_OPERATOR_TYPES.length;
const DESKTOP_RUST_OPERATOR_METADATA = deepFreeze(${JSON.stringify(metadataByOperator, null, 2)});
const DESKTOP_RUST_OPERATOR_TYPES = Object.freeze(Object.keys(DESKTOP_RUST_OPERATOR_METADATA).sort());
const DESKTOP_RUST_OPERATOR_COUNT = DESKTOP_RUST_OPERATOR_TYPES.length;

function hasKnownRustOperator(type) {
  return Object.prototype.hasOwnProperty.call(KNOWN_RUST_OPERATOR_METADATA, String(type || "").trim());
}

function hasDesktopRustOperator(type) {
  return Object.prototype.hasOwnProperty.call(DESKTOP_RUST_OPERATOR_METADATA, String(type || "").trim());
}

function getDesktopRustOperatorMetadata(type) {
  const normalizedType = String(type || "").trim();
  return hasDesktopRustOperator(normalizedType) ? DESKTOP_RUST_OPERATOR_METADATA[normalizedType] : null;
}

function assertDesktopRustOperator(type) {
  const normalizedType = String(type || "").trim();
  if (!hasDesktopRustOperator(normalizedType)) {
    throw new Error(\`desktop rust operator not manifest-authorized: \${normalizedType}\`);
  }
  return DESKTOP_RUST_OPERATOR_METADATA[normalizedType];
}

module.exports = {
  DESKTOP_RUST_OPERATOR_MANIFEST_VERSION,
  DESKTOP_RUST_OPERATOR_MANIFEST_AUTHORITY,
  DESKTOP_RUST_OPERATOR_SCHEMA_PATH,
  DESKTOP_RUST_OPERATOR_SOURCE_INPUTS,
  KNOWN_RUST_OPERATOR_METADATA,
  KNOWN_RUST_OPERATOR_TYPES,
  KNOWN_RUST_OPERATOR_COUNT,
  DESKTOP_RUST_OPERATOR_METADATA,
  DESKTOP_RUST_OPERATOR_TYPES,
  DESKTOP_RUST_OPERATOR_COUNT,
  hasKnownRustOperator,
  hasDesktopRustOperator,
  getDesktopRustOperatorMetadata,
  assertDesktopRustOperator,
};
`;
}

function buildRendererRustOperatorModuleSource(manifest) {
  const result = validateOperatorManifest(manifest);
  if (!result.ok) {
    throw new Error(result.errors.join("; "));
  }
  const normalized = result.manifest;
  const knownOperators = normalized.operators
    .sort((a, b) => a.operator.localeCompare(b.operator));
  const desktopOperators = normalized.operators
    .filter((item) => item.desktop_exposable)
    .sort((a, b) => a.operator.localeCompare(b.operator));
  const knownMetadataByOperator = Object.fromEntries(
    knownOperators.map((item) => [item.operator, item]),
  );
  const desktopMetadataByOperator = Object.fromEntries(
    desktopOperators.map((item) => [item.operator, item]),
  );

  return `// Generated by ops/scripts/export_operator_manifest.ps1. Do not edit manually.

function deepFreeze(value) {
  if (!value || typeof value !== "object") return value;
  Object.getOwnPropertyNames(value).forEach((name) => {
    const next = value[name];
    if (next && typeof next === "object") {
      deepFreeze(next);
    }
  });
  return Object.freeze(value);
}

export const RENDERER_RUST_OPERATOR_MANIFEST_VERSION = ${JSON.stringify(normalized.manifest_version)};
export const RENDERER_RUST_OPERATOR_MANIFEST_AUTHORITY = ${JSON.stringify(normalized.authority)};
export const RENDERER_RUST_OPERATOR_SCHEMA_PATH = ${JSON.stringify(DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH.replace(/\\\\/g, "/"))};
export const RENDERER_RUST_OPERATOR_SOURCE_INPUTS = Object.freeze(${JSON.stringify(normalized.source_inputs)});
export const KNOWN_RUST_OPERATOR_METADATA = deepFreeze(${JSON.stringify(knownMetadataByOperator, null, 2)});
export const KNOWN_RUST_OPERATOR_TYPES = Object.freeze(Object.keys(KNOWN_RUST_OPERATOR_METADATA).sort());
export const KNOWN_RUST_OPERATOR_COUNT = KNOWN_RUST_OPERATOR_TYPES.length;
export const DESKTOP_RUST_OPERATOR_METADATA = deepFreeze(${JSON.stringify(desktopMetadataByOperator, null, 2)});
export const DESKTOP_RUST_OPERATOR_TYPES = Object.freeze(Object.keys(DESKTOP_RUST_OPERATOR_METADATA).sort());
export const DESKTOP_RUST_OPERATOR_COUNT = DESKTOP_RUST_OPERATOR_TYPES.length;

export function hasKnownRustOperator(type) {
  return Object.prototype.hasOwnProperty.call(KNOWN_RUST_OPERATOR_METADATA, String(type || "").trim());
}

export function hasDesktopRustOperator(type) {
  return Object.prototype.hasOwnProperty.call(DESKTOP_RUST_OPERATOR_METADATA, String(type || "").trim());
}

export function getDesktopRustOperatorMetadata(type) {
  const normalizedType = String(type || "").trim();
  return hasDesktopRustOperator(normalizedType) ? DESKTOP_RUST_OPERATOR_METADATA[normalizedType] : null;
}
`;
}

function buildOperatorManifest(repoRoot) {
  const rustCatalogPath = path.join(repoRoot, "apps", "accel-rust", "src", "operator_catalog_data.rs");
  const rustWorkflowEnginePath = path.join(repoRoot, "apps", "accel-rust", "src", "operators", "workflow", "engine.rs");

  const catalogText = readText(rustCatalogPath);
  const workflowEngineText = readText(rustWorkflowEnginePath);
  const publishedEntries = parsePublishedEntries(catalogText);
  const domainCatalog = parseDomainCatalog(catalogText);
  const workflowOperators = parseWorkflowOperators(workflowEngineText);
  const streaming = parseBooleanMatchFunction(catalogText, "infer_streaming");
  const cache = parseBooleanMatchFunction(catalogText, "infer_cache");
  const checkpoint = parseBooleanMatchFunction(catalogText, "infer_checkpoint");
  const ioContract = parseBooleanMatchFunction(catalogText, "infer_io_contract");
  const desktopHidden = parseBooleanMatchFunction(catalogText, "infer_desktop_hidden");
  const paletteHidden = parseBooleanMatchFunction(catalogText, "infer_palette_hidden");

  const operators = uniqueSorted([...domainCatalog.keys()]).map((operator) => {
    const domainEntry = domainCatalog.get(operator);
    const publishedCapabilities = publishedEntries.get(operator);
    const workflowExposable = workflowOperators.has(operator);
    const desktopExposable = workflowExposable && !desktopHidden.has(operator);
    const published = publishedEntries.has(operator);
    return {
      operator,
      version: inferVersion(operator),
      domain: domainEntry.domain,
      source_module: domainEntry.source_module,
      published,
      workflow_exposable: workflowExposable,
      desktop_exposable: desktopExposable,
      palette_hidden: desktopExposable && paletteHidden.has(operator),
      direct_http_only: published && !workflowExposable,
      internal_only: !desktopExposable && !(published && !workflowExposable),
      contracts: OPERATOR_CONTRACT_OVERRIDES[operator],
      capabilities: publishedCapabilities || {
        streaming: streaming.has(operator),
        cache: cache.has(operator),
        checkpoint: checkpoint.has(operator),
        io_contract: ioContract.has(operator),
      },
    };
  });

  const result = validateOperatorManifest({
    manifest_version: "v1",
    authority: "rust_operator_catalog",
    owner: "apps/accel-rust",
    source_inputs: [
      "apps/accel-rust/src/operator_catalog_data.rs",
      "apps/accel-rust/src/operators/workflow/engine.rs",
    ],
    operators,
  });
  if (!result.ok) {
    throw new Error(result.errors.join("; "));
  }
  return result.manifest;
}

function getManifestPath(repoRoot, outputPath) {
  return path.resolve(repoRoot, outputPath || DEFAULT_MANIFEST_RELATIVE_PATH);
}

function getDesktopModulePath(repoRoot, outputPath) {
  return path.resolve(repoRoot, outputPath || DEFAULT_DESKTOP_MODULE_RELATIVE_PATH);
}

function getRendererModulePath(repoRoot, outputPath) {
  return path.resolve(repoRoot, outputPath || DEFAULT_RENDERER_MODULE_RELATIVE_PATH);
}

function readOperatorManifest(manifestPath) {
  const result = validateOperatorManifest(JSON.parse(readText(manifestPath)));
  if (!result.ok) {
    throw new Error(result.errors.join("; "));
  }
  return result.manifest;
}

module.exports = {
  DEFAULT_MANIFEST_RELATIVE_PATH,
  DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH,
  DEFAULT_DESKTOP_MODULE_RELATIVE_PATH,
  DEFAULT_RENDERER_MODULE_RELATIVE_PATH,
  buildOperatorManifest,
  buildDesktopRustOperatorModuleSource,
  buildRendererRustOperatorModuleSource,
  getDesktopModulePath,
  getRendererModulePath,
  getManifestPath,
  normalizeManifest,
  readOperatorManifest,
  validateOperatorManifest,
};
