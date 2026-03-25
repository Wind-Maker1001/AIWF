const fs = require("fs");
const path = require("path");

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function loadNodeConfigContractSet(contractPath) {
  const payload = JSON.parse(fs.readFileSync(contractPath, "utf8"));
  const nodes = Array.isArray(payload?.nodes) ? payload.nodes : [];
  const contractByType = {};
  const qualityByType = {};
  for (const item of nodes) {
    const type = String(item?.type || "").trim();
    if (!type) continue;
    const quality = String(item?.quality || "").trim();
    contractByType[type] = {
      type,
      quality,
      validators: Array.isArray(item?.validators) ? item.validators : [],
    };
    qualityByType[type] = quality;
  }
  return {
    schemaVersion: String(payload?.schema_version || "").trim(),
    authority: String(payload?.authority || "").trim(),
    owner: String(payload?.owner || "").trim(),
    contractByType,
    contractTypes: uniqueSorted(Object.keys(contractByType)),
    qualityByType,
    raw: payload,
  };
}

function renderCommonJsModule(data) {
  return [
    `const NODE_CONFIG_CONTRACT_SET_SCHEMA_VERSION = ${JSON.stringify(data.schemaVersion)};`,
    `const NODE_CONFIG_CONTRACT_SET_AUTHORITY = ${JSON.stringify(data.authority)};`,
    `const NODE_CONFIG_CONTRACT_TYPES = Object.freeze(${JSON.stringify(data.contractTypes, null, 2)});`,
    `const NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE = Object.freeze(${JSON.stringify(data.qualityByType, null, 2)});`,
    `const NODE_CONFIG_CONTRACTS_BY_TYPE = Object.freeze(${JSON.stringify(data.contractByType, null, 2)});`,
    "",
    "module.exports = {",
    "  NODE_CONFIG_CONTRACT_SET_SCHEMA_VERSION,",
    "  NODE_CONFIG_CONTRACT_SET_AUTHORITY,",
    "  NODE_CONFIG_CONTRACT_TYPES,",
    "  NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE,",
    "  NODE_CONFIG_CONTRACTS_BY_TYPE,",
    "};",
    "",
  ].join("\n");
}

function renderEsmModule(data) {
  return [
    `export const NODE_CONFIG_CONTRACT_SET_SCHEMA_VERSION = ${JSON.stringify(data.schemaVersion)};`,
    `export const NODE_CONFIG_CONTRACT_SET_AUTHORITY = ${JSON.stringify(data.authority)};`,
    `export const NODE_CONFIG_CONTRACT_TYPES = Object.freeze(${JSON.stringify(data.contractTypes, null, 2)});`,
    `export const NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE = Object.freeze(${JSON.stringify(data.qualityByType, null, 2)});`,
    `export const NODE_CONFIG_CONTRACTS_BY_TYPE = Object.freeze(${JSON.stringify(data.contractByType, null, 2)});`,
    "",
  ].join("\n");
}

module.exports = {
  loadNodeConfigContractSet,
  renderCommonJsModule,
  renderEsmModule,
};
