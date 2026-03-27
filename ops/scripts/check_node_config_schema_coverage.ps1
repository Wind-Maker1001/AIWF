param(
  [string]$RepoRoot = "",
  [int]$MinimumNestedShapeConstrained = 19,
  [string[]]$RequiredNestedNodeTypes = @(
    "load_rows_v3",
    "quality_check_v3",
    "quality_check_v4",
    "office_slot_fill_v1",
    "optimizer_v1",
    "parquet_io_v2",
    "plugin_registry_v1",
    "transform_rows_v3",
    "lineage_v3",
    "rule_simulator_v1",
    "constraint_solver_v1",
    "udf_wasm_v2"
  )
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}

$pythonScript = @'
import json
import os
import sys
from pathlib import Path

repo_root = Path(sys.argv[1]).resolve()
os.environ["AIWF_REPO_ROOT"] = str(repo_root)
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

from aiwf.node_config_contract_runtime import build_node_config_contract_runtime_summary

print(json.dumps(build_node_config_contract_runtime_summary(), ensure_ascii=False))
'@

$nodeScript = @'
const path = require("path");
const { pathToFileURL } = require("url");
const DEFAULT_REQUIRED_NESTED_NODE_TYPES = [
  "load_rows_v3",
  "quality_check_v3",
  "quality_check_v4",
  "office_slot_fill_v1",
  "optimizer_v1",
  "parquet_io_v2",
  "plugin_registry_v1",
  "transform_rows_v3",
  "lineage_v3",
  "rule_simulator_v1",
  "constraint_solver_v1",
  "udf_wasm_v2",
];
const DEFAULT_MIN_NESTED_SHAPE_CONSTRAINED = 19;

function parseCsvList(value) {
  return String(value || "")
    .split(",")
    .map((item) => item.trim())
    .filter(Boolean);
}

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map((item) => String(item || "").trim()).filter(Boolean))).sort();
}

(async () => {
  const repoRoot = process.argv[2];
  const requiredNestedNodeTypes = (() => {
    const fromEnv = parseCsvList(process.env.AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES);
    return fromEnv.length > 0 ? Array.from(new Set(fromEnv)) : DEFAULT_REQUIRED_NESTED_NODE_TYPES;
  })();
  const minimumNestedShapeConstrained = (() => {
    const raw = Number.parseInt(process.env.AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED || "", 10);
    return Number.isFinite(raw) && raw >= 0 ? raw : DEFAULT_MIN_NESTED_SHAPE_CONSTRAINED;
  })();
  const cjsPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_contract.js");
  const esmPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "workflow-contract.js");
  const catalogPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "defaults-catalog.js");
  const contractPath = path.join(repoRoot, "contracts", "desktop", "node_config_contracts.v1.json");
  const contractCjsPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_node_config_contract.generated.js");
  const contractEsmPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "node_config_contract.generated.js");

  const cjs = require(cjsPath);
  const esm = await import(pathToFileURL(esmPath).href);
  const catalog = await import(pathToFileURL(catalogPath).href);
  const contractJson = JSON.parse(require("fs").readFileSync(contractPath, "utf8"));
  const contractCjs = require(contractCjsPath);
  const contractEsm = await import(pathToFileURL(contractEsmPath).href);
  const pythonRuntime = JSON.parse(process.env.AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON || "{}");

  const cjsIds = Array.isArray(cjs.NODE_CONFIG_SCHEMA_IDS) ? cjs.NODE_CONFIG_SCHEMA_IDS : [];
  const esmIds = Array.isArray(esm.NODE_CONFIG_SCHEMA_IDS) ? esm.NODE_CONFIG_SCHEMA_IDS : [];
  const cjsQuality = cjs.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE && typeof cjs.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE === "object"
    ? cjs.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE
    : {};
  const esmQuality = esm.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE && typeof esm.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE === "object"
    ? esm.NODE_CONFIG_SCHEMA_QUALITY_BY_TYPE
    : {};
  const catalogIds = new Set((Array.isArray(catalog.NODE_CATALOG) ? catalog.NODE_CATALOG : []).map((item) => String(item?.type || "")));
  const allowedQuality = ["typed", "enum_constrained", "nested_shape_constrained"];
  const issues = [];
  const contractTypes = Array.isArray(contractJson?.nodes)
    ? Array.from(new Set(contractJson.nodes.map((item) => String(item?.type || "").trim()).filter(Boolean))).sort()
    : [];
  const contractValidatorKinds = uniqueSorted((Array.isArray(contractJson?.nodes) ? contractJson.nodes : []).flatMap((item) => (
    Array.isArray(item?.validators) ? item.validators.map((validator) => String(validator?.kind || "").trim()) : []
  )));
  const TARGET_NODE_TYPES = contractTypes;
  const contractQuality = Object.fromEntries((Array.isArray(contractJson?.nodes) ? contractJson.nodes : [])
    .map((item) => [String(item?.type || "").trim(), String(item?.quality || "").trim()]));
  const contractAuthority = String(contractJson?.authority || "").trim();
  const contractSchemaVersion = String(contractJson?.schema_version || "").trim();
  const contractModuleTypeDrift = Array.from(new Set(
    contractTypes.filter((type) => !(Array.isArray(contractCjs.NODE_CONFIG_CONTRACT_TYPES) ? contractCjs.NODE_CONFIG_CONTRACT_TYPES : []).includes(type))
      .concat(contractTypes.filter((type) => !(Array.isArray(contractEsm.NODE_CONFIG_CONTRACT_TYPES) ? contractEsm.NODE_CONFIG_CONTRACT_TYPES : []).includes(type)))
      .concat((Array.isArray(contractCjs.NODE_CONFIG_CONTRACT_TYPES) ? contractCjs.NODE_CONFIG_CONTRACT_TYPES : []).filter((type) => !contractTypes.includes(type)))
      .concat((Array.isArray(contractEsm.NODE_CONFIG_CONTRACT_TYPES) ? contractEsm.NODE_CONFIG_CONTRACT_TYPES : []).filter((type) => !contractTypes.includes(type)))
  ));
  const contractModuleQualityDrift = contractTypes.filter((type) => (
    String(contractQuality[type] || "") !== String((contractCjs.NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE || {})[type] || "")
    || String(contractQuality[type] || "") !== String((contractEsm.NODE_CONFIG_CONTRACT_QUALITY_BY_TYPE || {})[type] || "")
  ));
  const requiredNestedTypesMissingFromContract = requiredNestedNodeTypes.filter((type) => !contractTypes.includes(type));

  const missingFromCatalog = TARGET_NODE_TYPES.filter((type) => !catalogIds.has(type));
  const missingFromCjs = TARGET_NODE_TYPES.filter((type) => !cjsIds.includes(type));
  const missingFromEsm = TARGET_NODE_TYPES.filter((type) => !esmIds.includes(type));
  const drift = Array.from(new Set(
    cjsIds.filter((type) => !esmIds.includes(type))
      .concat(esmIds.filter((type) => !cjsIds.includes(type)))
  ));
  const missingQualityCjs = TARGET_NODE_TYPES.filter((type) => !Object.prototype.hasOwnProperty.call(cjsQuality, type));
  const missingQualityEsm = TARGET_NODE_TYPES.filter((type) => !Object.prototype.hasOwnProperty.call(esmQuality, type));
  const invalidQuality = TARGET_NODE_TYPES.filter((type) => !allowedQuality.includes(String(cjsQuality[type] || "")));
  const qualityDrift = TARGET_NODE_TYPES.filter((type) => String(cjsQuality[type] || "") !== String(esmQuality[type] || ""));
  const qualityCounts = Object.fromEntries(allowedQuality.map((quality) => [
    quality,
    TARGET_NODE_TYPES.filter((type) => cjsQuality[type] === quality).length,
  ]));
  const nestedCovered = TARGET_NODE_TYPES.filter((type) => cjsQuality[type] === "nested_shape_constrained");
  const requiredNestedMissing = requiredNestedNodeTypes.filter((type) => cjsQuality[type] !== "nested_shape_constrained");
  const nestedShapeConstrainedDeficit = Math.max(0, minimumNestedShapeConstrained - nestedCovered.length);
  const pythonRuntimeContractAuthority = String(pythonRuntime?.authority || "").trim();
  const pythonRuntimeSchemaVersion = String(pythonRuntime?.schema_version || "").trim();
  const pythonRuntimeContractTypes = uniqueSorted(pythonRuntime?.contract_types || []);
  const pythonRuntimeSupportedValidatorKinds = uniqueSorted(pythonRuntime?.supported_validator_kinds || []);
  const pythonRuntimeMissingTypes = contractTypes.filter((type) => !pythonRuntimeContractTypes.includes(type));
  const pythonRuntimeStaleTypes = pythonRuntimeContractTypes.filter((type) => !contractTypes.includes(type));
  const pythonRuntimeMissingValidatorKinds = contractValidatorKinds.filter((kind) => !pythonRuntimeSupportedValidatorKinds.includes(kind));
  const pythonRuntimeAuthorityDrift = pythonRuntimeContractAuthority && pythonRuntimeContractAuthority !== contractAuthority
    ? [pythonRuntimeContractAuthority]
    : (pythonRuntimeContractAuthority ? [] : ["<missing>"]);
  const pythonRuntimeSchemaVersionDrift = pythonRuntimeSchemaVersion && pythonRuntimeSchemaVersion !== contractSchemaVersion
    ? [pythonRuntimeSchemaVersion]
    : (pythonRuntimeSchemaVersion ? [] : ["<missing>"]);

  if (missingFromCatalog.length > 0) issues.push(`node config schema target missing from defaults catalog: ${missingFromCatalog.join(", ")}`);
  if (missingFromCjs.length > 0) issues.push(`node config schema coverage missing in workflow_contract.js: ${missingFromCjs.join(", ")}`);
  if (missingFromEsm.length > 0) issues.push(`node config schema coverage missing in renderer workflow-contract.js: ${missingFromEsm.join(", ")}`);
  if (drift.length > 0) issues.push(`node config schema id drift between CJS and ESM helpers: ${drift.join(", ")}`);
  if (missingQualityCjs.length > 0) issues.push(`node config schema quality missing in workflow_contract.js: ${missingQualityCjs.join(", ")}`);
  if (missingQualityEsm.length > 0) issues.push(`node config schema quality missing in renderer workflow-contract.js: ${missingQualityEsm.join(", ")}`);
  if (invalidQuality.length > 0) issues.push(`node config schema quality invalid in workflow_contract.js: ${invalidQuality.join(", ")}`);
  if (qualityDrift.length > 0) issues.push(`node config schema quality drift between CJS and ESM helpers: ${qualityDrift.join(", ")}`);
  if (requiredNestedTypesMissingFromContract.length > 0) issues.push(`node config contract missing required nested types: ${requiredNestedTypesMissingFromContract.join(", ")}`);
  if (contractModuleTypeDrift.length > 0) issues.push(`node config contract module type drift: ${contractModuleTypeDrift.join(", ")}`);
  if (contractModuleQualityDrift.length > 0) issues.push(`node config contract module quality drift: ${contractModuleQualityDrift.join(", ")}`);
  if (nestedShapeConstrainedDeficit > 0) {
    issues.push(`node config schema nested_shape_constrained count too low: ${nestedCovered.length} < ${minimumNestedShapeConstrained}`);
  }
  if (requiredNestedMissing.length > 0) {
    issues.push(`node config schema required nested coverage missing: ${requiredNestedMissing.join(", ")}`);
  }
  if (pythonRuntimeAuthorityDrift.length > 0) {
    issues.push(`node config python runtime authority drift: ${pythonRuntimeAuthorityDrift.join(", ")}`);
  }
  if (pythonRuntimeSchemaVersionDrift.length > 0) {
    issues.push(`node config python runtime schema version drift: ${pythonRuntimeSchemaVersionDrift.join(", ")}`);
  }
  if (pythonRuntimeMissingTypes.length > 0) {
    issues.push(`node config python runtime missing contract types: ${pythonRuntimeMissingTypes.join(", ")}`);
  }
  if (pythonRuntimeStaleTypes.length > 0) {
    issues.push(`node config python runtime has stale contract types: ${pythonRuntimeStaleTypes.join(", ")}`);
  }
  if (pythonRuntimeMissingValidatorKinds.length > 0) {
    issues.push(`node config python runtime missing validator kinds: ${pythonRuntimeMissingValidatorKinds.join(", ")}`);
  }

  const summary = {
    status: issues.length > 0 ? "failed" : "passed",
    contractPath,
    contractCjsPath,
    contractEsmPath,
    contractAuthority,
    contractSchemaVersion,
    contractValidatorKindCount: contractValidatorKinds.length,
    targetCount: TARGET_NODE_TYPES.length,
    coveredCount: TARGET_NODE_TYPES.filter((type) => cjsIds.includes(type) && esmIds.includes(type)).length,
    minimumNestedShapeConstrained: minimumNestedShapeConstrained,
    nestedShapeConstrainedCount: nestedCovered.length,
    nestedShapeConstrainedDeficit,
    requiredNestedNodeTypes: requiredNestedNodeTypes,
    requiredNestedSatisfied: requiredNestedNodeTypes.filter((type) => cjsQuality[type] === "nested_shape_constrained"),
    requiredNestedMissing,
    qualityCounts,
    pythonRuntimeContractAuthority,
    pythonRuntimeSchemaVersion,
    pythonRuntimeCoveredCount: pythonRuntimeContractTypes.length,
    pythonRuntimeSupportedValidatorKindCount: pythonRuntimeSupportedValidatorKinds.length,
    pythonRuntimeSupportedValidatorKinds,
    drift: {
      missingFromCatalog,
      missingFromCjs,
      missingFromEsm,
      idDrift: drift,
      missingQualityCjs,
      missingQualityEsm,
      invalidQuality,
      qualityDrift,
      contractModuleTypeDrift,
      contractModuleQualityDrift,
      requiredNestedTypesMissingFromContract,
      pythonRuntimeAuthorityDrift,
      pythonRuntimeSchemaVersionDrift,
      pythonRuntimeMissingTypes,
      pythonRuntimeStaleTypes,
      pythonRuntimeMissingValidatorKinds,
    },
    issues,
    covered: TARGET_NODE_TYPES,
  };
  console.log(JSON.stringify(summary));
  if (summary.status !== "passed") {
    process.exit(1);
  }
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
'@

if ($null -eq $env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED) {
  $previousMinimumNestedShapeConstrained = $null
} else {
  $previousMinimumNestedShapeConstrained = $env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED
}
if ($null -eq $env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES) {
  $previousRequiredNestedNodeTypes = $null
} else {
  $previousRequiredNestedNodeTypes = $env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES
}
$previousPythonRuntimeSummary = if ($null -eq $env:AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON) { $null } else { $env:AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON }
$env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED = [string]$MinimumNestedShapeConstrained
$env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES = (@($RequiredNestedNodeTypes | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join ",")
$pythonRuntimeSummaryJson = $pythonScript | python - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "python node config contract runtime summary failed"
}
$env:AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON = [string]$pythonRuntimeSummaryJson

try {
  $nodeScript | node - $RepoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "node config schema coverage checks failed"
  }

  Ok "node config schema coverage check passed"
}
finally {
  if ($null -eq $previousMinimumNestedShapeConstrained) {
    Remove-Item Env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED = $previousMinimumNestedShapeConstrained
  }
  if ($null -eq $previousRequiredNestedNodeTypes) {
    Remove-Item Env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES = $previousRequiredNestedNodeTypes
  }
  if ($null -eq $previousPythonRuntimeSummary) {
    Remove-Item Env:AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_NODE_CONFIG_PYTHON_RUNTIME_JSON = $previousPythonRuntimeSummary
  }
}
