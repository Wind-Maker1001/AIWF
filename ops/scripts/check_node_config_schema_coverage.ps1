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

$requiredTypesJson = ConvertTo-Json -InputObject @($RequiredNestedNodeTypes) -Compress

$nodeScript = @'
const fs = require("fs");
const path = require("path");

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map((item) => String(item || "").trim()).filter(Boolean))).sort();
}

(async () => {
  const repoRoot = process.argv[2];
  const minimumNestedShapeConstrained = Number.parseInt(process.env.AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED || "19", 10);
  const requiredNestedNodeTypes = JSON.parse(process.env.AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES_JSON || "[]");

  const contractPath = path.join(repoRoot, "contracts", "desktop", "node_config_contracts.v1.json");
  const rustAuthorityPath = path.join(repoRoot, "apps", "accel-rust", "src", "governance_ops", "contracts", "workflow_contract.rs");
  const rustHandlerPath = path.join(repoRoot, "apps", "accel-rust", "src", "http", "handlers_extended", "platform.rs");
  const glueValidationClientPath = path.join(repoRoot, "apps", "glue-python", "aiwf", "workflow_validation_client.py");
  const desktopWorkflowContractPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "workflow-contract.js");
  const packageJsonPath = path.join(repoRoot, "apps", "dify-desktop", "package.json");
  const liteManifestPath = path.join(repoRoot, "apps", "dify-desktop", "build", "electron-builder.lite.json");
  const generatedCjsPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_node_config_contract.generated.js");
  const generatedEsmPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "node_config_contract.generated.js");

  const contractJson = JSON.parse(fs.readFileSync(contractPath, "utf8"));
  const rustAuthorityText = fs.readFileSync(rustAuthorityPath, "utf8");
  const rustHandlerText = fs.readFileSync(rustHandlerPath, "utf8");
  const glueValidationClientText = fs.readFileSync(glueValidationClientPath, "utf8");
  const desktopWorkflowContractText = fs.readFileSync(desktopWorkflowContractPath, "utf8");
  const packageJson = JSON.parse(fs.readFileSync(packageJsonPath, "utf8").replace(/^\uFEFF/, ""));
  const liteManifest = JSON.parse(fs.readFileSync(liteManifestPath, "utf8").replace(/^\uFEFF/, ""));

  const contractNodes = Array.isArray(contractJson?.nodes) ? contractJson.nodes : [];
  const contractTypes = uniqueSorted(contractNodes.map((item) => item?.type));
  const contractQuality = Object.fromEntries(contractNodes.map((item) => [
    String(item?.type || "").trim(),
    String(item?.quality || "").trim(),
  ]).filter(([type]) => !!type));
  const contractValidatorKinds = uniqueSorted(contractNodes.flatMap((item) => (
    Array.isArray(item?.validators) ? item.validators.map((validator) => validator?.kind) : []
  )));

  const qualityCounts = {
    typed: contractTypes.filter((type) => contractQuality[type] === "typed").length,
    enum_constrained: contractTypes.filter((type) => contractQuality[type] === "enum_constrained").length,
    nested_shape_constrained: contractTypes.filter((type) => contractQuality[type] === "nested_shape_constrained").length,
  };
  const requiredNestedMissing = requiredNestedNodeTypes.filter((type) => contractQuality[type] !== "nested_shape_constrained");
  const requiredNestedSatisfied = requiredNestedNodeTypes.filter((type) => contractQuality[type] === "nested_shape_constrained");
  const nestedShapeConstrainedDeficit = Math.max(0, minimumNestedShapeConstrained - qualityCounts.nested_shape_constrained);

  const rustAuthorityDrift = [];
  if (!/NODE_CONFIG_CONTRACT_AUTHORITY:\s*&str\s*=\s*"contracts\/desktop\/node_config_contracts\.v1\.json"/.test(rustAuthorityText)) {
    rustAuthorityDrift.push("rust workflow contract authority constant drift");
  }
  if (!/fn\s+load_node_config_contracts\(\)/.test(rustAuthorityText)) {
    rustAuthorityDrift.push("rust workflow contract loader missing");
  }
  if (!/workflow_validation_assets\(\)/.test(rustAuthorityText)) {
    rustAuthorityDrift.push("rust workflow validation assets bootstrap missing");
  }
  if (!/validate_contract_backed_node_config/.test(rustAuthorityText)) {
    rustAuthorityDrift.push("rust contract-backed node config validation missing");
  }

  const rustHandlerDrift = [];
  if (!/workflow_contract_v1_operator/.test(rustHandlerText)) {
    rustHandlerDrift.push("rust workflow contract endpoint missing");
  }
  if (!/run_workflow_contract_v1\(WorkflowContractV1Req/.test(rustHandlerText)) {
    rustHandlerDrift.push("rust draft/reference execution no longer routes through workflow_contract_v1");
  }
  if (!/workflow_reference_run_v1_operator/.test(rustHandlerText)) {
    rustHandlerDrift.push("rust workflow reference execution endpoint missing");
  }
  if (!/workflow_draft_run_v1_operator/.test(rustHandlerText)) {
    rustHandlerDrift.push("rust workflow draft execution endpoint missing");
  }

  const glueValidationClientDrift = [];
  if (!/operator_url\(base_url,\s*"\/operators\/workflow_contract_v1\/validate"\)/.test(glueValidationClientText)) {
    glueValidationClientDrift.push("glue authoritative validation client no longer targets workflow_contract_v1 validate");
  }
  if (!/validation_scope/.test(glueValidationClientText)) {
    glueValidationClientDrift.push("glue authoritative validation client no longer forwards validation_scope");
  }

  const desktopGeneratedHelperReferences = [];
  if (/node_config_contract\.generated\.js/.test(desktopWorkflowContractText)) {
    desktopGeneratedHelperReferences.push("renderer/workflow/workflow-contract.js");
  }

  const packagingGeneratedHelperReferences = [];
  const fullFiles = Array.isArray(packageJson?.build?.files) ? packageJson.build.files : [];
  const liteFiles = Array.isArray(liteManifest?.files) ? liteManifest.files : [];
  if (fullFiles.includes("workflow_node_config_contract.generated.js")) {
    packagingGeneratedHelperReferences.push("apps/dify-desktop/package.json");
  }
  if (liteFiles.includes("workflow_node_config_contract.generated.js")) {
    packagingGeneratedHelperReferences.push("apps/dify-desktop/build/electron-builder.lite.json");
  }

  const generatedDesktopHelperFilesPresent = [];
  if (fs.existsSync(generatedCjsPath)) generatedDesktopHelperFilesPresent.push("apps/dify-desktop/workflow_node_config_contract.generated.js");
  if (fs.existsSync(generatedEsmPath)) generatedDesktopHelperFilesPresent.push("apps/dify-desktop/renderer/workflow/node_config_contract.generated.js");

  const issues = [];
  if (nestedShapeConstrainedDeficit > 0) {
    issues.push(`node config schema nested_shape_constrained count too low: ${qualityCounts.nested_shape_constrained} < ${minimumNestedShapeConstrained}`);
  }
  if (requiredNestedMissing.length > 0) {
    issues.push(`node config schema required nested coverage missing: ${requiredNestedMissing.join(", ")}`);
  }
  if (rustAuthorityDrift.length > 0) issues.push(...rustAuthorityDrift);
  if (rustHandlerDrift.length > 0) issues.push(...rustHandlerDrift);
  if (glueValidationClientDrift.length > 0) issues.push(...glueValidationClientDrift);
  if (desktopGeneratedHelperReferences.length > 0) {
    issues.push(`desktop workflow contract still imports generated node config helpers: ${desktopGeneratedHelperReferences.join(", ")}`);
  }
  if (packagingGeneratedHelperReferences.length > 0) {
    issues.push(`desktop packaging still references generated node config helpers: ${packagingGeneratedHelperReferences.join(", ")}`);
  }
  if (generatedDesktopHelperFilesPresent.length > 0) {
    issues.push(`generated desktop node config helper files still present: ${generatedDesktopHelperFilesPresent.join(", ")}`);
  }

  const rustAuthorityCoveredCount =
    rustAuthorityDrift.length === 0 && rustHandlerDrift.length === 0 && glueValidationClientDrift.length === 0
      ? contractTypes.length
      : 0;

  const payload = {
    status: issues.length > 0 ? "failed" : "passed",
    contractPath,
    rustAuthorityPath,
    rustHandlerPath,
    glueValidationClientPath,
    contractAuthority: String(contractJson?.authority || "").trim(),
    contractSchemaVersion: String(contractJson?.schema_version || "").trim(),
    contractValidatorKindCount: contractValidatorKinds.length,
    targetCount: contractTypes.length,
    coveredCount: rustAuthorityCoveredCount,
    minimumNestedShapeConstrained: minimumNestedShapeConstrained,
    nestedShapeConstrainedCount: qualityCounts.nested_shape_constrained,
    nestedShapeConstrainedDeficit,
    requiredNestedNodeTypes,
    requiredNestedSatisfied,
    requiredNestedMissing,
    rustAuthorityCoveredCount,
    qualityCounts,
    drift: {
      rustAuthorityDrift,
      rustHandlerDrift,
      glueValidationClientDrift,
      desktopGeneratedHelperReferences,
      packagingGeneratedHelperReferences,
      generatedDesktopHelperFilesPresent,
      requiredNestedTypesMissingFromContract: requiredNestedNodeTypes.filter((type) => !contractTypes.includes(type)),
    },
    covered: contractTypes,
    issues: uniqueSorted(issues),
  };

  console.log(JSON.stringify(payload));
  if (payload.status !== "passed") {
    process.exit(1);
  }
})().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
'@

$previousMinimum = $env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED
$previousRequired = $env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES_JSON
$env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED = [string]$MinimumNestedShapeConstrained
$env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES_JSON = $requiredTypesJson

try {
  $nodeScript | node - $RepoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "node config schema coverage checks failed"
  }
  Ok "node config schema coverage check passed"
}
finally {
  if ($null -eq $previousMinimum) {
    Remove-Item Env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_NODE_CONFIG_SCHEMA_MIN_NESTED_SHAPE_CONSTRAINED = $previousMinimum
  }
  if ($null -eq $previousRequired) {
    Remove-Item Env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES_JSON -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_NODE_CONFIG_SCHEMA_REQUIRED_NESTED_TYPES_JSON = $previousRequired
  }
}
