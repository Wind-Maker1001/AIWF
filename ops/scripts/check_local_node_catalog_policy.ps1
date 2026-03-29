param(
  [string]$RepoRoot = "",
  [string[]]$RequiredLocalNodeTypes = @()
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

$nodeScript = @'
const path = require("path");
const { pathToFileURL } = require("url");

function uniqueSorted(values) {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function splitEnvList(name) {
  return uniqueSorted(String(process.env[name] || "").split(","));
}

(async () => {
  const repoRoot = process.argv[2];
  const requiredLocalNodeTypes = splitEnvList("AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES");

  const defaultsCatalogModule = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "defaults-catalog.js")).href);
  const localPresentationsModule = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "local-node-presentations.js")).href);
  const localPalettePolicyModule = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "local-node-palette-policy.js")).href);
  const workflowNodeCatalogContract = require(path.join(repoRoot, "apps", "dify-desktop", "workflow_node_catalog_contract.js"));

  const defaultsCatalog = Array.isArray(defaultsCatalogModule.NODE_CATALOG) ? defaultsCatalogModule.NODE_CATALOG : [];
  const localCatalog = Array.isArray(defaultsCatalogModule.LOCAL_NODE_CATALOG) ? defaultsCatalogModule.LOCAL_NODE_CATALOG : [];
  const localNodeTypes = Array.isArray(localPalettePolicyModule.LOCAL_NODE_TYPES) ? localPalettePolicyModule.LOCAL_NODE_TYPES.map(String) : [];
  const workflowNodeCatalogLocalTypes = Array.isArray(workflowNodeCatalogContract.LOCAL_WORKFLOW_NODE_TYPES)
    ? workflowNodeCatalogContract.LOCAL_WORKFLOW_NODE_TYPES.map(String)
    : [];
  const workflowNodeCatalogRegisteredTypes = Array.isArray(workflowNodeCatalogContract.REGISTERED_WORKFLOW_NODE_TYPES)
    ? workflowNodeCatalogContract.REGISTERED_WORKFLOW_NODE_TYPES.map(String)
    : [];
  const buildLocalNodePalettePolicy = localPalettePolicyModule.buildLocalNodePalettePolicy;
  if (typeof buildLocalNodePalettePolicy !== "function") {
    throw new Error("buildLocalNodePalettePolicy export missing");
  }

  const localPresentations = localPresentationsModule.LOCAL_NODE_PRESENTATIONS && typeof localPresentationsModule.LOCAL_NODE_PRESENTATIONS === "object"
    ? localPresentationsModule.LOCAL_NODE_PRESENTATIONS
    : {};
  const policyResult = buildLocalNodePalettePolicy(localPresentations);

  const localTypeSet = new Set(localNodeTypes);
  const runtimeLocalCatalog = defaultsCatalog.filter((item) => localTypeSet.has(String(item?.type || "").trim()));
  const runtimeLocalTypes = uniqueSorted(runtimeLocalCatalog.map((item) => item?.type));
  const expectedEntries = Array.isArray(policyResult.entries) ? policyResult.entries : [];
  const expectedByType = new Map(expectedEntries.map((item) => [String(item.type), item]));
  const runtimeByType = new Map(runtimeLocalCatalog.map((item) => [String(item.type), item]));

  const missingCatalogTypes = localNodeTypes.filter((type) => !runtimeByType.has(type));
  const staleCatalogTypes = runtimeLocalTypes.filter((type) => !localTypeSet.has(type));
  const missingWorkflowNodeCatalogTypes = localNodeTypes.filter((type) => !workflowNodeCatalogRegisteredTypes.includes(type));
  const workflowNodeCatalogLocalTypeDrift = uniqueSorted([
    ...localNodeTypes.filter((type) => !workflowNodeCatalogLocalTypes.includes(type)),
    ...workflowNodeCatalogLocalTypes.filter((type) => !localTypeSet.has(type)),
  ]);
  const missingCatalogPolicySourceTypes = runtimeLocalCatalog
    .filter((item) => String(item?.policy_source || "").trim().toLowerCase() !== "local_policy")
    .map((item) => String(item?.type || "").trim())
    .filter(Boolean);
  const catalogMetadataDrift = localNodeTypes.filter((type) => runtimeByType.has(type)
    && JSON.stringify(expectedByType.get(type)) !== JSON.stringify(runtimeByType.get(type)));
  const requiredLocalTypeMissing = requiredLocalNodeTypes.filter((type) => !localTypeSet.has(type));

  const issues = [];
  if (!policyResult.ok) {
    issues.push(...policyResult.errors);
  }
  if (missingCatalogTypes.length > 0) {
    issues.push(`defaults-catalog.js missing local node types: ${missingCatalogTypes.join(", ")}`);
  }
  if (staleCatalogTypes.length > 0) {
    issues.push(`defaults-catalog.js has stale local node types: ${staleCatalogTypes.join(", ")}`);
  }
  if (missingWorkflowNodeCatalogTypes.length > 0) {
    issues.push(`workflow_node_catalog_contract.js missing local node types: ${missingWorkflowNodeCatalogTypes.join(", ")}`);
  }
  if (workflowNodeCatalogLocalTypeDrift.length > 0) {
    issues.push(`workflow_node_catalog_contract.js local node type drift: ${workflowNodeCatalogLocalTypeDrift.join(", ")}`);
  }
  if (missingCatalogPolicySourceTypes.length > 0) {
    issues.push(`defaults-catalog.js local node policy_source drift: ${missingCatalogPolicySourceTypes.join(", ")}`);
  }
  if (catalogMetadataDrift.length > 0) {
    issues.push(`defaults-catalog.js local node metadata drift: ${catalogMetadataDrift.join(", ")}`);
  }
  if (requiredLocalTypeMissing.length > 0) {
    issues.push(`required local node types missing from policy truth: ${requiredLocalTypeMissing.join(", ")}`);
  }

  const summary = {
    status: issues.length > 0 ? "failed" : "passed",
    localNodeTypeCount: localNodeTypes.length,
    localCatalogCount: runtimeLocalCatalog.length,
    workflowNodeCatalogLocalTypeCount: workflowNodeCatalogLocalTypes.length,
    requiredLocalNodeTypes,
    drift: {
      invalidSections: Array.isArray(policyResult.details?.invalidSections) ? policyResult.details.invalidSections : [],
      missingSectionTypes: Array.isArray(policyResult.details?.missingSectionTypes) ? policyResult.details.missingSectionTypes : [],
      missingPresentationTypes: Array.isArray(policyResult.details?.missingPresentationTypes) ? policyResult.details.missingPresentationTypes : [],
      stalePresentationTypes: Array.isArray(policyResult.details?.stalePresentationTypes) ? policyResult.details.stalePresentationTypes : [],
      invalidPresentationEntries: Array.isArray(policyResult.details?.invalidPresentationEntries) ? policyResult.details.invalidPresentationEntries : [],
      duplicatePinnedTypes: Array.isArray(policyResult.details?.duplicatePinnedTypes) ? policyResult.details.duplicatePinnedTypes : [],
      stalePinnedTypes: Array.isArray(policyResult.details?.stalePinnedTypes) ? policyResult.details.stalePinnedTypes : [],
      missingCatalogTypes,
      staleCatalogTypes,
      missingWorkflowNodeCatalogTypes,
      workflowNodeCatalogLocalTypeDrift,
      missingCatalogPolicySourceTypes,
      catalogMetadataDrift,
      requiredLocalTypeMissing,
    },
    issues: uniqueSorted(issues),
  };

  console.log(JSON.stringify(summary));
  if (summary.status !== "passed") {
    process.exit(1);
  }
})().catch((error) => {
  console.log(JSON.stringify({
    status: "failed",
    issues: [error && error.stack ? error.stack : String(error)],
  }));
  process.exit(1);
});
'@

if ($null -eq $env:AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES) {
  $previousRequiredLocalNodeTypes = $null
} else {
  $previousRequiredLocalNodeTypes = $env:AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES
}
$env:AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES = (@($RequiredLocalNodeTypes | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join ",")

try {
  $nodeScript | node - $RepoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "local node catalog policy checks failed"
  }

  Ok "local node catalog policy check passed"
}
finally {
  if ($null -eq $previousRequiredLocalNodeTypes) {
    Remove-Item Env:AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_LOCAL_NODE_POLICY_REQUIRED_TYPES = $previousRequiredLocalNodeTypes
  }
}
