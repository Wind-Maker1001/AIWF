param(
  [string]$RepoRoot = "",
  [string[]]$RequiredPublishedOperators = @(),
  [string]$ManifestPath = ""
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

$supportPath = Join-Path $PSScriptRoot "operator_manifest_support.js"
if (-not (Test-Path $supportPath)) {
  throw "operator manifest support script not found: $supportPath"
}

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");
const support = require(process.argv[2]);

function parseRegexMatches(text, regex) {
  return [...text.matchAll(regex)].map((match) => match[1]);
}

function uniqueSorted(values) {
  return Array.from(new Set(values.map((value) => String(value || "").trim()).filter(Boolean))).sort();
}

function splitEnvList(name) {
  return uniqueSorted(String(process.env[name] || "").split(","));
}

function mapByOperator(items) {
  return new Map((Array.isArray(items) ? items : []).map((item) => [String(item.operator), item]));
}

(async () => {
  const repoRoot = process.argv[3];
  const manifestPathArg = process.env.AIWF_OPERATOR_MANIFEST_PATH || "";
  const manifestPath = support.getManifestPath(repoRoot, manifestPathArg || support.DEFAULT_MANIFEST_RELATIVE_PATH);
  const schemaPath = support.getManifestPath(repoRoot, support.DEFAULT_MANIFEST_SCHEMA_RELATIVE_PATH);
  const desktopModulePath = support.getDesktopModulePath(repoRoot, support.DEFAULT_DESKTOP_MODULE_RELATIVE_PATH);
  const rendererModulePath = support.getRendererModulePath(repoRoot, support.DEFAULT_RENDERER_MODULE_RELATIVE_PATH);
  const requiredPublishedOperators = splitEnvList("AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS");
  const generatedManifest = support.buildOperatorManifest(repoRoot);
  const expectedDesktopModuleSource = support.buildDesktopRustOperatorModuleSource(generatedManifest);
  const expectedRendererModuleSource = support.buildRendererRustOperatorModuleSource(generatedManifest);

  let checkedInManifest = null;
  let manifestReadError = "";
  try {
    checkedInManifest = support.readOperatorManifest(manifestPath);
  } catch (error) {
    manifestReadError = error && error.message ? error.message : String(error);
  }

  const schemaMissing = !fs.existsSync(schemaPath);
  const rendererModuleMissing = !fs.existsSync(rendererModulePath);
  let desktopModule = null;
  let desktopModuleReadError = "";
  try {
    desktopModule = require(desktopModulePath);
  } catch (error) {
    desktopModuleReadError = error && error.message ? error.message : String(error);
  }
  let rendererModuleDrift = false;
  let rendererModuleReadError = "";
  if (!rendererModuleMissing) {
    try {
      rendererModuleDrift = fs.readFileSync(rendererModulePath, "utf8") !== expectedRendererModuleSource;
    } catch (error) {
      rendererModuleReadError = error && error.message ? error.message : String(error);
    }
  }

  const defaultsCatalogPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "defaults-catalog.js");
  const rustPresentationPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "rust-operator-presentations.js");
  const rustPalettePolicyPath = path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "rust-operator-palette-policy.js");
  const registryPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_chiplets", "registry.js");
  const rustOpsDomainPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_chiplets", "domains", "rust_ops_domain.js");
  const builtinDomainsPath = path.join(repoRoot, "apps", "dify-desktop", "workflow_chiplets", "domains", "builtin_domains.js");

  let rustPresentationModule = null;
  let rustPresentationReadError = "";
  try {
    rustPresentationModule = await import(pathToFileURL(rustPresentationPath).href);
  } catch (error) {
    rustPresentationReadError = error && error.message ? error.message : String(error);
  }
  let rustPalettePolicyModule = null;
  let rustPalettePolicyReadError = "";
  try {
    rustPalettePolicyModule = await import(pathToFileURL(rustPalettePolicyPath).href);
  } catch (error) {
    rustPalettePolicyReadError = error && error.message ? error.message : String(error);
  }

  let defaultsCatalogModule = null;
  let defaultsCatalogReadError = "";
  try {
    defaultsCatalogModule = await import(pathToFileURL(defaultsCatalogPath).href);
  } catch (error) {
    defaultsCatalogReadError = error && error.message ? error.message : String(error);
  }
  const defaultsCatalog = uniqueSorted(
    (Array.isArray(defaultsCatalogModule.NODE_CATALOG) ? defaultsCatalogModule.NODE_CATALOG : [])
      .map((item) => String(item?.type || "")),
  );
  const defaultsCatalogItems = Array.isArray(defaultsCatalogModule.NODE_CATALOG) ? defaultsCatalogModule.NODE_CATALOG : [];
  const rustPresentations = rustPresentationModule && rustPresentationModule.RUST_OPERATOR_PRESENTATIONS && typeof rustPresentationModule.RUST_OPERATOR_PRESENTATIONS === "object"
    ? rustPresentationModule.RUST_OPERATOR_PRESENTATIONS
    : {};

  const { WorkflowChipletRegistry } = require(registryPath);
  const { registerRustOpsDomainChiplets } = require(rustOpsDomainPath);
  const { registerBuiltinWorkflowDomains } = require(builtinDomainsPath);

  const rustRegistry = new WorkflowChipletRegistry();
  registerRustOpsDomainChiplets(
    rustRegistry,
    { runIsolatedTask: async () => ({ ok: true }) },
    {
      rustBase: () => "http://127.0.0.1:18082",
      rustRequired: () => true,
      resolveIsolationLevel: () => "none",
      resolveSandboxLimits: () => ({}),
    },
  );
  const rustMappedOperators = uniqueSorted(rustRegistry.list());

  const builtinRegistry = new WorkflowChipletRegistry();
  registerBuiltinWorkflowDomains(builtinRegistry, {
    fs,
    path,
    collectFiles: () => [],
    summarizeCorpus: () => ({}),
    computeViaRust: async () => ({}),
    runOfflineCleaning: async () => ({ artifacts: [] }),
    readArtifactById: () => null,
    writeWorkflowSummary: async () => ({ path: "", sha256: "" }),
    sha256Text: () => "",
    nodeOutputByType: () => ({}),
    callExternalAi: async () => ({ ok: true, text: "" }),
    auditAiText: async () => ({ passed: true, reasons: [] }),
    runIsolatedTask: async () => ({ ok: true }),
  });
  const builtinOperators = uniqueSorted(builtinRegistry.list());

  const generatedByOperator = mapByOperator(generatedManifest.operators);
  const checkedInByOperator = mapByOperator(checkedInManifest ? checkedInManifest.operators : []);
  const checkedInDesktopByOperator = mapByOperator(checkedInManifest
    ? checkedInManifest.operators.filter((item) => item.desktop_exposable)
    : []);
  const generatedOperators = uniqueSorted([...generatedByOperator.keys()]);
  const checkedInOperators = uniqueSorted([...checkedInByOperator.keys()]);
  const publishedOperators = generatedManifest.operators.filter((item) => item.published).map((item) => item.operator);
  const workflowExposableOperators = generatedManifest.operators.filter((item) => item.workflow_exposable).map((item) => item.operator);
  const desktopExposableOperators = generatedManifest.operators.filter((item) => item.desktop_exposable).map((item) => item.operator);
  const palettePolicyResult = rustPalettePolicyModule && typeof rustPalettePolicyModule.buildRustOperatorPalettePolicy === "function"
    ? rustPalettePolicyModule.buildRustOperatorPalettePolicy(
      Object.fromEntries((checkedInManifest ? checkedInManifest.operators : generatedManifest.operators)
        .filter((item) => item.desktop_exposable)
        .map((item) => [item.operator, item])),
      rustPresentations,
    )
    : { ok: false, errors: rustPalettePolicyReadError ? [rustPalettePolicyReadError] : ["rust operator palette policy module missing builder"], details: {} };
  const rustPresentationTypes = uniqueSorted(Object.keys(rustPresentations));
  const missingPresentationOperators = Array.isArray(palettePolicyResult?.details?.missingPresentationOperators)
    ? uniqueSorted(palettePolicyResult.details.missingPresentationOperators)
    : desktopExposableOperators.filter((operator) => !Object.prototype.hasOwnProperty.call(rustPresentations, operator));
  const stalePresentationOperators = Array.isArray(palettePolicyResult?.details?.stalePresentationOperators)
    ? uniqueSorted(palettePolicyResult.details.stalePresentationOperators)
    : rustPresentationTypes.filter((operator) => !desktopExposableOperators.includes(operator));
  const invalidPresentationEntries = Array.isArray(palettePolicyResult?.details?.invalidPresentationEntries)
    ? uniqueSorted(palettePolicyResult.details.invalidPresentationEntries)
    : rustPresentationTypes.filter((operator) => {
      const item = rustPresentations[operator];
      return !item
        || typeof item !== "object"
        || Array.isArray(item)
        || !String(item.name || "").trim()
        || !String(item.desc || "").trim();
    });
  const missingPaletteSectionDomains = Array.isArray(palettePolicyResult?.details?.missingPaletteSectionDomains)
    ? uniqueSorted(palettePolicyResult.details.missingPaletteSectionDomains)
    : [];
  const invalidPaletteSections = Array.isArray(palettePolicyResult?.details?.invalidSections)
    ? uniqueSorted(palettePolicyResult.details.invalidSections)
    : [];
  const stalePinnedOperators = Array.isArray(palettePolicyResult?.details?.stalePinnedOperators)
    ? uniqueSorted(palettePolicyResult.details.stalePinnedOperators)
    : [];
  const duplicatePinnedOperators = Array.isArray(palettePolicyResult?.details?.duplicatePinnedOperators)
    ? uniqueSorted(palettePolicyResult.details.duplicatePinnedOperators)
    : [];
  const rustKnownSet = new Set(generatedOperators);
  const workflowExposableSet = new Set(workflowExposableOperators);
  const desktopExposableSet = new Set(desktopExposableOperators);

  const manifestMissingOperators = checkedInManifest ? generatedOperators.filter((operator) => !checkedInByOperator.has(operator)) : generatedOperators;
  const manifestStaleOperators = checkedInManifest ? checkedInOperators.filter((operator) => !generatedByOperator.has(operator)) : [];
  const manifestMetadataDrift = checkedInManifest
    ? generatedOperators.filter((operator) => checkedInByOperator.has(operator)
      && JSON.stringify(generatedByOperator.get(operator)) !== JSON.stringify(checkedInByOperator.get(operator)))
    : [];
  const desktopModuleTypes = desktopModule && Array.isArray(desktopModule.DESKTOP_RUST_OPERATOR_TYPES)
    ? uniqueSorted(desktopModule.DESKTOP_RUST_OPERATOR_TYPES)
    : [];
  const desktopModuleByOperator = desktopModule && desktopModule.DESKTOP_RUST_OPERATOR_METADATA && typeof desktopModule.DESKTOP_RUST_OPERATOR_METADATA === "object"
    ? mapByOperator(Object.entries(desktopModule.DESKTOP_RUST_OPERATOR_METADATA).map(([operator, item]) => ({
      operator,
      ...item,
    })))
    : new Map();
  const desktopModuleMissingOperators = checkedInManifest
    ? desktopExposableOperators.filter((operator) => !desktopModuleByOperator.has(operator))
    : [];
  const desktopModuleStaleOperators = checkedInManifest
    ? desktopModuleTypes.filter((operator) => !checkedInDesktopByOperator.has(operator))
    : desktopModuleTypes;
  const desktopModuleMetadataDrift = checkedInManifest
    ? desktopExposableOperators.filter((operator) => desktopModuleByOperator.has(operator)
      && JSON.stringify(checkedInDesktopByOperator.get(operator)) !== JSON.stringify(desktopModuleByOperator.get(operator)))
    : [];

  const defaultsCatalogRustOperators = defaultsCatalog.filter((operator) => rustKnownSet.has(operator));
  const rustCatalogItems = defaultsCatalogItems.filter((item) => rustKnownSet.has(String(item?.type || "").trim()));
  const missingPublishedInCatalog = publishedOperators.filter((operator) => !defaultsCatalog.includes(operator));
  const missingPublishedInRouting = publishedOperators.filter((operator) => !rustMappedOperators.includes(operator));
  const missingDesktopExposableInCatalog = desktopExposableOperators.filter((operator) => !defaultsCatalog.includes(operator));
  const missingDesktopExposableInRouting = desktopExposableOperators.filter((operator) => !rustMappedOperators.includes(operator));
  const missingCatalogGroupEntries = rustCatalogItems
    .filter((item) => !String(item?.group || "").trim())
    .map((item) => String(item?.type || "").trim())
    .filter(Boolean);
  const missingCatalogPolicySectionEntries = rustCatalogItems
    .filter((item) => !String(item?.policy_section || "").trim())
    .map((item) => String(item?.type || "").trim())
    .filter(Boolean);
  const missingCatalogPolicySourceEntries = rustCatalogItems
    .filter((item) => String(item?.policy_source || "").trim().toLowerCase() !== "rust_manifest")
    .map((item) => String(item?.type || "").trim())
    .filter(Boolean);
  const staleRustRouting = rustMappedOperators.filter((operator) => !desktopExposableSet.has(operator));
  const staleCatalogOperators = defaultsCatalogRustOperators.filter((operator) => !desktopExposableSet.has(operator));
  const requiredPublishedMissing = requiredPublishedOperators.filter((operator) => !publishedOperators.includes(operator));

  const issues = [];
  if (manifestReadError) {
    issues.push(`operators manifest unreadable: ${manifestReadError}`);
  }
  if (schemaMissing) {
    issues.push(`operators manifest schema missing: ${schemaPath}`);
  }
  if (desktopModuleReadError) {
    issues.push(`desktop rust operator manifest module unreadable: ${desktopModuleReadError}`);
  }
  if (rustPresentationReadError) {
    issues.push(`rust operator presentations unreadable: ${rustPresentationReadError}`);
  }
  if (rustPalettePolicyReadError) {
    issues.push(`rust operator palette policy unreadable: ${rustPalettePolicyReadError}`);
  }
  if (defaultsCatalogReadError) {
    issues.push(`defaults catalog unreadable: ${defaultsCatalogReadError}`);
  }
  if (rendererModuleMissing) {
    issues.push(`renderer rust operator manifest module missing: ${rendererModulePath}`);
  }
  if (rendererModuleReadError) {
    issues.push(`renderer rust operator manifest module unreadable: ${rendererModuleReadError}`);
  }
  if (rendererModuleDrift) {
    issues.push(`renderer rust operator manifest module drift: ${rendererModulePath}`);
  }
  if (manifestMissingOperators.length > 0) {
    issues.push(`checked-in operators manifest missing Rust operators: ${manifestMissingOperators.join(", ")}`);
  }
  if (manifestStaleOperators.length > 0) {
    issues.push(`checked-in operators manifest has stale operators: ${manifestStaleOperators.join(", ")}`);
  }
  if (manifestMetadataDrift.length > 0) {
    issues.push(`checked-in operators manifest metadata drift: ${manifestMetadataDrift.join(", ")}`);
  }
  if (desktopModuleMissingOperators.length > 0) {
    issues.push(`desktop rust operator manifest module missing desktop-exposable operators: ${desktopModuleMissingOperators.join(", ")}`);
  }
  if (desktopModuleStaleOperators.length > 0) {
    issues.push(`desktop rust operator manifest module has stale operators: ${desktopModuleStaleOperators.join(", ")}`);
  }
  if (desktopModuleMetadataDrift.length > 0) {
    issues.push(`desktop rust operator manifest module metadata drift: ${desktopModuleMetadataDrift.join(", ")}`);
  }
  if (!palettePolicyResult.ok) {
    for (const error of uniqueSorted(palettePolicyResult.errors || [])) {
      issues.push(`rust operator palette policy invalid: ${error}`);
    }
  }
  if (missingPaletteSectionDomains.length > 0) {
    issues.push(`rust operator palette policy missing domains: ${missingPaletteSectionDomains.join(", ")}`);
  }
  if (invalidPaletteSections.length > 0) {
    issues.push(`rust operator palette sections invalid: ${invalidPaletteSections.join(", ")}`);
  }
  if (stalePinnedOperators.length > 0) {
    issues.push(`rust operator palette pinned order has stale operators: ${stalePinnedOperators.join(", ")}`);
  }
  if (duplicatePinnedOperators.length > 0) {
    issues.push(`rust operator palette pinned order duplicated: ${duplicatePinnedOperators.join(", ")}`);
  }
  if (missingPresentationOperators.length > 0) {
    issues.push(`rust operator presentations missing desktop-exposable operators: ${missingPresentationOperators.join(", ")}`);
  }
  if (stalePresentationOperators.length > 0) {
    issues.push(`rust operator presentations have stale operators: ${stalePresentationOperators.join(", ")}`);
  }
  if (invalidPresentationEntries.length > 0) {
    issues.push(`rust operator presentations invalid entries: ${invalidPresentationEntries.join(", ")}`);
  }
  if (missingPublishedInCatalog.length > 0) {
    issues.push(`desktop defaults-catalog.js missing published Rust operators: ${missingPublishedInCatalog.join(", ")}`);
  }
  if (missingPublishedInRouting.length > 0) {
    issues.push(`desktop rust_ops_domain.js missing published Rust operators: ${missingPublishedInRouting.join(", ")}`);
  }
  if (missingDesktopExposableInCatalog.length > 0) {
    issues.push(`desktop defaults-catalog.js missing desktop-exposable Rust operators: ${missingDesktopExposableInCatalog.join(", ")}`);
  }
  if (missingDesktopExposableInRouting.length > 0) {
    issues.push(`desktop rust_ops_domain.js missing desktop-exposable Rust operators: ${missingDesktopExposableInRouting.join(", ")}`);
  }
  if (missingCatalogGroupEntries.length > 0) {
    issues.push(`desktop defaults-catalog.js missing Rust operator groups: ${missingCatalogGroupEntries.join(", ")}`);
  }
  if (missingCatalogPolicySectionEntries.length > 0) {
    issues.push(`desktop defaults-catalog.js missing Rust operator policy sections: ${missingCatalogPolicySectionEntries.join(", ")}`);
  }
  if (missingCatalogPolicySourceEntries.length > 0) {
    issues.push(`desktop defaults-catalog.js missing Rust operator policy sources: ${missingCatalogPolicySourceEntries.join(", ")}`);
  }
  if (staleRustRouting.length > 0) {
    issues.push(`desktop rust_ops_domain.js exposes Rust operators outside manifest desktop exposure: ${staleRustRouting.join(", ")}`);
  }
  if (staleCatalogOperators.length > 0) {
    issues.push(`desktop defaults-catalog.js exposes Rust operators outside manifest desktop exposure: ${staleCatalogOperators.join(", ")}`);
  }
  if (requiredPublishedMissing.length > 0) {
    issues.push(`required published Rust operators missing from catalog truth: ${requiredPublishedMissing.join(", ")}`);
  }

  const summary = {
    status: issues.length > 0 ? "failed" : "passed",
    manifestPath,
    schemaPath,
    desktopModulePath,
    rendererModulePath,
    manifestOperatorCount: generatedManifest.summary.known_operator_count,
    publishedCount: generatedManifest.summary.published_operator_count,
    workflowCount: generatedManifest.summary.workflow_exposable_count,
    desktopExposableCount: generatedManifest.summary.desktop_exposable_count,
    desktopModuleCount: desktopModuleTypes.length,
    rustMappedCount: rustMappedOperators.length,
    defaultsCatalogCount: defaultsCatalog.length,
    builtinOperatorCount: builtinOperators.length,
    requiredPublishedOperators,
    drift: {
      manifestMissingOperators,
      manifestStaleOperators,
      manifestMetadataDrift,
      desktopModuleMissingOperators,
      desktopModuleStaleOperators,
      desktopModuleMetadataDrift,
      rendererModuleDrift: rendererModuleDrift ? ["renderer_module"] : [],
      invalidPaletteSections,
      missingPaletteSectionDomains,
      stalePinnedOperators,
      duplicatePinnedOperators,
      missingPresentationOperators,
      stalePresentationOperators,
      invalidPresentationEntries,
      missingPublishedInCatalog,
      missingPublishedInRouting,
      missingDesktopExposableInCatalog,
      missingDesktopExposableInRouting,
      missingCatalogGroupEntries,
      missingCatalogPolicySectionEntries,
      missingCatalogPolicySourceEntries,
      staleRustRouting,
      staleCatalogOperators,
      requiredPublishedMissing,
    },
    issues,
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

if ($null -eq $env:AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS) {
  $previousRequiredPublishedOperators = $null
} else {
  $previousRequiredPublishedOperators = $env:AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS
}
if ($null -eq $env:AIWF_OPERATOR_MANIFEST_PATH) {
  $previousManifestPath = $null
} else {
  $previousManifestPath = $env:AIWF_OPERATOR_MANIFEST_PATH
}
$env:AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS = (@($RequiredPublishedOperators | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) -join ",")
$env:AIWF_OPERATOR_MANIFEST_PATH = $ManifestPath

try {
  $nodeScript | node - $supportPath $RepoRoot
  if ($LASTEXITCODE -ne 0) {
    throw "operator catalog sync checks failed"
  }

  Ok "operator catalog sync check passed"
}
finally {
  if ($null -eq $previousRequiredPublishedOperators) {
    Remove-Item Env:AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_OPERATOR_CATALOG_REQUIRED_PUBLISHED_OPERATORS = $previousRequiredPublishedOperators
  }
  if ($null -eq $previousManifestPath) {
    Remove-Item Env:AIWF_OPERATOR_MANIFEST_PATH -ErrorAction SilentlyContinue
  } else {
    $env:AIWF_OPERATOR_MANIFEST_PATH = $previousManifestPath
  }
}
