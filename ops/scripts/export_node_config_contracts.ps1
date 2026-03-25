param(
  [string]$RepoRoot = "",
  [string]$ContractPath = "",
  [string]$CjsModulePath = "",
  [string]$EsmModulePath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $ContractPath) {
  $ContractPath = Join-Path $RepoRoot "contracts\desktop\node_config_contracts.v1.json"
}
if (-not $CjsModulePath) {
  $CjsModulePath = Join-Path $RepoRoot "apps\dify-desktop\workflow_node_config_contract.generated.js"
}
if (-not $EsmModulePath) {
  $EsmModulePath = Join-Path $RepoRoot "apps\dify-desktop\renderer\workflow\node_config_contract.generated.js"
}

if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const support = require(process.argv[2]);
const contractPath = process.argv[3];
const cjsModulePath = process.argv[4];
const esmModulePath = process.argv[5];

const contract = support.loadNodeConfigContractSet(contractPath);
fs.mkdirSync(path.dirname(cjsModulePath), { recursive: true });
fs.mkdirSync(path.dirname(esmModulePath), { recursive: true });
fs.writeFileSync(cjsModulePath, support.renderCommonJsModule(contract), "utf8");
fs.writeFileSync(esmModulePath, support.renderEsmModule(contract), "utf8");
console.log(JSON.stringify({
  status: "passed",
  contractPath,
  cjsModulePath,
  esmModulePath,
  contractTypes: contract.contractTypes,
}));
'@

$supportPath = Join-Path $RepoRoot "ops\scripts\node_config_contract_support.js"
$nodeScript | node - $supportPath $ContractPath $CjsModulePath $EsmModulePath
if ($LASTEXITCODE -ne 0) {
  throw "node config contract export failed"
}

Ok "node config contract modules exported"
