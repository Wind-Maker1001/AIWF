param(
  [string]$RepoRoot = "",
  [string]$ManifestPath = "",
  [string]$DesktopModulePath = "",
  [string]$WinUiModulePath = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not $ManifestPath) {
  $ManifestPath = Join-Path $RepoRoot "contracts\governance\governance_capabilities.v1.json"
}
if (-not $DesktopModulePath) {
  $DesktopModulePath = Join-Path $RepoRoot "apps\dify-desktop\workflow_governance_capabilities.generated.js"
}
if (-not $WinUiModulePath) {
  $WinUiModulePath = Join-Path $RepoRoot "apps\dify-native-winui\src\WinUI3Bootstrap\Runtime\GovernanceCapabilities.Generated.cs"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}

$pythonScript = @'
import json
import sys
from pathlib import Path

repo_root = Path(sys.argv[1])
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

from aiwf.governance_surface import list_governance_surface_entries

items = []
for entry in list_governance_surface_entries():
    items.append({
        "capability": str(entry.get("capability") or "").strip(),
        "route_prefix": str(entry.get("route_prefix") or "").strip(),
        "owned_route_prefixes": list(entry.get("owned_route_prefixes") or []),
    })

print(json.dumps({
    "schema_version": "governance_capabilities.v1",
    "source_authority": "apps/glue-python/aiwf/governance_surface.py",
    "items": items,
}, ensure_ascii=False))
'@

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const support = require(process.argv[2]);
const manifestPath = process.argv[3];
const desktopModulePath = process.argv[4];
const winUiModulePath = process.argv[5];

const surfaceExport = JSON.parse(process.env.AIWF_GOVERNANCE_CAPABILITY_SURFACE_EXPORT_JSON || "{}");
const manifest = support.buildGovernanceCapabilityDataFromSurfaceExport(surfaceExport);
fs.mkdirSync(path.dirname(manifestPath), { recursive: true });
fs.mkdirSync(path.dirname(desktopModulePath), { recursive: true });
fs.mkdirSync(path.dirname(winUiModulePath), { recursive: true });
fs.writeFileSync(manifestPath, support.renderManifestJson(manifest), "utf8");
fs.writeFileSync(desktopModulePath, support.renderDesktopModule(manifest), "utf8");
fs.writeFileSync(winUiModulePath, support.renderWinUiModule(manifest), "utf8");
console.log(JSON.stringify({
  status: "passed",
  manifestPath,
  desktopModulePath,
  winUiModulePath,
  capabilities: manifest.items.map((item) => item.capability),
}));
'@

$supportPath = Join-Path $RepoRoot "ops\scripts\governance_capability_support.js"
$surfaceExportJson = $pythonScript | python - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "governance capability source export failed"
}

$env:AIWF_GOVERNANCE_CAPABILITY_SURFACE_EXPORT_JSON = [string]$surfaceExportJson
try {
  $nodeScript | node - $supportPath $ManifestPath $DesktopModulePath $WinUiModulePath
  if ($LASTEXITCODE -ne 0) {
    throw "governance capability export failed"
  }
}
finally {
  Remove-Item Env:AIWF_GOVERNANCE_CAPABILITY_SURFACE_EXPORT_JSON -ErrorAction SilentlyContinue
}

Ok "governance capability assets exported"
