param(
  [string]$ProjectDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\dify-native-winui\src\WinUI3Bootstrap"
}

Info "native bootstrap dir: $ProjectDir"

try {
  dotnet --info | Out-Null
}
catch {
  Warn "dotnet SDK not found. Install .NET SDK 8+ and WinUI 3 workload first."
  exit 1
}

Warn "WinUI project file is not generated yet in this scaffold."
Warn "Next step: create WinUI 3 csproj/solution in $ProjectDir, then run dotnet build."
exit 1
