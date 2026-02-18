param(
  [string]$ProjectDir = "",
  [switch]$BuildWin,
  [switch]$BuildInstaller
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\dify-desktop"
}

if (-not (Test-Path $ProjectDir)) {
  throw "dify-desktop not found: $ProjectDir"
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}
if (-not (Get-Command npm -ErrorAction SilentlyContinue)) {
  throw "npm not found in PATH"
}

Push-Location $ProjectDir
try {
  Info "installing desktop dependencies"
  npm install
  if ($LASTEXITCODE -ne 0) { throw "npm install failed" }

  Info "running desktop smoke"
  npm run smoke
  if ($LASTEXITCODE -ne 0) { throw "desktop smoke failed" }
  Ok "desktop smoke passed"

  if ($BuildWin) {
    Info "building windows portable exe"
    npm run build:win
    if ($LASTEXITCODE -ne 0) { throw "build windows portable exe failed" }
    Ok "windows portable exe built at $ProjectDir\\dist"
    if ($BuildInstaller) {
      Info "building windows installer exe"
      npm run build:win:installer
      if ($LASTEXITCODE -ne 0) { throw "build windows installer exe failed" }
      Ok "windows installer exe built at $ProjectDir\\dist"
    }
    exit 0
  }

  Info "starting desktop app"
  npm run dev
}
finally {
  Pop-Location
}
