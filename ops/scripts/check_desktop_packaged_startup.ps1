param(
  [string]$DesktopDir = "",
  [int]$BootWaitSec = 8
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DesktopDir) {
  $DesktopDir = Join-Path $root "apps\dify-desktop"
}
if (-not (Test-Path $DesktopDir)) {
  throw "desktop dir not found: $DesktopDir"
}

Push-Location $DesktopDir
try {
  Info "building desktop unpacked app"
  $builderCli = Join-Path $DesktopDir "node_modules\electron-builder\out\cli\cli.js"
  if (-not (Test-Path $builderCli)) {
    throw "electron-builder cli not found: $builderCli"
  }
  node --no-deprecation $builderCli --win --dir
  if ($LASTEXITCODE -ne 0) { throw "desktop unpacked build failed" }

  $exe = Join-Path $DesktopDir "dist\win-unpacked\AIWF Dify Desktop.exe"
  if (-not (Test-Path $exe)) {
    throw "unpacked exe not found: $exe"
  }

  Info "starting unpacked desktop executable"
  $proc = Start-Process -FilePath $exe -ArgumentList "--workflow" -PassThru
  Start-Sleep -Seconds $BootWaitSec

  if ($proc.HasExited) {
    throw "desktop process exited too early with code: $($proc.ExitCode)"
  }

  Stop-Process -Id $proc.Id -Force
  Ok "desktop packaged startup check passed"
}
finally {
  Pop-Location
}
