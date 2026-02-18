param(
  [string]$DesktopDir = "",
  [int]$BootWaitSec = 8
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DesktopDir) {
  $DesktopDir = Join-Path $root "apps\dify-desktop"
}
if (-not (Test-Path $DesktopDir)) {
  throw "desktop dir not found: $DesktopDir"
}
$DesktopDir = (Resolve-Path $DesktopDir).Path

Push-Location $DesktopDir
try {
  Info "building desktop lite unpacked app"
  $builderCli = Join-Path $DesktopDir "node_modules\electron-builder\out\cli\cli.js"
  if (-not (Test-Path $builderCli)) {
    throw "electron-builder cli not found: $builderCli"
  }
  node --no-deprecation $builderCli --config build/electron-builder.lite.json --win --dir
  if ($LASTEXITCODE -ne 0) { throw "desktop lite unpacked build failed" }

  $unpackedDir = Join-Path $DesktopDir "dist-lite\win-unpacked"
  if (-not (Test-Path $unpackedDir)) {
    throw "lite unpacked dir not found: $unpackedDir"
  }
  $fullUnpackedDir = Join-Path $DesktopDir "dist\win-unpacked"
  if (Test-Path $fullUnpackedDir) {
    $fullBytes = (Get-ChildItem -Path $fullUnpackedDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    $liteBytes = (Get-ChildItem -Path $unpackedDir -Recurse -File -ErrorAction SilentlyContinue | Measure-Object -Property Length -Sum).Sum
    $deltaMb = [math]::Round(($fullBytes - $liteBytes) / 1MB, 2)
    if ($deltaMb -lt 20) {
      throw "lite unpacked payload delta too small: ${deltaMb}MB (expected >= 20MB)"
    }
    Ok "lite unpacked payload delta: ${deltaMb}MB"
  } else {
    Warn "full unpacked dir missing, skip payload delta check: $fullUnpackedDir"
  }
  $fullInstaller = Join-Path $DesktopDir "dist\AIWF Dify Desktop Setup 1.0.1.exe"
  $liteInstaller = Join-Path $DesktopDir "dist-lite\AIWF Dify Desktop Lite Setup 1.0.1.exe"
  if ((Test-Path $fullInstaller) -and (Test-Path $liteInstaller)) {
    $fullMb = [math]::Round((Get-Item $fullInstaller).Length / 1MB, 2)
    $liteMb = [math]::Round((Get-Item $liteInstaller).Length / 1MB, 2)
    $installerDelta = [math]::Round($fullMb - $liteMb, 2)
    Ok "installer size full=${fullMb}MB lite=${liteMb}MB delta=${installerDelta}MB"
  }

  $exe = Get-ChildItem -Path $unpackedDir -Filter "*.exe" -File |
    Where-Object { $_.Name -notlike "unins*.exe" } |
    Select-Object -First 1
  if (-not $exe) {
    throw "lite unpacked exe not found in: $unpackedDir"
  }

  $toolsDir = Join-Path $unpackedDir "resources\tools"
  if (-not (Test-Path $toolsDir)) {
    throw "lite tools dir missing: $toolsDir"
  }
  $litePayload = @(Get-ChildItem -Path $toolsDir -Recurse -File -ErrorAction SilentlyContinue |
    Where-Object { $_.Name -notmatch '^README\.md$|^manifest.*\.json$' })
  if ($litePayload.Count -gt 0) {
    $sample = ($litePayload | Select-Object -First 3 | ForEach-Object { $_.FullName }) -join "; "
    throw "lite package contains unexpected tool payload files: $sample"
  }

  Info "starting lite unpacked desktop executable"
  $proc = Start-Process -FilePath $exe.FullName -ArgumentList "--workflow" -PassThru
  Start-Sleep -Seconds $BootWaitSec

  if ($proc.HasExited) {
    throw "desktop lite process exited too early with code: $($proc.ExitCode)"
  }

  Stop-Process -Id $proc.Id -Force
  Ok "desktop lite packaged startup check passed"
}
finally {
  Pop-Location
}
