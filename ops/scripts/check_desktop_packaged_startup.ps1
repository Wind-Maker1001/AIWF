param(
  [string]$DesktopDir = "",
  [int]$BootWaitSec = 8
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Stop-LingeringDesktopProcesses([string]$WorkspaceRoot) {
  $workspacePrefix = ([System.IO.Path]::GetFullPath($WorkspaceRoot)).TrimEnd("\")
  $matches = @(Get-CimInstance Win32_Process -ErrorAction SilentlyContinue | Where-Object {
    $_.ProcessId -ne $PID -and
    $_.ExecutablePath -and
    ([System.IO.Path]::GetFullPath($_.ExecutablePath)).StartsWith($workspacePrefix, [System.StringComparison]::OrdinalIgnoreCase) -and
    @("electron.exe", "AIWF Dify Desktop.exe", "AIWF Dify Desktop Lite.exe") -contains $_.Name
  })

  foreach ($item in @($matches | Sort-Object ProcessId -Descending)) {
    try {
      taskkill /F /T /PID $item.ProcessId | Out-Null
      Warn ("stopped lingering desktop process pid={0} name={1}" -f $item.ProcessId, $item.Name)
    } catch {
      Warn ("failed stopping lingering desktop process pid={0}: {1}" -f $item.ProcessId, $_.Exception.Message)
    }
  }

  if ($matches.Count -gt 0) {
    Start-Sleep -Seconds 2
  }
}

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $DesktopDir) {
  $DesktopDir = Join-Path $root "apps\dify-desktop"
}
if (-not (Test-Path $DesktopDir)) {
  throw "desktop dir not found: $DesktopDir"
}

Push-Location $DesktopDir
try {
  $bootMarker = Join-Path $env:TEMP "aiwf_desktop_boot_marker.json"
  Remove-Item $bootMarker -Force -ErrorAction SilentlyContinue
  $prevBootMarker = $env:AIWF_BOOT_MARKER_PATH
  $proc = $null
  Stop-LingeringDesktopProcesses -WorkspaceRoot $DesktopDir
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
  $env:AIWF_BOOT_MARKER_PATH = $bootMarker
  $proc = Start-Process -FilePath $exe -ArgumentList "--workflow" -PassThru
  Start-Sleep -Seconds $BootWaitSec

  if ($proc.HasExited) {
    throw "desktop process exited too early with code: $($proc.ExitCode)"
  }
  if (-not (Test-Path $bootMarker)) {
    throw "desktop process did not complete startup boot marker handshake"
  }

  $boot = Get-Content -Raw -Encoding UTF8 $bootMarker | ConvertFrom-Json
  $stage = [string]($boot.stage)
  if ($stage -eq "boot_failed" -or $stage -eq "uncaught_exception") {
    throw ("desktop process reported startup failure stage=" + $stage + ": " + [string]($boot.error))
  }
  if ($stage -ne "bootstrapped" -and $stage -ne "app_ready") {
    throw ("desktop process boot marker stage is unexpected: " + $stage)
  }

  Stop-Process -Id $proc.Id -Force
  Ok "desktop packaged startup check passed"
}
finally {
  if ($proc -and -not $proc.HasExited) {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
  }
  if ($null -eq $prevBootMarker) { Remove-Item Env:AIWF_BOOT_MARKER_PATH -ErrorAction SilentlyContinue } else { $env:AIWF_BOOT_MARKER_PATH = $prevBootMarker }
  Remove-Item $bootMarker -Force -ErrorAction SilentlyContinue
  Pop-Location
}
