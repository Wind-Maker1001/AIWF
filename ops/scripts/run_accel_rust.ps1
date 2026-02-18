param(
  [string]$EnvFile = "",
  [string]$ProjectDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\accel-rust"
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) { return }

  Get-Content $Path | ForEach-Object {
    $line = $_.Trim()
    if ($line -eq "" -or $line.StartsWith("#")) { return }

    $idx = $line.IndexOf('=')
    if ($idx -le 0) { return }

    $k = $line.Substring(0, $idx).Trim()
    $v = $line.Substring($idx + 1).Trim().Trim('"').Trim("'")
    [System.Environment]::SetEnvironmentVariable($k, $v, "Process")
  }
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
  throw "cargo not found in PATH"
}

if (-not (Test-Path $ProjectDir)) {
  throw "accel-rust not found: $ProjectDir"
}

Import-DotEnv $EnvFile
Set-Location $ProjectDir

$port = if ($env:AIWF_ACCEL_RUST_PORT) { $env:AIWF_ACCEL_RUST_PORT } else { "18082" }
Info "Starting accel-rust on port $port"

cargo run
