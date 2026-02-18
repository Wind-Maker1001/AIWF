param(
  [string]$EnvFile = "",
  [string]$ProjectDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Err($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\base-java"
}

if (-not (Test-Path $ProjectDir)) {
  throw "base-java not found: $ProjectDir"
}

if (Test-Path $EnvFile) {
  $env:AIWF_ENV_FILE = $EnvFile
  Info "AIWF_ENV_FILE=$EnvFile"
} else {
  Info "env file not found, continuing: $EnvFile"
}

if (-not (Get-Command mvn -ErrorAction SilentlyContinue)) {
  Err "mvn not found in PATH. Install Maven and JDK 21 first."
  exit 1
}

$validateScript = Join-Path $root "ops\scripts\validate_env.ps1"
if (Test-Path $validateScript) {
  powershell -ExecutionPolicy Bypass -File $validateScript -EnvFile $EnvFile
}

Set-Location $ProjectDir
Info "Starting base-java ..."
mvn "spring-boot:run"
