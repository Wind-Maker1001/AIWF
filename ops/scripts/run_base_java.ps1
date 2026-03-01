param(
  [string]$EnvFile = "",
  [string]$ProjectDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Err($m){ Write-Host "[ERR ] $m" -ForegroundColor Red }
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
function IsTrueLike([string]$v) {
  if ([string]::IsNullOrWhiteSpace($v)) { return $false }
  $x = $v.Trim()
  return [string]::Equals($x, "1", [System.StringComparison]::OrdinalIgnoreCase) -or `
    [string]::Equals($x, "true", [System.StringComparison]::OrdinalIgnoreCase) -or `
    [string]::Equals($x, "yes", [System.StringComparison]::OrdinalIgnoreCase)
}
function IsPlaceholderPassword([string]$v) {
  if ([string]::IsNullOrWhiteSpace($v)) { return $false }
  return $v -match "^__SET_.*__$"
}

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
  Import-DotEnv $EnvFile
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

$trustedPref = [string]$env:AIWF_SQL_TRUSTED
$password = [string]$env:AIWF_SQL_PASSWORD
$useTrusted = (IsTrueLike $trustedPref)
if ($useTrusted) {
  $env:AIWF_SQL_AUTH_SUFFIX = ";integratedSecurity=true;authenticationScheme=NativeAuthentication"
  $env:AIWF_SQL_USER = ""
  $env:AIWF_SQL_PASSWORD = ""
  Info "base-java SQL auth mode: trusted (integrated security)"
} else {
  if ([string]::IsNullOrWhiteSpace($password) -or (IsPlaceholderPassword $password)) {
    throw "AIWF_SQL_PASSWORD is missing/placeholder. Set real SQL password or enable AIWF_SQL_TRUSTED=true explicitly."
  }
  $env:AIWF_SQL_AUTH_SUFFIX = ""
  Info "base-java SQL auth mode: sql_user"
}

Set-Location $ProjectDir
Info "Starting base-java ..."
mvn "spring-boot:run"
