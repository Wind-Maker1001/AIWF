param(
  [string]$EnvFile = "",
  [string]$ProjectDir = "",
  [switch]$CreateVenv
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $EnvFile) {
  $EnvFile = Join-Path $root "ops\config\dev.env"
}
if (-not $ProjectDir) {
  $ProjectDir = Join-Path $root "apps\glue-python"
}

function Import-DotEnv([string]$Path) {
  if (-not (Test-Path $Path)) {
    Warn "env file not found: $Path"
    return
  }

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

if (-not (Test-Path $ProjectDir)) {
  throw "glue-python not found: $ProjectDir"
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}

Import-DotEnv $EnvFile
$validateScript = Join-Path $root "ops\scripts\validate_env.ps1"
if (Test-Path $validateScript) {
  powershell -ExecutionPolicy Bypass -File $validateScript -EnvFile $EnvFile
}
Set-Location $ProjectDir

if ($CreateVenv) {
  if (-not (Test-Path ".venv")) {
    Info "Creating .venv"
    python -m venv .venv
  }

  $venvPython = Join-Path $ProjectDir ".venv\Scripts\python.exe"
  & $venvPython -m pip install -U pip
  $lockFile = Join-Path $ProjectDir "requirements-lock.txt"
  if (Test-Path $lockFile) {
    & $venvPython -m pip install -r $lockFile
  } else {
    & $venvPython -m pip install -r requirements.txt
  }

  $port = if ($env:AIWF_GLUE_PY_PORT) { $env:AIWF_GLUE_PY_PORT } else { "18081" }
  Info "Starting glue-python with .venv on port $port"
  & $venvPython -m uvicorn app:app --host 0.0.0.0 --port $port
  exit $LASTEXITCODE
}

$port = if ($env:AIWF_GLUE_PY_PORT) { $env:AIWF_GLUE_PY_PORT } else { "18081" }
Info "Starting glue-python on port $port"
python -m uvicorn app:app --host 0.0.0.0 --port $port
