param(
  [string]$EnvFile = "",
  [string]$ProjectDir = "",
  [switch]$CreateVenv,
  [switch]$RequireEnhancedIngest
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

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

function Check-OptionalDeps([string]$PythonCmd) {
  $script = @'
import importlib
import importlib.util

modules = [
    ("pandera", "quality contract"),
    ("rapidfuzz", "fuzzy matching"),
    ("dateparser", "date normalization"),
    ("phonenumbers", "phone normalization"),
    ("python_calamine", "xlsx fast reader"),
    ("paddleocr", "image OCR"),
    ("docling", "document extraction"),
]

for name, label in modules:
    if importlib.util.find_spec(name) is None:
        print(f"{name}|{label}|missing|module not installed")
        continue
    try:
        mod = importlib.import_module(name)
        version = getattr(mod, "__version__", "") or ""
        print(f"{name}|{label}|ok|{version}")
    except Exception as exc:
        print(f"{name}|{label}|missing|{exc}")
'@
  $raw = $script | & $PythonCmd -
  $statusMap = @{}
  foreach ($line in ($raw -split "`r?`n")) {
    $text = $line.Trim()
    if (-not $text) { continue }
    $parts = $text.Split("|")
    if ($parts.Length -lt 3) { continue }
    $name = $parts[0]
    $label = $parts[1]
    $status = $parts[2]
    $detail = if ($parts.Length -ge 4) { $parts[3] } else { "" }
    $statusMap[$name] = $status
    if ($status -eq "ok") {
      Ok "optional dependency ready: $name ($label) $detail"
    } else {
      Warn "optional dependency missing: $name ($label) $detail"
    }
  }
  return $statusMap
}

function Enforce-EnhancedIngestDeps([hashtable]$StatusMap) {
  if (-not $RequireEnhancedIngest) { return }
  $required = @("pandera", "python_calamine", "paddleocr", "docling")
  $missing = @()
  foreach ($name in $required) {
    if (-not $StatusMap.ContainsKey($name) -or $StatusMap[$name] -ne "ok") {
      $missing += $name
    }
  }
  if ($missing.Count -gt 0) {
    throw "enhanced ingest dependencies missing: $($missing -join ', ')"
  }
}

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
  $optionalDeps = Check-OptionalDeps $venvPython
  Enforce-EnhancedIngestDeps $optionalDeps

  $port = if ($env:AIWF_GLUE_PY_PORT) { $env:AIWF_GLUE_PY_PORT } else { "18081" }
  Info "Starting glue-python with .venv on port $port"
  & $venvPython -m uvicorn app:app --host 0.0.0.0 --port $port
  exit $LASTEXITCODE
}

$port = if ($env:AIWF_GLUE_PY_PORT) { $env:AIWF_GLUE_PY_PORT } else { "18081" }
$optionalDeps = Check-OptionalDeps "python"
Enforce-EnhancedIngestDeps $optionalDeps
Info "Starting glue-python on port $port"
python -m uvicorn app:app --host 0.0.0.0 --port $port
