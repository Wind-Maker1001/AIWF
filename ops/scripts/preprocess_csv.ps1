param(
  [Parameter(Mandatory = $true)][string]$InputCsv,
  [Parameter(Mandatory = $true)][string]$OutputCsv,
  [string]$ConfigJson = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ throw "[FAIL] $m" }

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Fail "python not found in PATH"
}
if (-not (Test-Path $InputCsv)) {
  Fail "input csv not found: $InputCsv"
}

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$glueDir = Join-Path $repoRoot "apps\glue-python"
if (-not (Test-Path $glueDir)) {
  Fail "glue-python dir not found: $glueDir"
}

$inputAbs = (Resolve-Path $InputCsv).Path
$outputAbs = $OutputCsv
if (-not [System.IO.Path]::IsPathRooted($outputAbs)) {
  $outputAbs = Join-Path (Get-Location) $outputAbs
}
$configAbs = $ConfigJson
if ($configAbs -and $configAbs.Trim().Length -gt 0) {
  if (-not [System.IO.Path]::IsPathRooted($configAbs)) {
    $candidateRepo = Join-Path $repoRoot $configAbs
    if (Test-Path $candidateRepo) {
      $configAbs = $candidateRepo
    } else {
      $configAbs = (Resolve-Path $configAbs).Path
    }
  }
}

Push-Location $glueDir
try {
  Info "preprocessing csv"
  if ($configAbs -and $configAbs.Trim().Length -gt 0) {
    python -m aiwf.preprocess --input $inputAbs --output $outputAbs --config $configAbs
  } else {
    python -m aiwf.preprocess --input $inputAbs --output $outputAbs
  }
  if ($LASTEXITCODE -ne 0) {
    Fail "preprocess failed"
  }
  Ok "preprocess finished"
}
finally {
  Pop-Location
}
