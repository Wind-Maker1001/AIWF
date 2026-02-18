param(
  [Parameter(Mandatory = $true)][string]$InputDir,
  [Parameter(Mandatory = $true)][string]$OutputJsonl,
  [string]$ConfigJson = ".\rules\templates\preprocess_debate_evidence.json",
  [bool]$OcrEnabled = $true,
  [bool]$XlsxAllSheets = $true,
  [int]$MaxRetries = 1,
  [ValidateSet("skip", "raise")][string]$OnFileError = "skip"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ throw "[FAIL] $m" }

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Fail "python not found in PATH"
}
if (-not (Test-Path $InputDir)) {
  Fail "input directory not found: $InputDir"
}

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$glueDir = Join-Path $repoRoot "apps\glue-python"
if (-not (Test-Path $glueDir)) {
  Fail "glue-python dir not found: $glueDir"
}

$cfgAbs = $ConfigJson
if (-not [System.IO.Path]::IsPathRooted($cfgAbs)) {
  $candidate = Join-Path $repoRoot $cfgAbs
  if (Test-Path $candidate) {
    $cfgAbs = $candidate
  } else {
    $cfgAbs = (Resolve-Path $cfgAbs).Path
  }
}
if (-not (Test-Path $cfgAbs)) {
  Fail "config not found: $cfgAbs"
}

$inputAbs = (Resolve-Path $InputDir).Path
$outputAbs = $OutputJsonl
if (-not [System.IO.Path]::IsPathRooted($outputAbs)) {
  $outputAbs = Join-Path (Get-Location) $outputAbs
}

$files = Get-ChildItem -Path $inputAbs -Recurse -File -ErrorAction SilentlyContinue |
  Where-Object { $_.Extension.ToLower() -in @(".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".bmp", ".tif", ".tiff", ".xlsx", ".xlsm") } |
  Select-Object -ExpandProperty FullName
if (-not $files -or $files.Count -eq 0) {
  Fail "no supported evidence files found in $inputAbs"
}

Info ("found {0} files" -f $files.Count)

$jsonText = Get-Content -Path $cfgAbs -Raw -Encoding UTF8
$cfg = $jsonText | ConvertFrom-Json
if (-not $cfg.preprocess) {
  $cfg = [pscustomobject]@{ preprocess = [pscustomobject]@{} }
}
$cfg.preprocess | Add-Member -NotePropertyName input_files -NotePropertyValue $files -Force
$cfg.preprocess | Add-Member -NotePropertyName output_format -NotePropertyValue "jsonl" -Force
$cfg.preprocess | Add-Member -NotePropertyName ocr_enabled -NotePropertyValue $OcrEnabled -Force
$cfg.preprocess | Add-Member -NotePropertyName xlsx_all_sheets -NotePropertyValue $XlsxAllSheets -Force
$cfg.preprocess | Add-Member -NotePropertyName max_retries -NotePropertyValue $MaxRetries -Force
$cfg.preprocess | Add-Member -NotePropertyName on_file_error -NotePropertyValue $OnFileError -Force

$tmpCfg = Join-Path $env:TEMP ("aiwf_evidence_ingest_{0}.json" -f ([guid]::NewGuid().ToString("N")))
$cfg | ConvertTo-Json -Depth 20 | Set-Content -Path $tmpCfg -Encoding UTF8

try {
  Push-Location $glueDir
  try {
    # --input is required by CLI but ignored when input_files is provided in config.
    $seed = $files[0]
    python -m aiwf.preprocess --input $seed --output $outputAbs --config $tmpCfg
    if ($LASTEXITCODE -ne 0) {
      Fail "evidence ingest failed"
    }
  }
  finally {
    Pop-Location
  }

  Ok ("evidence ingest finished: {0}" -f $outputAbs)
}
finally {
  Remove-Item -Path $tmpCfg -ErrorAction SilentlyContinue
}
