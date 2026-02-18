param(
  [string]$GlueDir = "",
  [string]$DesktopToolsDir = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $GlueDir) {
  $root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
  $GlueDir = Join-Path $root "apps\glue-python"
  if (-not $DesktopToolsDir) {
    $DesktopToolsDir = Join-Path $root "apps\dify-desktop\tools"
  }
}

if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}

$pyCode = @'
import importlib.util
import json

mods = {
  "pypdf":"PDF ingest",
  "PIL":"Image ingest",
  "pytesseract":"Image OCR",
  "openpyxl":"XLSX ingest",
  "docx":"DOCX ingest",
  "pptx":"PPTX export"
}
out = {}
for m, desc in mods.items():
  out[m] = {
    "available": importlib.util.find_spec(m) is not None,
    "purpose": desc
  }
print(json.dumps(out, ensure_ascii=False))
'@

Push-Location $GlueDir
try {
  $raw = $pyCode | python -
  $obj = $raw | ConvertFrom-Json
  foreach($k in $obj.PSObject.Properties.Name){
    $v = $obj.$k
    if($v.available){
      Ok "$k available ($($v.purpose))"
    } else {
      Warn "$k missing ($($v.purpose))"
    }
  }
}
finally {
  Pop-Location
}

$tesseract = Get-Command tesseract -ErrorAction SilentlyContinue
if (-not $tesseract) {
  $fallbacks = @(
    "C:\Program Files\Tesseract-OCR\tesseract.exe",
    "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"
  )
  foreach($p in $fallbacks){
    if(Test-Path $p){
      $tesseract = $p
      break
    }
  }
}
if (-not $tesseract -and $DesktopToolsDir) {
  $bundled = @(
    (Join-Path $DesktopToolsDir "tesseract\tesseract.exe"),
    (Join-Path $DesktopToolsDir "tesseract.exe")
  )
  foreach($p in $bundled){
    if(Test-Path $p){
      $tesseract = $p
      break
    }
  }
}

if ($tesseract) {
  Ok "tesseract binary available"
} else {
  Warn "tesseract binary missing (required for OCR)"
}

$fontFiles = @()
$windirRoot = if ($env:WINDIR) { $env:WINDIR } else { "C:\Windows" }
$winFonts = Join-Path $windirRoot "Fonts"
if (Test-Path $winFonts) {
  try { $fontFiles += Get-ChildItem $winFonts -File | ForEach-Object { $_.Name.ToLowerInvariant() } } catch {}
}
if ($DesktopToolsDir) {
  $bundledFontsDir = Join-Path $DesktopToolsDir "fonts"
  if (Test-Path $bundledFontsDir) {
    try { $fontFiles += Get-ChildItem $bundledFontsDir -File | ForEach-Object { $_.Name.ToLowerInvariant() } } catch {}
  }
}
$fontFiles = $fontFiles | Select-Object -Unique
$coreZh = @("msyh.ttc","msyh.ttf","msyhbd.ttc","msyhbd.ttf","simsun.ttc","simsun.ttf","simhei.ttf","notosanscjk-regular.ttc","notosanscjk-sc-regular.otf","notosanscjksc-regular.otf")
$hasCoreZh = $false
foreach($f in $coreZh){
  if($fontFiles -contains $f){
    $hasCoreZh = $true
    break
  }
}
if($hasCoreZh){
  Ok "core Chinese font available (system or bundled)"
} else {
  Warn "core Chinese font missing; DOCX/PPTX Chinese layout may degrade"
}
