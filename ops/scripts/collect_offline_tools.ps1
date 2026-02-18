param(
  [string]$DesktopDir = "",
  [switch]$CleanFirst,
  [bool]$AutoDownloadLangPacks = $true
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
$toolsDir = Join-Path $DesktopDir "tools"
$tesseractDir = Join-Path $toolsDir "tesseract"
$tessdataDir = Join-Path $tesseractDir "tessdata"
$popplerDir = Join-Path $toolsDir "poppler\bin"
$fontsDir = Join-Path $toolsDir "fonts"

if ($CleanFirst -and (Test-Path $toolsDir)) {
  Remove-Item $toolsDir -Recurse -Force
}
New-Item -ItemType Directory -Force -Path $tesseractDir | Out-Null
New-Item -ItemType Directory -Force -Path $tessdataDir | Out-Null
New-Item -ItemType Directory -Force -Path $popplerDir | Out-Null
New-Item -ItemType Directory -Force -Path $fontsDir | Out-Null

function Copy-IfExists([string]$src, [string]$dst) {
  if (Test-Path $src) {
    Copy-Item $src $dst -Force
    return $true
  }
  return $false
}

function Try-DownloadFile([string]$url, [string]$dst) {
  try {
    Invoke-WebRequest -Uri $url -OutFile $dst -UseBasicParsing -TimeoutSec 40
    if (Test-Path $dst) { return $true }
  } catch {}
  return $false
}

$copied = [ordered]@{
  tesseract_exe = $false
  tessdata_chi_sim = $false
  tessdata_eng = $false
  pdftoppm_exe = $false
  poppler_dll_count = 0
  zh_font_core = $false
}

$tesseractCandidates = @(
  "$env:ProgramFiles\Tesseract-OCR\tesseract.exe",
  "$env:ProgramFiles(x86)\Tesseract-OCR\tesseract.exe"
)
foreach ($c in $tesseractCandidates) {
  if (Copy-IfExists $c (Join-Path $tesseractDir "tesseract.exe")) {
    $copied.tesseract_exe = $true
    break
  }
}

$tessdataCandidates = @(
  "$env:TESSDATA_PREFIX",
  "$env:ProgramFiles\Tesseract-OCR\tessdata",
  "$env:ProgramFiles(x86)\Tesseract-OCR\tessdata"
) | Where-Object { $_ -and $_.Trim() }

foreach ($d in $tessdataCandidates) {
  if (-not (Test-Path $d)) { continue }
  if (-not $copied.tessdata_chi_sim) {
    $copied.tessdata_chi_sim = Copy-IfExists (Join-Path $d "chi_sim.traineddata") (Join-Path $tessdataDir "chi_sim.traineddata")
  }
  if (-not $copied.tessdata_eng) {
    $copied.tessdata_eng = Copy-IfExists (Join-Path $d "eng.traineddata") (Join-Path $tessdataDir "eng.traineddata")
  }
  if ($copied.tessdata_chi_sim -and $copied.tessdata_eng) { break }
}

if ($AutoDownloadLangPacks) {
  if (-not $copied.tessdata_eng) {
    $copied.tessdata_eng = Try-DownloadFile "https://github.com/tesseract-ocr/tessdata_fast/raw/main/eng.traineddata" (Join-Path $tessdataDir "eng.traineddata")
  }
  if (-not $copied.tessdata_chi_sim) {
    $copied.tessdata_chi_sim = Try-DownloadFile "https://github.com/tesseract-ocr/tessdata_fast/raw/main/chi_sim.traineddata" (Join-Path $tessdataDir "chi_sim.traineddata")
  }
}

$pdftoppmCandidates = @(
  "$env:ProgramFiles\poppler\Library\bin\pdftoppm.exe",
  "$env:ProgramFiles\poppler\bin\pdftoppm.exe"
)
$localAppData = if ($env:LOCALAPPDATA -and $env:LOCALAPPDATA.Trim()) { $env:LOCALAPPDATA } else { Join-Path $HOME "AppData\\Local" }
$wingetPoppler = Join-Path $localAppData "Microsoft\\WinGet\\Packages"
if (Test-Path $wingetPoppler) {
  Get-ChildItem $wingetPoppler -Directory | Where-Object { $_.Name.ToLower().Contains("poppler") } | ForEach-Object {
    $pdftoppmCandidates += (Join-Path $_.FullName "poppler-25.07.0\Library\bin\pdftoppm.exe")
    $pdftoppmCandidates += (Join-Path $_.FullName "poppler-24.08.0\Library\bin\pdftoppm.exe")
  }
}

$pdftoppmSrc = ""
foreach ($c in $pdftoppmCandidates) {
  if (Test-Path $c) {
    Copy-Item $c (Join-Path $popplerDir "pdftoppm.exe") -Force
    $copied.pdftoppm_exe = $true
    $pdftoppmSrc = $c
    break
  }
}

if ($copied.pdftoppm_exe -and $pdftoppmSrc) {
  $srcBin = Split-Path -Parent $pdftoppmSrc
  $dlls = Get-ChildItem $srcBin -File -Filter "*.dll" -ErrorAction SilentlyContinue
  foreach ($dll in $dlls) {
    Copy-Item $dll.FullName (Join-Path $popplerDir $dll.Name) -Force
    $copied.poppler_dll_count += 1
  }
}

$fontCandidates = @(
  "$env:WINDIR\Fonts\msyh.ttc",
  "$env:WINDIR\Fonts\msyhbd.ttc",
  "$env:WINDIR\Fonts\simhei.ttf",
  "$env:WINDIR\Fonts\simsun.ttc"
)
foreach($f in $fontCandidates){
  if (Test-Path $f) {
    $name = [System.IO.Path]::GetFileName($f)
    Copy-Item $f (Join-Path $fontsDir $name) -Force
    $copied.zh_font_core = $true
  }
}
if (-not $copied.zh_font_core -and $AutoDownloadLangPacks) {
  $notoUrl = "https://github.com/notofonts/noto-cjk/raw/main/Sans/OTF/SimplifiedChinese/NotoSansCJKsc-Regular.otf"
  $notoDst = Join-Path $fontsDir "NotoSansCJKsc-Regular.otf"
  if (Try-DownloadFile $notoUrl $notoDst) {
    $copied.zh_font_core = $true
  }
}

@"
# Bundled Runtime Tools

This folder can contain optional offline runtime binaries.

Collected on: $(Get-Date -Format s)

Collected status:
- tesseract.exe: $($copied.tesseract_exe)
- chi_sim.traineddata: $($copied.tessdata_chi_sim)
- eng.traineddata: $($copied.tessdata_eng)
- pdftoppm.exe: $($copied.pdftoppm_exe)
- poppler dll count: $($copied.poppler_dll_count)
- zh core font bundled: $($copied.zh_font_core)

Expected layout:
- tools/tesseract/tesseract.exe
- tools/tesseract/tessdata/chi_sim.traineddata
- tools/tesseract/tessdata/eng.traineddata
- tools/poppler/bin/pdftoppm.exe
- tools/poppler/bin/*.dll
- tools/fonts/*.ttf|*.ttc|*.otf
"@ | Set-Content -Encoding UTF8 (Join-Path $toolsDir "README.md")

if (-not $copied.tesseract_exe) { Warn "tesseract.exe not found on this machine" }
if (-not $copied.pdftoppm_exe) { Warn "pdftoppm.exe not found on this machine" }
if (-not $copied.tessdata_chi_sim) { Warn "chi_sim.traineddata not found (Chinese OCR quality will degrade)" }
if (-not $copied.zh_font_core) { Warn "core Chinese font not bundled (DOCX/PPTX zh rendering may degrade)" }
Ok "offline tools collection complete: $toolsDir"
