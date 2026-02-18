param(
  [switch]$AutoFix
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Resolve-Tool([string[]]$Names, [string[]]$FallbackPaths) {
  foreach($n in $Names){
    $cmd = Get-Command $n -ErrorAction SilentlyContinue
    if ($cmd) { return $cmd.Source }
  }
  foreach($p in $FallbackPaths){
    if(Test-Path $p){ return $p }
  }
  return $null
}

function Ensure-Git {
  $git = Resolve-Tool -Names @("git") -FallbackPaths @("C:\Program Files\Git\cmd\git.exe")
  if ($git) {
    Ok "git available: $git"
    return
  }
  Warn "git not found"
  if (-not $AutoFix) { return }
  winget install -e --id Git.Git --source winget --accept-source-agreements --accept-package-agreements
  $git = Resolve-Tool -Names @("git") -FallbackPaths @("C:\Program Files\Git\cmd\git.exe")
  if (-not $git) { throw "failed to install git" }
  Ok "git installed: $git"
}

function Ensure-SqlCmd {
  $sqlcmd = Resolve-Tool -Names @("sqlcmd") -FallbackPaths @(
    "C:\Program Files\SqlCmd\sqlcmd.exe",
    "D:\SQL Server\Shared Function\Client SDK\ODBC\170\Tools\Binn\SQLCMD.EXE"
  )
  if ($sqlcmd) {
    Ok "sqlcmd available: $sqlcmd"
    return
  }
  Warn "sqlcmd not found"
}

function Ensure-Tesseract {
  $ts = Resolve-Tool -Names @("tesseract") -FallbackPaths @(
    "C:\Program Files\Tesseract-OCR\tesseract.exe",
    "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"
  )
  if ($ts) {
    Ok "tesseract available: $ts"
    return
  }
  Warn "tesseract not found"
  if (-not $AutoFix) { return }
  winget install -e --id UB-Mannheim.TesseractOCR --source winget --accept-source-agreements --accept-package-agreements
  $ts = Resolve-Tool -Names @("tesseract") -FallbackPaths @(
    "C:\Program Files\Tesseract-OCR\tesseract.exe",
    "C:\Program Files (x86)\Tesseract-OCR\tesseract.exe"
  )
  if (-not $ts) { throw "failed to install tesseract" }
  Ok "tesseract installed: $ts"
}

Ensure-Git
Ensure-SqlCmd
Ensure-Tesseract

