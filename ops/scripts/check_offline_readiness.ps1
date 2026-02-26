param(
  [string]$InstallRoot = "D:\Apps\AIWF\AIWF Dify Desktop"
)

$ErrorActionPreference = "Stop"

function Check-Item([string]$Path, [string]$Name) {
  if (Test-Path $Path) {
    Write-Host "[OK] $Name -> $Path"
    return $true
  }
  Write-Host "[MISSING] $Name -> $Path"
  return $false
}

$allOk = $true
$allOk = (Check-Item (Join-Path $InstallRoot "AIWF Dify Desktop.exe") "Desktop EXE") -and $allOk
$allOk = (Check-Item (Join-Path $InstallRoot "resources\tools\poppler\bin\pdftoppm.exe") "Poppler") -and $allOk
$allOk = (Check-Item (Join-Path $InstallRoot "resources\tools\tesseract\tesseract.exe") "Tesseract") -and $allOk
$allOk = (Check-Item (Join-Path $InstallRoot "resources\tools\tesseract\tessdata\chi_sim.traineddata") "Tesseract chi_sim data") -and $allOk
$allOk = (Check-Item (Join-Path $InstallRoot "resources\tools\tesseract\tessdata\eng.traineddata") "Tesseract eng data") -and $allOk

if ($allOk) {
  Write-Host ""
  Write-Host "Offline readiness check passed."
  exit 0
}

Write-Host ""
Write-Host "Offline readiness check failed."
exit 2
