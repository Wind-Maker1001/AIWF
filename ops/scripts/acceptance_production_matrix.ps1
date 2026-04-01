param(
  [string]$ProjectRoot = "D:\AIWF",
  [string]$DesktopRoot = "E:\Desktop_Real\AIWF",
  [string]$InputRoot = "E:\Desktop_Real\Samples",
  [switch]$IncludeDirty
)

$ErrorActionPreference = "Stop"

. (Join-Path $PSScriptRoot "cleaning_shadow_acceptance_support.ps1")

function Invoke-Step([string]$Name, [scriptblock]$Action) {
  Write-Host "==> $Name"
  & $Action
}
$desktopApp = Join-Path $ProjectRoot "apps\dify-desktop"
if (-not (Test-Path $desktopApp)) {
  throw "desktop app not found: $desktopApp"
}

Push-Location $desktopApp
try {
  Invoke-Step "Unit tests" { npm run -s test:unit | Out-Host }
  Invoke-Step "Smoke test" { npm run -s smoke | Out-Host }
  Invoke-Step "Acceptance real samples" {
    Invoke-ShadowCleaningMode {
      $env:AIWF_ACCEPTANCE_INPUT_ROOT = $InputRoot
      $env:AIWF_ACCEPTANCE_OUTPUT_ROOT = $DesktopRoot
      try {
        npm run -s acceptance:real | Out-Host
      }
      finally {
        Remove-Item Env:AIWF_ACCEPTANCE_INPUT_ROOT -ErrorAction SilentlyContinue
        Remove-Item Env:AIWF_ACCEPTANCE_OUTPUT_ROOT -ErrorAction SilentlyContinue
      }
    }
  }
  if ($IncludeDirty) {
    Invoke-Step "Dirty regression" { npm run -s test:regression:dirty | Out-Host }
  }
  Invoke-Step "Office gate" { npm run -s test:office-gate | Out-Host }
}
finally {
  Pop-Location
}

Write-Host ""
Write-Host "Production acceptance matrix completed."
