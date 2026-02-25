param(
  [string]$Version = "1.1.5",
  [string]$Channel = "stable",
  [int]$Rounds = 3,
  [int]$ReleaseAttemptsPerRound = 2,
  [switch]$CopyArtifactsToDesktop
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$releaseScript = Join-Path $PSScriptRoot "release_productize.ps1"
$buildScript = Join-Path $PSScriptRoot "run_dify_desktop.ps1"
$startupScript = Join-Path $PSScriptRoot "check_desktop_packaged_startup.ps1"
$acceptReal = Join-Path $PSScriptRoot "acceptance_desktop_real_sample.ps1"
$acceptFinance = Join-Path $PSScriptRoot "acceptance_desktop_finance_template.ps1"

$stableRoot = Join-Path $root ("release\stability_v{0}" -f $Version)
New-Item -ItemType Directory -Path $stableRoot -Force | Out-Null

Info "building desktop binaries once for v$Version"
powershell -ExecutionPolicy Bypass -File $buildScript -BuildWin -BuildInstaller
if ($LASTEXITCODE -ne 0) { throw "desktop build failed" }

$roundsOut = @()
for ($i = 1; $i -le [Math]::Max(1, $Rounds); $i += 1) {
  $roundTag = ("round_{0:00}" -f $i)
  $roundRoot = Join-Path $stableRoot $roundTag
  New-Item -ItemType Directory -Path $roundRoot -Force | Out-Null

  $item = [ordered]@{
    round = $i
    started_at = (Get-Date).ToString("o")
    release_productize = "failed"
    startup_check = "failed"
    acceptance_real = "failed"
    acceptance_finance = "failed"
    finished_at = ""
    error = ""
  }

  try {
    $releaseOk = $false
    $releaseErr = ""
    $maxAttempts = [Math]::Max(1, $ReleaseAttemptsPerRound)
    for ($attempt = 1; $attempt -le $maxAttempts; $attempt += 1) {
      Info "[$roundTag] running release_productize (full gates), attempt $attempt/$maxAttempts"
      try {
        powershell -ExecutionPolicy Bypass -File $releaseScript -Version $Version -Channel $Channel
        if ($LASTEXITCODE -ne 0) { throw "release_productize failed with exit code $LASTEXITCODE" }
        $releaseOk = $true
        break
      } catch {
        $releaseErr = [string]$_
        Write-Host "[WARN] [$roundTag] release attempt $attempt failed: $releaseErr" -ForegroundColor Yellow
      }
    }
    if (-not $releaseOk) { throw $releaseErr }
    $item.release_productize = "passed"

    Info "[$roundTag] running packaged startup check"
    powershell -ExecutionPolicy Bypass -File $startupScript
    if ($LASTEXITCODE -ne 0) { throw "packaged startup check failed" }
    $item.startup_check = "passed"

    Info "[$roundTag] running real-sample acceptance"
    $argsReal = @(
      "-ExecutionPolicy", "Bypass",
      "-File", $acceptReal,
      "-OutputRoot", (Join-Path $roundRoot "acceptance_real")
    )
    if ($CopyArtifactsToDesktop) { $argsReal += "-CopyArtifactsToDesktop" }
    powershell @argsReal
    if ($LASTEXITCODE -ne 0) { throw "acceptance real failed" }
    $item.acceptance_real = "passed"

    Info "[$roundTag] running finance-template acceptance"
    $argsFin = @(
      "-ExecutionPolicy", "Bypass",
      "-File", $acceptFinance,
      "-OutputRoot", (Join-Path $roundRoot "acceptance_finance")
    )
    if ($CopyArtifactsToDesktop) { $argsFin += "-CopyArtifactsToDesktop" }
    powershell @argsFin
    if ($LASTEXITCODE -ne 0) { throw "acceptance finance failed" }
    $item.acceptance_finance = "passed"

    Ok "[$roundTag] all checks passed"
  }
  catch {
    $item.error = [string]$_
    Write-Host "[ERR ] [$roundTag] $($item.error)" -ForegroundColor Red
  }
  finally {
    $item.finished_at = (Get-Date).ToString("o")
    $roundsOut += [pscustomobject]$item
  }
}

$allPass = $true
foreach ($r in $roundsOut) {
  if ($r.release_productize -ne "passed" -or $r.startup_check -ne "passed" -or $r.acceptance_real -ne "passed" -or $r.acceptance_finance -ne "passed") {
    $allPass = $false
    break
  }
}

$summary = [ordered]@{
  version = $Version
  channel = $Channel
  rounds = $Rounds
  release_attempts_per_round = $ReleaseAttemptsPerRound
  all_passed = $allPass
  generated_at = (Get-Date).ToString("o")
  items = $roundsOut
}

$jsonPath = Join-Path $stableRoot "stability_summary.json"
($summary | ConvertTo-Json -Depth 6) | Set-Content -Path $jsonPath -Encoding UTF8

$mdPath = Join-Path $stableRoot "stability_summary.md"
$lines = @()
$lines += "# AIWF v$Version Stability Seal Report"
$lines += ""
$lines += "- generated_at: $((Get-Date).ToString('o'))"
$lines += "- rounds: $Rounds"
$lines += "- release_attempts_per_round: $ReleaseAttemptsPerRound"
$lines += "- all_passed: $allPass"
$lines += ""
$lines += "| Round | release_productize | startup_check | acceptance_real | acceptance_finance |"
$lines += "|---|---|---|---|---|"
foreach ($r in $roundsOut) {
  $lines += "| $($r.round) | $($r.release_productize) | $($r.startup_check) | $($r.acceptance_real) | $($r.acceptance_finance) |"
}
Set-Content -Path $mdPath -Value ($lines -join [Environment]::NewLine) -Encoding UTF8

Ok "stability summary json: $jsonPath"
Ok "stability summary md: $mdPath"
if (-not $allPass) { exit 2 }
exit 0
