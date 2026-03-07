param(
  [string]$Branch = "",
  [string]$Ref = "",
  [string]$Owner = "ci",
  [string]$EnvFile = "",
  [ValidateSet("Default","Quick","Full")]
  [string]$CiProfile = "Full",
  [bool]$RunFullIntegration = $true,
  [bool]$WaitForQuick = $true,
  [bool]$RunFullIfMissing = $true,
  [bool]$WaitForFull = $true,
  [int]$PollSeconds = 20,
  [int]$QuickTimeoutMinutes = 30,
  [int]$FullTimeoutMinutes = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$statusScript = Join-Path $PSScriptRoot "get_ci_status.ps1"
$dispatchScript = Join-Path $PSScriptRoot "dispatch_full_integration_self_hosted.ps1"
if (-not (Test-Path $statusScript)) {
  throw "status script not found: $statusScript"
}
if (-not (Test-Path $dispatchScript)) {
  throw "dispatch script not found: $dispatchScript"
}

function Get-CiStatusSnapshot {
  & $statusScript -Branch $Branch -Ref $Ref -Quiet
}

function Wait-QuickCiSuccess {
  $deadline = (Get-Date).AddMinutes($QuickTimeoutMinutes)
  while ((Get-Date) -lt $deadline) {
    $status = Get-CiStatusSnapshot
    if ($null -eq $status.QuickCi) {
      Warn "Quick CI run for current head not found yet; waiting..."
      Start-Sleep -Seconds $PollSeconds
      continue
    }

    if ($status.QuickCi.Status -ne "completed") {
      Info ("Quick CI {0} still running" -f $status.QuickCi.Id)
      Start-Sleep -Seconds $PollSeconds
      continue
    }

    if ($status.QuickCi.Conclusion -ne "success") {
      throw ("Quick CI failed for current head: {0}" -f $status.QuickCi.Url)
    }

    Ok ("Quick CI passed: {0}" -f $status.QuickCi.Url)
    return $status
  }

  throw "timed out waiting for Quick CI to finish"
}

function Wait-FullCiSuccess {
  $deadline = (Get-Date).AddMinutes($FullTimeoutMinutes)
  while ((Get-Date) -lt $deadline) {
    $status = Get-CiStatusSnapshot
    if ($null -eq $status.ManualFullForHead) {
      Warn "Full Integration run for current head not found yet; waiting..."
      Start-Sleep -Seconds $PollSeconds
      continue
    }

    if ($status.ManualFullForHead.Status -ne "completed") {
      Info ("Full Integration {0} still running" -f $status.ManualFullForHead.Id)
      Start-Sleep -Seconds $PollSeconds
      continue
    }

    if ($status.ManualFullForHead.Conclusion -ne "success") {
      throw ("Full Integration failed for current head: {0}" -f $status.ManualFullForHead.Url)
    }

    Ok ("Full Integration passed: {0}" -f $status.ManualFullForHead.Url)
    return $status
  }

  throw "timed out waiting for Full Integration to finish"
}

$status = Get-CiStatusSnapshot
Info ("verifying CI for {0}@{1}" -f $status.Branch, $status.HeadSha)

if ($null -eq $status.QuickCi) {
  if (-not $WaitForQuick) {
    throw "Quick CI has not started for the current head"
  }
  $status = Wait-QuickCiSuccess
} elseif ($status.QuickCi.Status -ne "completed") {
  if (-not $WaitForQuick) {
    throw ("Quick CI still running: {0}" -f $status.QuickCi.Url)
  }
  $status = Wait-QuickCiSuccess
} elseif ($status.QuickCi.Conclusion -ne "success") {
  throw ("Quick CI failed for current head: {0}" -f $status.QuickCi.Url)
} else {
  Ok ("Quick CI already green: {0}" -f $status.QuickCi.Url)
}

if ($null -eq $status.ManualFullForHead) {
  if (-not $RunFullIfMissing) {
    throw "Full Integration has not run for the current head"
  }
  Info "dispatching Full Integration for current head"
  $dispatchRef = if ([string]::IsNullOrWhiteSpace($Ref)) { $status.Branch } else { $Ref.Trim() }
  $dispatchResult = & $dispatchScript `
    -Ref $dispatchRef `
    -Owner $Owner `
    -EnvFile $EnvFile `
    -CiProfile $CiProfile `
    -RunFullIntegration:$RunFullIntegration `
    -Wait:$WaitForFull `
    -PollSeconds $PollSeconds `
    -RunTimeoutMinutes $FullTimeoutMinutes
  if ($WaitForFull) {
    $status = Get-CiStatusSnapshot
  } else {
    $status = Get-CiStatusSnapshot
  }
} elseif ($status.ManualFullForHead.Status -ne "completed") {
  if (-not $WaitForFull) {
    throw ("Full Integration still running: {0}" -f $status.ManualFullForHead.Url)
  }
  $status = Wait-FullCiSuccess
} elseif ($status.ManualFullForHead.Conclusion -ne "success") {
  throw ("Full Integration failed for current head: {0}" -f $status.ManualFullForHead.Url)
} else {
  Ok ("Full Integration already green: {0}" -f $status.ManualFullForHead.Url)
}

if ($WaitForFull -and $null -ne $status.ManualFullForHead -and $status.ManualFullForHead.Conclusion -ne "success") {
  throw ("Full Integration did not finish successfully: {0}" -f $status.ManualFullForHead.Url)
}

if (-not [string]::IsNullOrWhiteSpace($status.ScheduledFullNote)) {
  Warn $status.ScheduledFullNote
}

Ok "branch CI verification passed"
[pscustomobject]@{
  Repo = $status.Repo
  Branch = $status.Branch
  HeadSha = $status.HeadSha
  QuickCi = $status.QuickCi
  ManualFullForHead = $status.ManualFullForHead
  ScheduledFullNote = $status.ScheduledFullNote
}
