function Invoke-GovernanceCapabilityExportStep {
  param(
    [Parameter(Mandatory = $true)][string]$ScriptPath,
    [string]$FailureScope = "operation"
  )

  $checkedAt = (Get-Date).ToString("s")
  if (-not (Test-Path $ScriptPath)) {
    return [ordered]@{
      ok = $false
      checked_at = $checkedAt
      error = "governance capability export script missing: $ScriptPath"
      failure_message = "$FailureScope blocked by governance capability export step"
    }
  }

  Info "refreshing governance capability generated assets"
  powershell -ExecutionPolicy Bypass -File $ScriptPath
  $checkedAt = (Get-Date).ToString("s")
  if ($LASTEXITCODE -ne 0) {
    return [ordered]@{
      ok = $false
      checked_at = $checkedAt
      error = "export_governance_capabilities.ps1 exit code $LASTEXITCODE"
      failure_message = "$FailureScope blocked by governance capability export step"
    }
  }

  Ok "governance capability generated assets refreshed"
  return [ordered]@{
    ok = $true
    checked_at = $checkedAt
    error = ""
    failure_message = ""
  }
}
