param(
  [string]$StateDir = "",
  [int]$WarnWhenExpiresInDays = 30,
  [int]$FailWhenExpiresInDays = 0,
  [switch]$FailIfMissing,
  [switch]$FailIfExpired,
  [switch]$FailIfExpiringSoon
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $StateDir) {
  $StateDir = Join-Path $env:LOCALAPPDATA "AIWF\personal_sideload_certificate"
}

$metadataPath = Join-Path $StateDir "aiwf_personal_sideload.metadata.json"
if (-not (Test-Path $metadataPath)) {
  if ($FailIfMissing) { throw "personal sideload certificate metadata not found: $metadataPath" }
  Warn ("personal sideload certificate metadata not found: " + $metadataPath)
  exit 0
}

$meta = Get-Content $metadataPath -Raw -Encoding UTF8 | ConvertFrom-Json
$notAfter = [datetime]$meta.not_after
$daysRemaining = [int][math]::Floor(($notAfter - (Get-Date)).TotalDays)

if ($daysRemaining -lt 0) {
  if ($FailIfExpired) { throw "personal sideload certificate is expired" }
  Warn "personal sideload certificate is expired"
} elseif ($FailWhenExpiresInDays -gt 0 -and $daysRemaining -le $FailWhenExpiresInDays) {
  throw ("personal sideload certificate expires within release block window: " + $daysRemaining + " day(s)")
} elseif ($daysRemaining -le $WarnWhenExpiresInDays) {
  if ($FailIfExpiringSoon) { throw "personal sideload certificate expires within warning window" }
  Warn ("personal sideload certificate expires soon: " + $daysRemaining + " day(s)")
}

Ok ("personal sideload certificate check passed; days_remaining=" + $daysRemaining)
Write-Output (([ordered]@{
  thumbprint = [string]$meta.thumbprint
  subject = [string]$meta.subject
  days_remaining = $daysRemaining
  warn_when_expires_in_days = $WarnWhenExpiresInDays
  fail_when_expires_in_days = $FailWhenExpiresInDays
  metadata_path = $metadataPath
} | ConvertTo-Json -Compress))
