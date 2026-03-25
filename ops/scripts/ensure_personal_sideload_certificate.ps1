param(
  [string]$Subject = "CN=AIWF Personal Sideload",
  [string]$StateDir = "",
  [int]$ValidDays = 365,
  [int]$RotateWhenExpiresInDays = 30,
  [switch]$RotateNow
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Get-ExistingPersonalCert([string]$ExpectedSubject) {
  Get-ChildItem Cert:\CurrentUser\My |
    Where-Object { $_.Subject -eq $ExpectedSubject -and $_.HasPrivateKey } |
    Sort-Object NotAfter -Descending |
    Select-Object -First 1
}

function New-RandomPassword() {
  -join ((48..57 + 65..90 + 97..122) | Get-Random -Count 24 | ForEach-Object { [char]$_ })
}

if (-not $StateDir) {
  $StateDir = Join-Path $env:LOCALAPPDATA "AIWF\personal_sideload_certificate"
}
New-Item -ItemType Directory -Path $StateDir -Force | Out-Null

$pfxPath = Join-Path $StateDir "aiwf_personal_sideload.pfx"
$cerPath = Join-Path $StateDir "aiwf_personal_sideload.cer"
$passwordPath = Join-Path $StateDir "aiwf_personal_sideload.password.txt"
$metadataPath = Join-Path $StateDir "aiwf_personal_sideload.metadata.json"

$existing = Get-ExistingPersonalCert $Subject
$now = Get-Date
$rotate = $RotateNow.IsPresent
$rotateReason = ""

if (-not $existing) {
  $rotate = $true
  $rotateReason = "missing"
} elseif ($existing.NotAfter -le $now) {
  $rotate = $true
  $rotateReason = "expired"
} elseif ($existing.NotAfter -le $now.AddDays($RotateWhenExpiresInDays)) {
  $rotate = $true
  $rotateReason = "expires_soon"
}

$password = if (Test-Path $passwordPath) {
  (Get-Content $passwordPath -Raw -Encoding UTF8).Trim()
} else {
  ""
}
if ([string]::IsNullOrWhiteSpace($password)) {
  $password = New-RandomPassword
  Set-Content -Path $passwordPath -Value $password -Encoding ASCII
}

$cert = $existing
if ($rotate) {
  Info ("creating/rotating personal sideload certificate: " + $rotateReason)
  $cert = New-SelfSignedCertificate `
    -Type CodeSigningCert `
    -Subject $Subject `
    -CertStoreLocation "Cert:\CurrentUser\My" `
    -KeyExportPolicy Exportable `
    -KeySpec Signature `
    -NotAfter $now.AddDays($ValidDays)
}

$securePassword = ConvertTo-SecureString $password -AsPlainText -Force
Export-PfxCertificate -Cert ("Cert:\CurrentUser\My\" + $cert.Thumbprint) -FilePath $pfxPath -Password $securePassword | Out-Null
Export-Certificate -Cert ("Cert:\CurrentUser\My\" + $cert.Thumbprint) -FilePath $cerPath | Out-Null

$daysRemaining = [int][math]::Floor(($cert.NotAfter - $now).TotalDays)
if ($daysRemaining -le $RotateWhenExpiresInDays) {
  Warn ("personal sideload certificate expires soon: " + $daysRemaining + " day(s) remaining")
}

$result = [ordered]@{
  subject = $cert.Subject
  thumbprint = $cert.Thumbprint
  not_before = $cert.NotBefore.ToString("s")
  not_after = $cert.NotAfter.ToString("s")
  days_remaining = $daysRemaining
  rotated = $rotate
  rotate_reason = $rotateReason
  state_dir = $StateDir
  pfx_path = $pfxPath
  cer_path = $cerPath
  password_path = $passwordPath
}
($result | ConvertTo-Json -Depth 4) | Set-Content $metadataPath -Encoding UTF8

Ok ("personal sideload certificate ready: " + $cert.Thumbprint)
Write-Output (($result | ConvertTo-Json -Compress))
