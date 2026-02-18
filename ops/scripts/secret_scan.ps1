param(
  [string]$Root = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ Write-Host "[FAIL] $m" -ForegroundColor Red }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$gitCandidates = @(
  "git",
  "C:\\Program Files\\Git\\cmd\\git.exe",
  "C:\\Program Files\\Git\\bin\\git.exe"
)
$git = $null
foreach ($g in $gitCandidates) {
  try {
    if ($g -eq "git") {
      if (Get-Command git -ErrorAction SilentlyContinue) { $git = "git"; break }
    } elseif (Test-Path $g) {
      $git = $g; break
    }
  } catch {}
}
if (-not $git) {
  throw "git not found; cannot run secret scan"
}

Push-Location $Root
try {
  $files = & $git ls-files
} finally {
  Pop-Location
}

$deny = @(
  @{ name = "private_key"; re = "-----BEGIN (RSA|OPENSSH|EC|DSA|PRIVATE) PRIVATE KEY-----" },
  @{ name = "openai_sk"; re = "\bsk-[A-Za-z0-9]{20,}\b" },
  @{ name = "aws_akia"; re = "\bAKIA[0-9A-Z]{16}\b" }
)

$issues = New-Object System.Collections.Generic.List[object]

foreach ($rel in $files) {
  $path = Join-Path $Root $rel
  if (-not (Test-Path $path)) { continue }
  if ($rel -match "^docs/" -or $rel -match "^docs\\") { continue }
  if ($rel -match "^ops/logs/" -or $rel -match "^ops\\logs\\") { continue }
  if ($rel -match "\.png$|\.jpg$|\.jpeg$|\.gif$|\.ico$|\.pdf$|\.pptx$|\.docx$|\.xlsx$|\.zip$|\.jar$|\.exe$|\.dll$|\.so$|\.dylib$|\.ttf$|\.otf$|\.woff2?$|\.parquet$|\.db$|\.bin$|\.lock$|\.asar$") { continue }

  $txt = $null
  try {
    $txt = Get-Content -Raw -LiteralPath $path -ErrorAction Stop
  } catch {
    continue
  }
  if ($null -eq $txt) { continue }

  foreach ($rule in $deny) {
    if ([regex]::IsMatch($txt, $rule.re, [System.Text.RegularExpressions.RegexOptions]::IgnoreCase)) {
      $issues.Add([pscustomobject]@{ file = $rel; rule = $rule.name; detail = "high-confidence secret pattern" })
    }
  }

  if ($rel -match "^ops/config/dev\.env$|^ops\\config\\dev\.env$") {
    $line = ($txt -split "`r?`n") | Where-Object { $_ -match "^AIWF_SQL_PASSWORD=" } | Select-Object -First 1
    if ($line) {
      $val = ($line -replace "^AIWF_SQL_PASSWORD=", "").Trim()
      $allowed = @("", "__SET_LOCAL_SQL_PASSWORD__", "<YOUR_SA_PASSWORD>", "<APP_PASSWORD>")
      if ($allowed -notcontains $val) {
        $issues.Add([pscustomobject]@{ file = $rel; rule = "dev_env_sql_password"; detail = "AIWF_SQL_PASSWORD must be placeholder" })
      }
    }
  }
}

if ($issues.Count -gt 0) {
  Fail "secret scan failed"
  $issues | ForEach-Object { Write-Host (" - " + $_.file + " [" + $_.rule + "]: " + $_.detail) -ForegroundColor Red }
  exit 1
}

Ok "secret scan passed"
exit 0
