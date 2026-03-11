param(
  [string]$LockFile = "",
  [string]$AdvisoryDb = "",
  [string]$AdvisoryUrl = "https://github.com/RustSec/advisory-db.git",
  [switch]$SkipFetch,
  [switch]$DenyWarnings,
  [switch]$Json
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $LockFile) {
  $LockFile = Join-Path $root "apps\accel-rust\Cargo.lock"
}
if (-not $AdvisoryDb) {
  $AdvisoryDb = Join-Path $HOME ".cargo\advisory-db"
}

if (-not (Get-Command cargo -ErrorAction SilentlyContinue)) {
  throw "cargo not found in PATH"
}
if (-not (Get-Command git -ErrorAction SilentlyContinue)) {
  throw "git not found in PATH"
}

$cargoAuditCmd = Get-Command cargo-audit -ErrorAction SilentlyContinue
if (-not $cargoAuditCmd) {
  throw 'cargo-audit not found in PATH; install with `cargo install cargo-audit`'
}

if (-not (Test-Path $LockFile)) {
  throw "lock file not found: $LockFile"
}

if (-not (Test-Path $AdvisoryDb)) {
  Info "cloning RustSec advisory database to $AdvisoryDb"
  $parent = Split-Path -Parent $AdvisoryDb
  if ($parent) {
    New-Item -ItemType Directory -Force -Path $parent | Out-Null
  }
  git clone --quiet --depth 1 $AdvisoryUrl $AdvisoryDb *> $null
  if ($LASTEXITCODE -ne 0) {
    throw "failed to clone advisory database: $AdvisoryUrl"
  }
  Ok "advisory database cloned"
} elseif (-not $SkipFetch) {
  Info "updating RustSec advisory database"
  git -C $AdvisoryDb fetch --quiet --depth 1 origin *> $null
  if ($LASTEXITCODE -eq 0) {
    git -C $AdvisoryDb reset --hard --quiet FETCH_HEAD *> $null
    if ($LASTEXITCODE -eq 0) {
      Ok "advisory database updated"
    } else {
      Warn "advisory database fetch succeeded but reset failed; using existing checkout"
    }
  } else {
    Warn "advisory database update failed; using existing local checkout"
  }
} else {
  Warn "skip advisory database fetch"
}

$args = @(
  "audit",
  "--db", $AdvisoryDb,
  "--no-fetch",
  "--file", $LockFile
)
if ($DenyWarnings) {
  $args += @("--deny", "warnings")
}
if ($Json) {
  $args += "--json"
}

Info ("running cargo audit against {0}" -f $LockFile)
& cargo @args
if ($LASTEXITCODE -ne 0) {
  throw "cargo audit reported findings or failed"
}
Ok "cargo audit passed"
