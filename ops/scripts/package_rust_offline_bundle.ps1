param(
  [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
  [string]$OutDir = "",
  [switch]$SkipBuild
)

$ErrorActionPreference = "Stop"
function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ Write-Host "[FAIL] $m" -ForegroundColor Red; throw $m }

$rustRoot = Join-Path $ProjectRoot "apps\accel-rust"
if (-not $OutDir) { $OutDir = Join-Path $ProjectRoot "release\offline_rust_bundle" }
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

if (-not $SkipBuild) {
  Push-Location $rustRoot
  try {
    Info "building accel-rust release"
    cargo build --release -q
    if ($LASTEXITCODE -ne 0) { Fail "cargo build --release failed" }
  } finally { Pop-Location }
}

$exe = Join-Path $rustRoot "target\release\accel-rust.exe"
if (-not (Test-Path $exe)) { Fail "missing exe: $exe" }

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$bundle = Join-Path $OutDir "accel-rust-offline-$stamp"
$bin = Join-Path $bundle "bin"
$conf = Join-Path $bundle "conf"
$data = Join-Path $bundle "data"
New-Item -ItemType Directory -Force -Path $bin,$conf,$data | Out-Null
Copy-Item -Force $exe (Join-Path $bin "accel-rust.exe")

$envExample = @'
AIWF_ALLOW_EGRESS=false
AIWF_TENANT_MAX_CONCURRENCY=4
AIWF_TENANT_MAX_ROWS=250000
AIWF_TENANT_MAX_PAYLOAD_BYTES=134217728
AIWF_TENANT_MAX_WORKFLOW_STEPS=128
AIWF_OPERATOR_ALLOWLIST=
AIWF_OPERATOR_DENYLIST=
'@
$envExample | Set-Content -Path (Join-Path $conf "env.offline.example") -Encoding UTF8

$health = @'
param([string]$Base="http://127.0.0.1:18082")
try {
  $h = Invoke-RestMethod -Uri "$Base/health" -Method Get -TimeoutSec 5
  if ($h.ok -ne $true) { throw "health not ok" }
  Write-Host "[ OK ] health endpoint ready"
  exit 0
} catch {
  Write-Host "[FAIL] health check failed: $($_.Exception.Message)"
  exit 1
}
'@
$health | Set-Content -Path (Join-Path $bundle "healthcheck.ps1") -Encoding UTF8

$readme = @"
# accel-rust offline bundle

## run
1. start: `bin\accel-rust.exe`
2. verify: `powershell -ExecutionPolicy Bypass -File .\healthcheck.ps1`
3. use local endpoints only (`AIWF_ALLOW_EGRESS=false`)
"@
$readme | Set-Content -Path (Join-Path $bundle "README.txt") -Encoding UTF8

$hash = (Get-FileHash -Algorithm SHA256 (Join-Path $bin "accel-rust.exe")).Hash.ToLowerInvariant()
"$hash  accel-rust.exe" | Set-Content -Path (Join-Path $bundle "accel-rust.exe.sha256.txt") -Encoding UTF8

Ok "offline bundle ready: $bundle"
