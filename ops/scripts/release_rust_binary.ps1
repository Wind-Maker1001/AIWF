param(
  [string]$ProjectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path,
  [string]$OutDir = "",
  [switch]$SkipTest
)

$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Fail($m){ Write-Host "[FAIL] $m" -ForegroundColor Red; throw $m }

$rustRoot = Join-Path $ProjectRoot "apps\accel-rust"
if (-not (Test-Path (Join-Path $rustRoot "Cargo.toml"))) {
  Fail "Cargo.toml not found: $rustRoot"
}

if (-not $OutDir) {
  $OutDir = Join-Path $ProjectRoot "release\rust"
}
New-Item -ItemType Directory -Force -Path $OutDir | Out-Null

Push-Location $rustRoot
try {
  if (-not $SkipTest) {
    Info "running rust tests"
    cargo test -q
    if ($LASTEXITCODE -ne 0) { Fail "cargo test failed" }
  }
  Info "building rust release binary"
  cargo build --release -q
  if ($LASTEXITCODE -ne 0) { Fail "cargo build --release failed" }
} finally {
  Pop-Location
}

$exe = Join-Path $rustRoot "target\release\accel-rust.exe"
if (-not (Test-Path $exe)) { Fail "release exe not found: $exe" }

$stamp = Get-Date -Format "yyyyMMdd_HHmmss"
$pkgDir = Join-Path $OutDir "accel-rust_$stamp"
New-Item -ItemType Directory -Force -Path $pkgDir | Out-Null
$dstExe = Join-Path $pkgDir "accel-rust.exe"
Copy-Item -Force $exe $dstExe

$hash = (Get-FileHash -Algorithm SHA256 $dstExe).Hash.ToLowerInvariant()
$hashPath = Join-Path $pkgDir "accel-rust.exe.sha256.txt"
"$hash  accel-rust.exe" | Set-Content -Path $hashPath -Encoding UTF8

$meta = @{
  built_at = (Get-Date).ToUniversalTime().ToString("o")
  source = $rustRoot
  exe = $dstExe
  sha256 = $hash
} | ConvertTo-Json -Depth 4
$metaPath = Join-Path $pkgDir "build_meta.json"
$meta | Set-Content -Path $metaPath -Encoding UTF8

Ok "rust release packaged: $pkgDir"
