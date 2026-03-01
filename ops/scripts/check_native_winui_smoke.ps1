param(
  [string]$Root = "",
  [string]$Configuration = "Release",
  [int]$AliveSeconds = 8,
  [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$nativeRoot = Join-Path $Root "apps\dify-native-winui"
$solutionPath = Join-Path $nativeRoot "AIWF.Native.WinUI.sln"
$projectDir = Join-Path $nativeRoot "src\WinUI3Bootstrap"

if (-not (Test-Path $solutionPath)) {
  throw "native winui solution not found: $solutionPath"
}

$msbuildCandidates = @(
  "D:\Environments\Microsoft Visual Studio\insiders\MSBuild\Current\Bin\amd64\MSBuild.exe",
  "D:\Environments\Microsoft Visual Studio\insiders\MSBuild\Current\Bin\MSBuild.exe"
)
$msbuildPath = $msbuildCandidates | Where-Object { Test-Path $_ } | Select-Object -First 1
if (-not $msbuildPath) {
  throw "MSBuild.exe not found in expected VS Insiders paths."
}

if (-not $SkipBuild) {
  Info "building native winui solution"
  & $msbuildPath $solutionPath /t:Restore,Build /p:Configuration=$Configuration /p:Platform=x64 /v:m
  if ($LASTEXITCODE -ne 0) {
    throw "native winui build failed"
  }
  Ok "native winui build passed"
} else {
  Warn "skip native winui build"
}

$searchRoot = Join-Path $projectDir ("bin\x64\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64")
$exe = Get-ChildItem -Path $searchRoot -Filter "WinUI3Bootstrap.exe" -File -ErrorAction SilentlyContinue | Select-Object -First 1
if (-not $exe) {
  throw "native winui exe not found under: $searchRoot"
}

Info ("launching native winui app: " + $exe.FullName)
$proc = Start-Process -FilePath $exe.FullName -PassThru
Start-Sleep -Seconds $AliveSeconds

if ($proc.HasExited) {
  throw ("native winui app exited early, exitCode=" + $proc.ExitCode)
}

Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
Ok ("native winui smoke passed (alive for " + $AliveSeconds + "s)")
