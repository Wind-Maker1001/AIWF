param(
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [int]$AutoExitMs = 2500,
  [double]$MaxWindowActivatedMs = 0,
  [double]$MaxMainWindowCtorMs = 0,
  [double]$MaxCanvasInitMs = 0,
  [double]$MaxCanvasPrewarmMs = 0,
  [switch]$SkipBuild
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function Resolve-NativeWinUiExePath([string]$Root, [string]$Configuration) {
  $candidates = @(
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\x64\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe")),
    (Join-Path $Root ("apps\dify-native-winui\src\WinUI3Bootstrap\bin\" + $Configuration + "\net8.0-windows10.0.19041.0\win-x64\WinUI3Bootstrap.exe"))
  )

  return $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1
}

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$projectPath = Join-Path $Root "apps\dify-native-winui\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj"
if (-not (Test-Path $projectPath)) {
  throw "native winui project not found: $projectPath"
}

try {
  dotnet --info | Out-Null
}
catch {
  throw "dotnet SDK not found. Install .NET SDK 8+ and Windows App SDK prerequisites first."
}

if (-not $SkipBuild) {
  Info "building native winui project"
  dotnet build $projectPath -c $Configuration
  if ($LASTEXITCODE -ne 0) {
    throw "native winui build failed"
  }
  Ok "native winui build passed"
} else {
  Warn "skip native winui build"
}

$exePath = Resolve-NativeWinUiExePath -Root $Root -Configuration $Configuration
if (-not $exePath) {
  throw "native winui executable not found: $exePath"
}

$stamp = Get-Date -Format "yyyyMMdd-HHmmss"
$outputDir = Join-Path $Root "ops\logs\smoke\native-winui\$stamp"
New-Item -ItemType Directory -Force -Path $outputDir | Out-Null
$startupLogPath = Join-Path $outputDir "startup.json"

$env:AIWF_NATIVE_PERF_LOG_PATH = $startupLogPath
$env:AIWF_NATIVE_PERF_AUTO_EXIT_MS = "$AutoExitMs"

try {
  Info ("launching native winui app: " + $exePath)
  $proc = Start-Process -FilePath $exePath -PassThru
  $waitMs = [Math]::Max($AutoExitMs + 8000, 12000)
  if (-not $proc.WaitForExit($waitMs)) {
    Stop-Process -Id $proc.Id -Force -ErrorAction SilentlyContinue
    throw "native winui app did not exit within ${waitMs}ms"
  }
}
finally {
  Remove-Item Env:AIWF_NATIVE_PERF_LOG_PATH -ErrorAction SilentlyContinue
  Remove-Item Env:AIWF_NATIVE_PERF_AUTO_EXIT_MS -ErrorAction SilentlyContinue
}

if (-not (Test-Path $startupLogPath)) {
  throw "startup log not found: $startupLogPath"
}

$snapshot = Get-Content $startupLogPath -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $snapshot -or -not $snapshot.Marks) {
  throw "startup log is missing marks: $startupLogPath"
}

$marks = $snapshot.Marks
$requiredMarks = @(
  "main_window_ctor_enter",
  "main_window_ctor_exit",
  "window_activated",
  "canvas_workspace_init_enter",
  "canvas_workspace_init_exit",
  "canvas_prewarm_enter",
  "canvas_prewarm_exit"
)

foreach ($mark in $requiredMarks) {
  if ($null -eq $marks.$mark) {
    throw "startup mark missing: $mark"
  }
}

$ctorDuration = [math]::Round(([double]$marks.main_window_ctor_exit) - ([double]$marks.main_window_ctor_enter), 3)
$windowActivatedMs = [double]$marks.window_activated
$canvasInitDuration = [math]::Round(([double]$marks.canvas_workspace_init_exit) - ([double]$marks.canvas_workspace_init_enter), 3)
$canvasPrewarmDuration = [math]::Round(([double]$marks.canvas_prewarm_exit) - ([double]$marks.canvas_prewarm_enter), 3)

Info ("startup log: " + $startupLogPath)
Info ("first window activated: " + $windowActivatedMs + " ms")
Info ("main window ctor duration: " + $ctorDuration + " ms")
Info ("canvas workspace init duration: " + $canvasInitDuration + " ms")
Info ("canvas prewarm duration: " + $canvasPrewarmDuration + " ms")

function Assert-MaxMetric([string]$Label, [double]$Actual, [double]$Limit) {
  if ($Limit -le 0) {
    return
  }

  if ($Actual -gt $Limit) {
    throw ("native winui smoke budget failed: " + $Label + " actual=" + $Actual.ToString("0.###") + "ms limit=" + $Limit.ToString("0.###") + "ms")
  }
}

Assert-MaxMetric "window_activated" $windowActivatedMs $MaxWindowActivatedMs
Assert-MaxMetric "main_window_ctor" $ctorDuration $MaxMainWindowCtorMs
Assert-MaxMetric "canvas_workspace_init" $canvasInitDuration $MaxCanvasInitMs
Assert-MaxMetric "canvas_prewarm" $canvasPrewarmDuration $MaxCanvasPrewarmMs

Ok "native winui smoke passed"
