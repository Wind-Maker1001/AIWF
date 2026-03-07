param(
    [ValidateSet("Debug", "Release")]
    [string]$Configuration = "Debug",
    [int]$AutoExitMs = 2500,
    [string]$OutputDir = ""
)

$ErrorActionPreference = "Stop"

$repoRoot = Resolve-Path (Join-Path $PSScriptRoot "..\\..")
$winuiProject = Join-Path $repoRoot "apps\\dify-native-winui\\src\\WinUI3Bootstrap\\WinUI3Bootstrap.csproj"
$perfProject = Join-Path $repoRoot "apps\\dify-native-winui\\tests\\WinUI3Bootstrap.Perf\\WinUI3Bootstrap.Perf.csproj"

if ([string]::IsNullOrWhiteSpace($OutputDir)) {
    $stamp = Get-Date -Format "yyyyMMdd-HHmmss"
    $OutputDir = Join-Path $repoRoot "ops\\logs\\perf\\native-winui\\$stamp"
}

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null

$startupLog = Join-Path $OutputDir "startup.json"
$jsonReport = Join-Path $OutputDir "baseline.json"
$markdownReport = Join-Path $OutputDir "baseline.md"

Push-Location $repoRoot
try {
    & dotnet build $winuiProject -c $Configuration
    if ($LASTEXITCODE -ne 0) {
        throw "WinUI build failed."
    }

    $exePath = Join-Path $repoRoot "apps\\dify-native-winui\\src\\WinUI3Bootstrap\\bin\\$Configuration\\net8.0-windows10.0.19041.0\\win-x64\\WinUI3Bootstrap.exe"
    if (-not (Test-Path $exePath)) {
        throw "WinUI executable not found: $exePath"
    }

    $env:AIWF_NATIVE_PERF_LOG_PATH = $startupLog
    $env:AIWF_NATIVE_PERF_AUTO_EXIT_MS = "$AutoExitMs"
    try {
        $proc = Start-Process -FilePath $exePath -PassThru
        $waitMs = [Math]::Max($AutoExitMs + 8000, 12000)
        if (-not $proc.WaitForExit($waitMs)) {
            Stop-Process -Id $proc.Id -Force
            throw "Native WinUI app did not exit within ${waitMs}ms."
        }
    }
    finally {
        Remove-Item Env:AIWF_NATIVE_PERF_LOG_PATH -ErrorAction SilentlyContinue
        Remove-Item Env:AIWF_NATIVE_PERF_AUTO_EXIT_MS -ErrorAction SilentlyContinue
    }

    & dotnet run --project $perfProject -c $Configuration -- --startup-log $startupLog --json $jsonReport --markdown $markdownReport
    if ($LASTEXITCODE -ne 0) {
        throw "Perf aggregation failed."
    }
}
finally {
    Pop-Location
}

Write-Host "Native WinUI perf baseline saved to:"
Write-Host "  $jsonReport"
Write-Host "  $markdownReport"
