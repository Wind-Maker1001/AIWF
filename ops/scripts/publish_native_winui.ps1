param(
  [string]$Root = "",
  [ValidateSet("Debug", "Release")]
  [string]$Configuration = "Release",
  [string]$Version = "",
  [string]$OutDir = "",
  [switch]$PublishSingleFile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $Root) {
  $Root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$projectPath = Join-Path $Root "apps\dify-native-winui\src\WinUI3Bootstrap\WinUI3Bootstrap.csproj"
if (-not (Test-Path $projectPath)) {
  throw "native winui project not found: $projectPath"
}

if (-not $OutDir) {
  $label = if ([string]::IsNullOrWhiteSpace($Version)) { $Configuration.ToLowerInvariant() } else { $Version }
  $OutDir = Join-Path $Root ("release\native_winui_publish_{0}" -f $label)
}

if (Test-Path $OutDir) {
  Remove-Item (Join-Path $OutDir "*") -Recurse -Force -ErrorAction SilentlyContinue
}
New-Item -ItemType Directory -Path $OutDir -Force | Out-Null

$publishArgs = @(
  "publish",
  $projectPath,
  "-c", $Configuration,
  "-r", "win-x64",
  "-p:Platform=x64",
  "-p:SelfContained=true",
  "-p:WindowsPackageType=None",
  "-p:PublishSingleFile=$($PublishSingleFile.IsPresent.ToString().ToLowerInvariant())",
  "-o", $OutDir
)
if (-not [string]::IsNullOrWhiteSpace($Version)) {
  $publishArgs += "-p:Version=$Version"
  $publishArgs += "-p:InformationalVersion=$Version"
}

Info ("publishing native winui app to " + $OutDir)
dotnet @publishArgs
if ($LASTEXITCODE -ne 0) {
  throw "native winui publish failed"
}

$exePath = Join-Path $OutDir "WinUI3Bootstrap.exe"
if (-not (Test-Path $exePath)) {
  throw "published native winui executable not found: $exePath"
}

Ok ("native winui publish passed: " + $OutDir)
Write-Output $OutDir
