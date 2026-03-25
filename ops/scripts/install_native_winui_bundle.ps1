param(
  [string]$BundleRoot = "",
  [string]$InstallRoot = "",
  [switch]$CreateDesktopShortcut,
  [switch]$NoStartMenuShortcut,
  [switch]$SkipUninstallRegistration,
  [switch]$Force
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

function New-ShortcutFile([string]$Path, [string]$TargetPath, [string]$Arguments, [string]$WorkingDirectory, [string]$IconLocation) {
  $shell = New-Object -ComObject WScript.Shell
  $shortcut = $shell.CreateShortcut($Path)
  $shortcut.TargetPath = $TargetPath
  $shortcut.Arguments = $Arguments
  $shortcut.WorkingDirectory = $WorkingDirectory
  if (-not [string]::IsNullOrWhiteSpace($IconLocation)) {
    $shortcut.IconLocation = $IconLocation
  }
  $shortcut.Save()
}

function Remove-DirectoryContents([string]$Path) {
  if (-not (Test-Path $Path)) { return }
  Get-ChildItem -Path $Path -Force | ForEach-Object {
    Remove-Item $_.FullName -Recurse -Force -ErrorAction Stop
  }
}

if (-not $BundleRoot) {
  $BundleRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

$manifestPath = Join-Path $BundleRoot "manifest.json"
if (-not (Test-Path $manifestPath)) {
  throw "bundle manifest not found: $manifestPath"
}
$manifest = Get-Content $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
$version = [string]$manifest.version
if ([string]::IsNullOrWhiteSpace($version)) {
  throw "bundle manifest missing version: $manifestPath"
}

$entryRelative = [string]$manifest.entry
if ([string]::IsNullOrWhiteSpace($entryRelative)) {
  throw "bundle manifest missing entry: $manifestPath"
}

if (-not $InstallRoot) {
  $InstallRoot = Join-Path $env:LOCALAPPDATA "Programs\AIWF\NativeWinUI"
}

$appEntry = Join-Path $InstallRoot ($entryRelative -replace '/', '\')
$uninstallScript = Join-Path $InstallRoot "Uninstall_AIWF_Native_WinUI.ps1"
$installStatePath = Join-Path $InstallRoot "install_state.json"

if (Test-Path $InstallRoot) {
  if (-not $Force) {
    Warn "existing WinUI install found, replacing in-place"
  }
  Remove-DirectoryContents $InstallRoot
} else {
  New-Item -ItemType Directory -Path $InstallRoot -Force | Out-Null
}

Info ("copying native winui bundle into " + $InstallRoot)
Copy-Item (Join-Path $BundleRoot "*") $InstallRoot -Recurse -Force

if (-not (Test-Path $appEntry)) {
  throw "installed app entry not found: $appEntry"
}
if (-not (Test-Path $uninstallScript)) {
  throw "installed uninstall script not found: $uninstallScript"
}

$startMenuDir = Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\AIWF"
$startMenuLink = Join-Path $startMenuDir "AIWF Native WinUI.lnk"
$desktopLink = Join-Path ([Environment]::GetFolderPath("Desktop")) "AIWF Native WinUI.lnk"

if (-not $NoStartMenuShortcut) {
  New-Item -ItemType Directory -Path $startMenuDir -Force | Out-Null
  New-ShortcutFile -Path $startMenuLink -TargetPath $appEntry -Arguments "" -WorkingDirectory (Split-Path $appEntry -Parent) -IconLocation $appEntry
  Ok ("start menu shortcut created: " + $startMenuLink)
}

if ($CreateDesktopShortcut) {
  New-ShortcutFile -Path $desktopLink -TargetPath $appEntry -Arguments "" -WorkingDirectory (Split-Path $appEntry -Parent) -IconLocation $appEntry
  Ok ("desktop shortcut created: " + $desktopLink)
}

if (-not $SkipUninstallRegistration) {
  $estimatedKb = [int]([math]::Ceiling(((Get-ChildItem $InstallRoot -Recurse -File | Measure-Object -Property Length -Sum).Sum) / 1KB))
  $uninstallKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\AIWF.Native.WinUI"
  New-Item -Path $uninstallKeyPath -Force | Out-Null
  Set-ItemProperty -Path $uninstallKeyPath -Name "DisplayName" -Value "AIWF Native WinUI"
  Set-ItemProperty -Path $uninstallKeyPath -Name "DisplayVersion" -Value $version
  Set-ItemProperty -Path $uninstallKeyPath -Name "Publisher" -Value "AIWF"
  Set-ItemProperty -Path $uninstallKeyPath -Name "InstallLocation" -Value $InstallRoot
  Set-ItemProperty -Path $uninstallKeyPath -Name "DisplayIcon" -Value $appEntry
  Set-ItemProperty -Path $uninstallKeyPath -Name "EstimatedSize" -Value $estimatedKb -Type DWord
  Set-ItemProperty -Path $uninstallKeyPath -Name "UninstallString" -Value ("powershell -ExecutionPolicy Bypass -File `"" + $uninstallScript + "`"")
  Set-ItemProperty -Path $uninstallKeyPath -Name "QuietUninstallString" -Value ("powershell -ExecutionPolicy Bypass -File `"" + $uninstallScript + "`" -Quiet")
}

$installState = [ordered]@{
  installed_at = (Get-Date).ToString("s")
  version = $version
  install_root = $InstallRoot
  app_entry = $appEntry
  uninstall_registration = (-not $SkipUninstallRegistration)
  start_menu_link = if ($NoStartMenuShortcut) { "" } else { $startMenuLink }
  desktop_link = if ($CreateDesktopShortcut) { $desktopLink } else { "" }
}
($installState | ConvertTo-Json -Depth 4) | Set-Content $installStatePath -Encoding UTF8

Ok ("native winui installed: " + $InstallRoot)
