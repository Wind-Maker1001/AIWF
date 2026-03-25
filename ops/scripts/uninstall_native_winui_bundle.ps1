param(
  [string]$InstallRoot = "",
  [switch]$RemoveDesktopShortcut,
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ if (-not $Quiet) { Write-Host "[INFO] $m" -ForegroundColor Cyan } }
function Ok($m){ if (-not $Quiet) { Write-Host "[ OK ] $m" -ForegroundColor Green } }

function Remove-ShortcutIfOwned([string]$ShortcutPath, [string]$InstallRoot) {
  if (-not (Test-Path $ShortcutPath)) { return }
  try {
    $shell = New-Object -ComObject WScript.Shell
    $shortcut = $shell.CreateShortcut($ShortcutPath)
    $target = [string]$shortcut.TargetPath
    if ($target.StartsWith($InstallRoot, [System.StringComparison]::OrdinalIgnoreCase)) {
      Remove-Item $ShortcutPath -Force -ErrorAction SilentlyContinue
    }
  } catch {
    Remove-Item $ShortcutPath -Force -ErrorAction SilentlyContinue
  }
}

if (-not $InstallRoot) {
  $InstallRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
}

if (-not (Test-Path $InstallRoot)) {
  throw "install root not found: $InstallRoot"
}

$startMenuLink = Join-Path (Join-Path $env:APPDATA "Microsoft\Windows\Start Menu\Programs\AIWF") "AIWF Native WinUI.lnk"
$desktopLink = Join-Path ([Environment]::GetFolderPath("Desktop")) "AIWF Native WinUI.lnk"

Remove-ShortcutIfOwned -ShortcutPath $startMenuLink -InstallRoot $InstallRoot
if ($RemoveDesktopShortcut) {
  Remove-ShortcutIfOwned -ShortcutPath $desktopLink -InstallRoot $InstallRoot
}

$uninstallKeyPath = "HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall\AIWF.Native.WinUI"
if (Test-Path $uninstallKeyPath) {
  Remove-Item $uninstallKeyPath -Recurse -Force -ErrorAction SilentlyContinue
}

$installRootParent = Split-Path $InstallRoot -Parent
$escapedInstallRoot = $InstallRoot.Replace('"', '\"')
$cleanupCommand = "ping 127.0.0.1 -n 3 >nul && rmdir /s /q ""$escapedInstallRoot"""
Start-Process -FilePath "cmd.exe" -ArgumentList "/c $cleanupCommand" -WindowStyle Hidden | Out-Null

Ok ("native winui uninstall scheduled: " + $InstallRoot)
