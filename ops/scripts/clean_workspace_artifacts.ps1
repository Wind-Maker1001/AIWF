param(
  [switch]$KeepLatestInstaller = $true,
  [switch]$RemoveLogs,
  [switch]$RemoveDesktopUnpacked = $true,
  [switch]$RemoveDesktopDist,
  [switch]$RemoveDesktopLiteUnpacked = $true,
  [switch]$RemoveDesktopLiteDist,
  [switch]$RemoveTmp,
  [switch]$RemoveAppsTmp,
  [switch]$RemoveOfflineJobs,
  [switch]$RemoveRelease,
  [switch]$RemoveBusJobs,
  [int]$KeepLatestBusJobs = 3,
  [switch]$RemoveAccelTarget,
  [switch]$RemoveAccelNestedGit,
  [switch]$ForceDangerous,
  [switch]$DryRun
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Act($m){ if($DryRun){ Write-Host "[DRY ] $m" -ForegroundColor Yellow } else { Write-Host "[ACT ] $m" -ForegroundColor DarkCyan } }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$desktopDist = Join-Path $root "apps\dify-desktop\dist"
$desktopLiteDist = Join-Path $root "apps\dify-desktop\dist-lite"
$logsDir = Join-Path $root "ops\logs"
$accelTargetDir = Join-Path $root "apps\accel-rust\target"
$accelNestedGitDir = Join-Path $root "apps\accel-rust\.git"
$tmpDir = Join-Path $root "tmp"
$appsTmpDir = Join-Path $root "apps\tmp"
$offlineJobsDir = Join-Path $root "offline-jobs"
$releaseDir = Join-Path $root "release"
$busJobsDir = Join-Path $root "bus\jobs"

if (Test-Path $desktopDist) {
  if ($RemoveDesktopUnpacked) {
    $unpacked = Join-Path $desktopDist "win-unpacked"
    if (Test-Path $unpacked) {
      Act "remove $unpacked"
      if (-not $DryRun) {
        Remove-Item $unpacked -Recurse -Force
        Ok "removed dist/win-unpacked"
      }
    }
  }

  $installers = @(Get-ChildItem $desktopDist -File -Filter "AIWF Dify Desktop Setup *.exe" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending)
  if ($installers.Count -gt 1 -and $KeepLatestInstaller) {
    $toRemove = $installers | Select-Object -Skip 1
    foreach($f in $toRemove){
      Act "remove old installer: $($f.Name)"
      if (-not $DryRun) {
        Remove-Item $f.FullName -Force
      }
      $bm = "$($f.FullName).blockmap"
      if (Test-Path $bm) {
        Act "remove blockmap: $(Split-Path $bm -Leaf)"
        if (-not $DryRun) { Remove-Item $bm -Force }
      }
    }
    if (-not $DryRun) { Ok "old full installers cleaned" }
  }
}

if (Test-Path $desktopLiteDist) {
  if ($RemoveDesktopLiteUnpacked) {
    $unpackedLite = Join-Path $desktopLiteDist "win-unpacked"
    if (Test-Path $unpackedLite) {
      Act "remove $unpackedLite"
      if (-not $DryRun) {
        Remove-Item $unpackedLite -Recurse -Force
        Ok "removed dist-lite/win-unpacked"
      }
    }
  }

  $liteInstallers = @(Get-ChildItem $desktopLiteDist -File -Filter "AIWF Dify Desktop Lite Setup *.exe" -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending)
  if ($liteInstallers.Count -gt 1 -and $KeepLatestInstaller) {
    $toRemoveLite = $liteInstallers | Select-Object -Skip 1
    foreach($f in $toRemoveLite){
      Act "remove old lite installer: $($f.Name)"
      if (-not $DryRun) {
        Remove-Item $f.FullName -Force
      }
    }
    if (-not $DryRun) { Ok "old lite installers cleaned" }
  }
}

if ($RemoveLogs -and (Test-Path $logsDir)) {
  $targets = @("*.log", "*.err.log", "*.out.log", "*.txt")
  $seen = @{}
  foreach($p in $targets){
    Get-ChildItem $logsDir -File -Filter $p -ErrorAction SilentlyContinue | ForEach-Object {
      if ($seen.ContainsKey($_.FullName)) { return }
      $seen[$_.FullName] = $true
      Act "remove log: $($_.Name)"
      if (-not $DryRun) { Remove-Item $_.FullName -Force }
    }
  }
  if (-not $DryRun) { Ok "logs cleaned" }
}

if ($RemoveAccelTarget -and (Test-Path $accelTargetDir)) {
  Act "remove $accelTargetDir"
  if (-not $DryRun) {
    Remove-Item $accelTargetDir -Recurse -Force
    Ok "accel-rust target cleaned"
  }
}

if ($RemoveBusJobs -and (Test-Path $busJobsDir)) {
  $keep = [Math]::Max(0, $KeepLatestBusJobs)
  $jobDirs = @(Get-ChildItem $busJobsDir -Directory -ErrorAction SilentlyContinue | Sort-Object LastWriteTime -Descending)
  if ($jobDirs.Count -gt $keep) {
    $toRemoveJobs = $jobDirs | Select-Object -Skip $keep
    foreach($d in $toRemoveJobs){
      Act "remove bus job: $($d.FullName)"
      if (-not $DryRun) { Remove-Item $d.FullName -Recurse -Force }
    }
    if (-not $DryRun) { Ok "bus jobs cleaned (kept latest $keep)" }
  }
}

if ($RemoveTmp -and (Test-Path $tmpDir)) {
  Act "remove $tmpDir"
  if (-not $DryRun) {
    Remove-Item $tmpDir -Recurse -Force
    Ok "tmp cleaned"
  }
}

if ($RemoveAppsTmp -and (Test-Path $appsTmpDir)) {
  Act "remove $appsTmpDir"
  if (-not $DryRun) {
    Remove-Item $appsTmpDir -Recurse -Force
    Ok "apps/tmp cleaned"
  }
}

if ($RemoveOfflineJobs -and (Test-Path $offlineJobsDir)) {
  Act "remove $offlineJobsDir"
  if (-not $DryRun) {
    Remove-Item $offlineJobsDir -Recurse -Force
    Ok "offline-jobs cleaned"
  }
}

if ($RemoveRelease -and (Test-Path $releaseDir)) {
  Act "remove $releaseDir"
  if (-not $DryRun) {
    Remove-Item $releaseDir -Recurse -Force
    Ok "release cleaned"
  }
}

if ($RemoveDesktopDist -and (Test-Path $desktopDist)) {
  Act "remove $desktopDist"
  if (-not $DryRun) {
    Remove-Item $desktopDist -Recurse -Force
    Ok "desktop dist cleaned"
  }
}

if ($RemoveDesktopLiteDist -and (Test-Path $desktopLiteDist)) {
  Act "remove $desktopLiteDist"
  if (-not $DryRun) {
    Remove-Item $desktopLiteDist -Recurse -Force
    Ok "desktop dist-lite cleaned"
  }
}

if ($RemoveAccelNestedGit -and (Test-Path $accelNestedGitDir)) {
  if (-not $ForceDangerous) {
    throw "refuse to remove nested git metadata without -ForceDangerous"
  }
  Act "remove nested git dir: $accelNestedGitDir"
  if (-not $DryRun) {
    Remove-Item $accelNestedGitDir -Recurse -Force
    Ok "accel-rust nested git dir removed"
  }
}

if ($DryRun) {
  Ok "dry run finished (no files deleted)"
} else {
  Ok "workspace artifact cleanup finished"
}
