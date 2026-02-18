param(
  [string]$RepoDir = "",
  [string]$UserName = "AIWF Local",
  [string]$UserEmail = "aiwf-local@example.com",
  [string]$InitialCommitMessage = "chore: bootstrap repository",
  [string]$RemoteUrl = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoDir) {
  $RepoDir = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

function Resolve-GitExe {
  $cmd = Get-Command git -ErrorAction SilentlyContinue
  if ($cmd) { return $cmd.Source }
  $fallback = "C:\Program Files\Git\cmd\git.exe"
  if (Test-Path $fallback) { return $fallback }
  throw "git not found"
}

$git = Resolve-GitExe
Push-Location $RepoDir
try {
  if (-not (Test-Path ".git")) {
    & $git init | Out-Null
    Ok "initialized git repository"
  }

  & $git config user.name $UserName
  & $git config user.email $UserEmail
  Ok "configured git identity"

  $hasCommit = $false
  try {
    & $git cat-file -e HEAD 2>$null
    $hasCommit = ($LASTEXITCODE -eq 0)
  } catch {
    $hasCommit = $false
  }

  if (-not $hasCommit) {
    $nestedRepos = @()
    Get-ChildItem -Recurse -Force -Directory -Filter .git -ErrorAction SilentlyContinue | ForEach-Object {
      $repoDir = Split-Path -Parent $_.FullName
      if ((Resolve-Path $repoDir).Path -ne (Resolve-Path ".").Path) {
        $nestedRepos += $repoDir
      }
    }

    $addArgs = @("add", "-A", "--", ".")
    foreach($nr in $nestedRepos){
      $rel = Resolve-Path -Relative $nr
      $rel = ($rel -replace '^[.][\\/]', '')
      $rel = $rel.Replace("\", "/")
      if ($rel) {
        $addArgs += ":(exclude)$rel"
      }
    }
    & $git @addArgs
    & $git commit -m $InitialCommitMessage
    if ($LASTEXITCODE -ne 0) { throw "initial commit failed" }
    Ok "created initial commit"
  } else {
    Info "repository already has commits; skipping initial commit"
  }

  if ($RemoteUrl -and $RemoteUrl.Trim()) {
    $origin = (& $git remote) -contains "origin"
    if ($origin) {
      & $git remote set-url origin $RemoteUrl
    } else {
      & $git remote add origin $RemoteUrl
    }
    Ok "configured origin remote"
  } else {
    Info "remote url not provided; skipped origin setup"
  }

  & $git status --short
}
finally {
  Pop-Location
}
