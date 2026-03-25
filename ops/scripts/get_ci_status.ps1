param(
  [string]$Branch = "",
  [string]$Ref = "",
  [int]$PerPage = 10,
  [switch]$Quiet
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){
  if (-not $Quiet) {
    Write-Host "[INFO] $m" -ForegroundColor Cyan
  }
}
function Warn($m){
  if (-not $Quiet) {
    Write-Host "[WARN] $m" -ForegroundColor Yellow
  }
}

function Invoke-GitCapture {
  param(
    [Parameter(Mandatory=$true)][string[]]$Args
  )
  $output = & git @Args 2>&1
  if ($LASTEXITCODE -ne 0) {
    throw ("git {0} failed:`n{1}" -f ($Args -join " "), (($output | ForEach-Object { $_.ToString() }) -join [Environment]::NewLine))
  }
  return @($output | ForEach-Object { $_.ToString() })
}

function Resolve-RepoSlug {
  $remote = (Invoke-GitCapture -Args @("remote", "get-url", "origin") | Select-Object -First 1).Trim()
  if ($remote -match 'github\.com[:/](?<owner>[^/:\s]+)/(?<repo>[^/\s]+?)(?:\.git)?$') {
    return "{0}/{1}" -f $Matches.owner, $Matches.repo
  }
  throw "unable to parse GitHub repo from origin remote: $remote"
}

function Resolve-BranchName {
  param([string]$RequestedBranch)
  if (-not [string]::IsNullOrWhiteSpace($RequestedBranch)) {
    return $RequestedBranch.Trim()
  }
  $branch = (Invoke-GitCapture -Args @("branch", "--show-current") | Select-Object -First 1).Trim()
  if ([string]::IsNullOrWhiteSpace($branch)) {
    throw "current branch is detached; pass -Branch explicitly"
  }
  return $branch
}

function Resolve-HeadSha {
  param([string]$RequestedRef, [string]$FallbackBranch)
  if (-not [string]::IsNullOrWhiteSpace($RequestedRef)) {
    return (Invoke-GitCapture -Args @("rev-parse", $RequestedRef.Trim()) | Select-Object -First 1).Trim()
  }
  return (Invoke-GitCapture -Args @("rev-parse", $FallbackBranch) | Select-Object -First 1).Trim()
}

function Get-GitHubToken {
  if (-not [string]::IsNullOrWhiteSpace($env:GH_TOKEN)) {
    return $env:GH_TOKEN
  }
  if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) {
    return $env:GITHUB_TOKEN
  }

  $credText = @"
protocol=https
host=github.com
"@ | git credential-manager get --no-ui 2>$null

  if ($LASTEXITCODE -ne 0 -or [string]::IsNullOrWhiteSpace(($credText | Out-String))) {
    throw "GH_TOKEN/GITHUB_TOKEN not set and git credential-manager returned no GitHub credentials"
  }

  $cred = ($credText | Out-String) | ConvertFrom-StringData
  if ([string]::IsNullOrWhiteSpace($cred.password)) {
    throw "git credential-manager returned credentials without password"
  }
  return $cred.password
}

function New-GitHubHeaders {
  param([Parameter(Mandatory=$true)][string]$Token)
  return @{
    Authorization = "Bearer $Token"
    Accept = "application/vnd.github+json"
    "X-GitHub-Api-Version" = "2022-11-28"
  }
}

function Select-LatestRun {
  param(
    [AllowNull()]
    [AllowEmptyCollection()]
    [object[]]$Runs = @(),
    [Parameter(Mandatory=$true)][string]$Name,
    [string]$Event = "",
    [string]$HeadSha = ""
  )
  $filtered = @($Runs | Where-Object {
    $_.name -eq $Name -and
    ([string]::IsNullOrWhiteSpace($Event) -or $_.event -eq $Event) -and
    ([string]::IsNullOrWhiteSpace($HeadSha) -or $_.head_sha -eq $HeadSha)
  } | Sort-Object created_at -Descending)
  if ($filtered.Count -gt 0) {
    return $filtered[0]
  }
  return $null
}
function Get-LocalArchitectureScorecardSummary([string]$RepoRoot) {
  $path = Join-Path $RepoRoot "ops\logs\architecture\architecture_scorecard_release_ready_latest.json"
  $summary = [ordered]@{
    Path = $path
    Exists = $false
    OverallStatus = "missing"
    GeneratedAt = ""
    Profile = ""
  }
  if (-not (Test-Path $path)) {
    return [pscustomobject]$summary
  }
  try {
    $raw = Get-Content -Raw -Encoding UTF8 $path | ConvertFrom-Json
    $summary.Exists = $true
    $summary.OverallStatus = [string]$raw.overall_status
    $summary.GeneratedAt = [string]$raw.generated_at
    $summary.Profile = [string]$raw.profile
  } catch {
    $summary.Exists = $true
    $summary.OverallStatus = "unreadable"
  }
  return [pscustomobject]$summary
}

$repoSlug = Resolve-RepoSlug
$branchName = Resolve-BranchName -RequestedBranch $Branch
$headSha = Resolve-HeadSha -RequestedRef $Ref -FallbackBranch $branchName
$token = Get-GitHubToken
$headers = New-GitHubHeaders -Token $token
$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$architectureScorecard = Get-LocalArchitectureScorecardSummary -RepoRoot $repoRoot

Info ("querying CI status for {0}@{1}" -f $branchName, $headSha)
$repoMeta = Invoke-RestMethod -Uri ("https://api.github.com/repos/{0}" -f $repoSlug) -Headers $headers -Method Get
$defaultBranch = [string]$repoMeta.default_branch
$branchRuns = Invoke-RestMethod -Uri ("https://api.github.com/repos/{0}/actions/runs?branch={1}&per_page={2}" -f $repoSlug, $branchName, [Math]::Max(1, $PerPage)) -Headers $headers -Method Get
$scheduledFullRuns = Invoke-RestMethod -Uri ("https://api.github.com/repos/{0}/actions/workflows/full-integration-self-hosted.yml/runs?event=schedule&per_page={1}" -f $repoSlug, [Math]::Max(1, $PerPage)) -Headers $headers -Method Get

$quickRun = Select-LatestRun -Runs @($branchRuns.workflow_runs) -Name "Quick CI" -Event "push" -HeadSha $headSha
if ($null -eq $quickRun) {
  $quickRun = Select-LatestRun -Runs @($branchRuns.workflow_runs) -Name "Quick CI" -HeadSha $headSha
}
$manualFullForHeadRun = Select-LatestRun -Runs @($branchRuns.workflow_runs) -Name "Full Integration (Self-Hosted)" -Event "workflow_dispatch" -HeadSha $headSha
$manualFullLatestRun = Select-LatestRun -Runs @($branchRuns.workflow_runs) -Name "Full Integration (Self-Hosted)" -Event "workflow_dispatch"
$scheduledFullRun = Select-LatestRun -Runs @($scheduledFullRuns.workflow_runs) -Name "Full Integration (Self-Hosted)" -Event "schedule"
$scheduledFullNote = if ($null -ne $scheduledFullRun) {
  ""
} elseif (-not [string]::IsNullOrWhiteSpace($defaultBranch) -and -not [string]::Equals($branchName, $defaultBranch, [System.StringComparison]::OrdinalIgnoreCase)) {
  "Nightly schedule runs on the default branch only. Current branch '$branchName' will not produce ScheduledFull until merged into '$defaultBranch'. Use dispatch_full_integration_self_hosted.ps1 for branch validation."
} else {
  "No scheduled Full Integration run has been recorded yet for the default branch."
}

if (-not [string]::IsNullOrWhiteSpace($scheduledFullNote)) {
  Warn $scheduledFullNote
}

[pscustomobject]@{
  Repo = $repoSlug
  Branch = $branchName
  DefaultBranch = $defaultBranch
  HeadSha = $headSha
  QuickCi = if ($null -eq $quickRun) {
    $null
  } else {
    [pscustomobject]@{
      Id = [long]$quickRun.id
      Status = $quickRun.status
      Conclusion = $quickRun.conclusion
      Event = $quickRun.event
      CreatedAt = $quickRun.created_at
      Url = $quickRun.html_url
    }
  }
  ManualFullForHead = if ($null -eq $manualFullForHeadRun) {
    $null
  } else {
    [pscustomobject]@{
      Id = [long]$manualFullForHeadRun.id
      Status = $manualFullForHeadRun.status
      Conclusion = $manualFullForHeadRun.conclusion
      Event = $manualFullForHeadRun.event
      HeadSha = $manualFullForHeadRun.head_sha
      CreatedAt = $manualFullForHeadRun.created_at
      Url = $manualFullForHeadRun.html_url
    }
  }
  LatestManualFull = if ($null -eq $manualFullLatestRun) {
    $null
  } else {
    [pscustomobject]@{
      Id = [long]$manualFullLatestRun.id
      Status = $manualFullLatestRun.status
      Conclusion = $manualFullLatestRun.conclusion
      Event = $manualFullLatestRun.event
      HeadSha = $manualFullLatestRun.head_sha
      CreatedAt = $manualFullLatestRun.created_at
      Url = $manualFullLatestRun.html_url
    }
  }
  ScheduledFull = if ($null -eq $scheduledFullRun) {
    $null
  } else {
    [pscustomobject]@{
      Id = [long]$scheduledFullRun.id
      Status = $scheduledFullRun.status
      Conclusion = $scheduledFullRun.conclusion
      Event = $scheduledFullRun.event
      HeadBranch = $scheduledFullRun.head_branch
      HeadSha = $scheduledFullRun.head_sha
      CreatedAt = $scheduledFullRun.created_at
      Url = $scheduledFullRun.html_url
    }
  }
  ScheduledFullNote = $scheduledFullNote
  ArchitectureScorecard = $architectureScorecard
}
