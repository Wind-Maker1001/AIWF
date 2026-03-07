param(
  [string]$Ref = "",
  [string]$Owner = "ci",
  [string]$EnvFile = "",
  [ValidateSet("Default","Quick","Full")]
  [string]$CiProfile = "Full",
  [bool]$RunFullIntegration = $true,
  [switch]$Wait,
  [int]$PollSeconds = 20,
  [int]$DiscoverTimeoutSeconds = 120,
  [int]$RunTimeoutMinutes = 120
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

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

function Resolve-RefName {
  param([string]$RequestedRef)
  if (-not [string]::IsNullOrWhiteSpace($RequestedRef)) {
    return $RequestedRef.Trim()
  }
  $branch = (Invoke-GitCapture -Args @("branch", "--show-current") | Select-Object -First 1).Trim()
  if ([string]::IsNullOrWhiteSpace($branch)) {
    throw "current branch is detached; pass -Ref explicitly"
  }
  return $branch
}

function Resolve-RefSha {
  param([Parameter(Mandatory=$true)][string]$ResolvedRef)
  return (Invoke-GitCapture -Args @("rev-parse", $ResolvedRef) | Select-Object -First 1).Trim()
}

function Get-GitHubToken {
  if (-not [string]::IsNullOrWhiteSpace($env:GH_TOKEN)) {
    return $env:GH_TOKEN
  }
  if (-not [string]::IsNullOrWhiteSpace($env:GITHUB_TOKEN)) {
    return $env:GITHUB_TOKEN
  }

  $gcm = Get-Command git -ErrorAction SilentlyContinue
  if ($null -eq $gcm) {
    throw "git not found in PATH"
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

function Find-DispatchedRun {
  param(
    [Parameter(Mandatory=$true)][string]$RepoSlug,
    [Parameter(Mandatory=$true)][hashtable]$Headers,
    [Parameter(Mandatory=$true)][string]$RefName,
    [Parameter(Mandatory=$true)][string]$HeadSha,
    [Parameter(Mandatory=$true)][datetimeoffset]$NotBeforeUtc,
    [Parameter(Mandatory=$true)][int]$TimeoutSeconds,
    [Parameter(Mandatory=$true)][int]$PollIntervalSeconds
  )

  $deadline = (Get-Date).ToUniversalTime().AddSeconds($TimeoutSeconds)
  while ((Get-Date).ToUniversalTime() -lt $deadline) {
    $uri = "https://api.github.com/repos/$RepoSlug/actions/workflows/full-integration-self-hosted.yml/runs?branch=$RefName&event=workflow_dispatch&per_page=20"
    $resp = Invoke-RestMethod -Uri $uri -Headers $Headers -Method Get
    foreach ($run in @($resp.workflow_runs | Sort-Object created_at -Descending)) {
      if ($run.head_sha -ne $HeadSha) { continue }
      $createdAt = [datetimeoffset]::Parse($run.created_at)
      if ($createdAt -lt $NotBeforeUtc) { continue }
      return $run
    }
    Start-Sleep -Seconds $PollIntervalSeconds
  }
  throw "timed out waiting for dispatched workflow run to appear"
}

function Wait-WorkflowRun {
  param(
    [Parameter(Mandatory=$true)][string]$RepoSlug,
    [Parameter(Mandatory=$true)][hashtable]$Headers,
    [Parameter(Mandatory=$true)][long]$RunId,
    [Parameter(Mandatory=$true)][int]$TimeoutMinutes,
    [Parameter(Mandatory=$true)][int]$PollIntervalSeconds
  )

  $deadline = (Get-Date).ToUniversalTime().AddMinutes($TimeoutMinutes)
  while ((Get-Date).ToUniversalTime() -lt $deadline) {
    $run = Invoke-RestMethod -Uri "https://api.github.com/repos/$RepoSlug/actions/runs/$RunId" -Headers $Headers -Method Get
    Write-Host ("[INFO] run {0}: status={1} conclusion={2}" -f $RunId, $run.status, $(if ($null -eq $run.conclusion) { "null" } else { $run.conclusion }))
    if ($run.status -eq "completed") {
      return $run
    }
    Start-Sleep -Seconds $PollIntervalSeconds
  }
  throw "timed out waiting for workflow run $RunId to complete"
}

$repoSlug = Resolve-RepoSlug
$refName = Resolve-RefName -RequestedRef $Ref
$headSha = Resolve-RefSha -ResolvedRef $refName
$token = Get-GitHubToken
$headers = New-GitHubHeaders -Token $token
$dispatchStart = [datetimeoffset]::UtcNow.AddSeconds(-5)

Info ("dispatching Full Integration (Self-Hosted) for {0}@{1}" -f $refName, $headSha)
$body = @{
  ref = $refName
  inputs = @{
    owner = $Owner
    env_file = $EnvFile
    ci_profile = $CiProfile
    run_full_integration = $(if ($RunFullIntegration) { "true" } else { "false" })
  }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod `
  -Method Post `
  -Uri "https://api.github.com/repos/$repoSlug/actions/workflows/full-integration-self-hosted.yml/dispatches" `
  -Headers $headers `
  -Body $body | Out-Null

$run = Find-DispatchedRun `
  -RepoSlug $repoSlug `
  -Headers $headers `
  -RefName $refName `
  -HeadSha $headSha `
  -NotBeforeUtc $dispatchStart `
  -TimeoutSeconds $DiscoverTimeoutSeconds `
  -PollIntervalSeconds ([Math]::Max(5, $PollSeconds))

Info ("dispatched run id={0}" -f $run.id)
Info ("run url: {0}" -f $run.html_url)

if ($Wait) {
  $run = Wait-WorkflowRun `
    -RepoSlug $repoSlug `
    -Headers $headers `
    -RunId $run.id `
    -TimeoutMinutes $RunTimeoutMinutes `
    -PollIntervalSeconds ([Math]::Max(10, $PollSeconds))
  if ($run.conclusion -ne "success") {
    throw ("workflow run {0} finished with conclusion={1}: {2}" -f $run.id, $run.conclusion, $run.html_url)
  }
  Ok ("workflow run completed successfully: {0}" -f $run.html_url)
} else {
  Ok ("workflow run queued: {0}" -f $run.html_url)
}

[pscustomobject]@{
  Repo = $repoSlug
  Ref = $refName
  HeadSha = $headSha
  RunId = [long]$run.id
  Status = $run.status
  Conclusion = $run.conclusion
  Url = $run.html_url
}
