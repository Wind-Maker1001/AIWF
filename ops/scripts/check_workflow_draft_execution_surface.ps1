param(
  [string]$RepoRoot = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}

$issues = New-Object System.Collections.Generic.List[string]

function Add-Issue([string]$Message) {
  $issues.Add($Message)
}

function Get-FileText([string]$Path) {
  if (-not (Test-Path $Path)) {
    Add-Issue("missing file: $Path")
    return ""
  }
  return Get-Content $Path -Raw -Encoding UTF8
}

$executionServicePath = Join-Path $RepoRoot "apps\dify-desktop\workflow_execution_service.js"
$mainIpcPath = Join-Path $RepoRoot "apps\dify-desktop\main_ipc_workflow.js"
$runIpcPath = Join-Path $RepoRoot "apps\dify-desktop\workflow_ipc_run.js"
$queueAppsPath = Join-Path $RepoRoot "apps\dify-desktop\workflow_ipc_queue_apps.js"
$reviewIpcPath = Join-Path $RepoRoot "apps\dify-desktop\workflow_ipc_review.js"

$executionService = Get-FileText $executionServicePath
$mainIpc = Get-FileText $mainIpcPath
$runIpc = Get-FileText $runIpcPath
$queueApps = Get-FileText $queueAppsPath
$reviewIpc = Get-FileText $reviewIpcPath

if ($executionService -notmatch '/operators/workflow_draft_run_v1') {
  Add-Issue("workflow_execution_service no longer targets /operators/workflow_draft_run_v1")
}
if ($mainIpc -notmatch 'createWorkflowExecutionSupport') {
  Add-Issue("main_ipc_workflow no longer constructs workflowExecutionSupport")
}
if ($runIpc -notmatch 'executeDraftWorkflowAuthoritatively') {
  Add-Issue("workflow_ipc_run main path no longer routes draft execution through workflowExecutionSupport")
}
if ($queueApps -notmatch 'executeDraftWorkflowAuthoritatively' -and $queueApps -notmatch 'executeReferenceWorkflowAuthoritatively') {
  Add-Issue("workflow_ipc_queue_apps no longer routes execution through workflowExecutionSupport")
}
if ($reviewIpc -notmatch 'executeDraftWorkflowAuthoritatively' -and $reviewIpc -notmatch 'executeReferenceWorkflowAuthoritatively') {
  Add-Issue("workflow_ipc_review replay/auto-resume no longer routes execution through workflowExecutionSupport")
}

if ($issues.Count -gt 0) {
  throw ($issues -join [Environment]::NewLine)
}

Ok "workflow draft execution surface converged to Rust-first owner"
