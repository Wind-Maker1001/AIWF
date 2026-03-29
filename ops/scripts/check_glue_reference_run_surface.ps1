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

$glueAppPath = Join-Path $RepoRoot "apps\glue-python\app.py"
$jobServicePath = Join-Path $RepoRoot "apps\base-java\src\main\java\com\aiwf\base\service\JobService.java"
$glueGatewayPath = Join-Path $RepoRoot "apps\base-java\src\main\java\com\aiwf\base\glue\GlueGateway.java"
$glueClientPath = Join-Path $RepoRoot "apps\base-java\src\main\java\com\aiwf\base\glue\GlueClient.java"
$difyControllerPath = Join-Path $RepoRoot "apps\base-java\src\main\java\com\aiwf\base\web\DifyController.java"

$glueApp = Get-FileText $glueAppPath
$jobService = Get-FileText $jobServicePath
$glueGateway = Get-FileText $glueGatewayPath
$glueClient = Get-FileText $glueClientPath
$difyController = Get-FileText $difyControllerPath

if ($glueApp -notmatch '@app\.post\("/jobs/\{job_id\}/run-reference"\)') {
  Add-Issue("glue run-reference route missing")
}
if ($glueApp -notmatch 'get_workflow_version\(version_id\)') {
  Add-Issue("glue run-reference no longer resolves version references from governance workflow version store")
}
if ($glueApp -notmatch 'workflow_reference_run_v1\(') {
  Add-Issue("glue run-reference no longer calls Rust execution surface")
}
if ($glueApp -notmatch 'final_output = rust_out\.get\("final_output"\)' -or $glueApp -notmatch 'effective_output = final_output if final_output else execution') {
  Add-Issue("glue run-reference no longer treats final_output as the canonical terminal output")
}
if ($glueApp -notmatch 'workflow_reference bridge has been retired') {
  Add-Issue("glue legacy /run/{flow} route no longer rejects workflow_reference bridge")
}
if ($glueApp -match 'if version_id == "cleaning"') {
  Add-Issue("glue run-reference still contains version_id=cleaning compatibility mapping")
}
if ($glueApp -match 'workflow_id == "cleaning"' -or $glueApp -match 'run_cleaning_flow\(job_id, runner_req\)') {
  Add-Issue("glue run-reference still dispatches execution based on workflow_definition.workflow_id")
}
if ($glueGateway -notmatch 'GlueRunResult runReference\(String jobId, GlueRunReferenceReq request\);') {
  Add-Issue("GlueGateway missing runReference contract")
}
if ($glueClient -notmatch 'uri\("/jobs/\{jobId\}/run-reference", jobId\)') {
  Add-Issue("GlueClient no longer targets /jobs/{jobId}/run-reference")
}
if ($jobService -notmatch 'glue\.runReference\(jobId, new GlueRunReferenceReq\(') {
  Add-Issue("JobService runWorkflowReference no longer dispatches via glue.runReference")
}
if ($jobService -match 'glue\.runFlow\(jobId,\s*"workflow_reference"') {
  Add-Issue("JobService still dispatches reference runs through workflow_reference flow bridge")
}
if ($difyController -notmatch 'jobs\.runWorkflowReference\(') {
  Add-Issue("DifyController no longer uses runWorkflowReference")
}
if ($difyController -match 'jobs\.runFlow\(') {
  Add-Issue("DifyController still depends on legacy runFlow bridge")
}

if ($issues.Count -gt 0) {
  throw ($issues -join [Environment]::NewLine)
}

Ok "glue reference-first run surface converged"
