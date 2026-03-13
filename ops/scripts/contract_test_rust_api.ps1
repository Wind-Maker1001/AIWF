param(
  [string]$AccelUrl = "http://127.0.0.1:18082"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Get-JsonFieldValue {
  param(
    $Object,
    [string]$Name
  )
  if ($null -eq $Object) { return $null }
  $prop = $Object.PSObject.Properties[$Name]
  if ($null -eq $prop) { return $null }
  return $prop.Value
}
function Invoke-JsonAllowHttpError {
  param(
    [string]$Method = "GET",
    [string]$Uri,
    [string]$ContentType = "application/json",
    [string]$Body = "",
    [int]$TimeoutSec = 20
  )
  try {
    if ($Method -eq "GET") {
      $res = Invoke-RestMethod -Uri $Uri -TimeoutSec $TimeoutSec
    } else {
      $res = Invoke-RestMethod -Method $Method -Uri $Uri -ContentType $ContentType -Body $Body -TimeoutSec $TimeoutSec
    }
    return @{ ok = $true; status_code = 200; body = $res }
  } catch {
    $resp = $_.Exception.Response
    if ($null -eq $resp) { throw }
    $status = 0
    try { $status = [int]$resp.StatusCode } catch {}
    $reader = New-Object System.IO.StreamReader($resp.GetResponseStream())
    $raw = $reader.ReadToEnd()
    $obj = $null
    try { $obj = $raw | ConvertFrom-Json } catch { $obj = @{ raw = $raw } }
    return @{ ok = $false; status_code = $status; body = $obj }
  }
}

Info "contract test: health"
$h = Invoke-RestMethod -Uri "$AccelUrl/health" -TimeoutSec 10
if (-not $h.ok) { throw "health contract failed: ok missing/false" }

Info "contract test: transform_rows_v2"
$contractRunId = "contract-" + [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds()
$body = @{
  run_id = $contractRunId
  idempotency_key = "$contractRunId-submit"
  rows = @(
    @{ id = "1"; amount = "10" },
    @{ id = "2"; amount = "20" }
  )
  rules = @{ casts = @{ id = "int"; amount = "float" } }
  quality_gates = @{ min_output_rows = 1 }
} | ConvertTo-Json -Depth 8
$t = Invoke-RestMethod -Method Post -Uri "$AccelUrl/operators/transform_rows_v2" -ContentType "application/json" -Body $body -TimeoutSec 20
if (-not $t.ok) { throw "transform_rows_v2 contract failed: ok false" }
foreach($k in @("operator","status","trace_id","rows","stats","quality","gate_result")) {
  if (-not $t.PSObject.Properties.Name.Contains($k)) { throw "transform_rows_v2 missing field: $k" }
}

Info "contract test: async submit/poll/cancel shape"
$sub = Invoke-RestMethod -Method Post -Uri "$AccelUrl/operators/transform_rows_v2/submit" -ContentType "application/json" -Body $body -TimeoutSec 20
if (-not $sub.task_id) { throw "submit missing task_id" }
$task = $null
for($i=0; $i -lt 20; $i++) {
  $qr = Invoke-JsonAllowHttpError -Method "GET" -Uri ("$AccelUrl/tasks/{0}" -f $sub.task_id) -TimeoutSec 20
  if ($qr.ok) {
    $task = $qr.body
    break
  }
  $err = $qr.body
  $errCode = Get-JsonFieldValue -Object $err -Name "error"
  $isNotFound = ($qr.status_code -eq 404) -or ($errCode -eq "task_not_found")
  if (-not $isNotFound) {
    throw "tasks query failed: status=$($qr.status_code) body=$($err | ConvertTo-Json -Depth 6 -Compress)"
  }
  Start-Sleep -Milliseconds 150
}
if ($null -eq $task) {
  throw "task lookup did not stabilize in time: $($sub.task_id)"
}
foreach($k in @("task_id","operator","status","created_at","updated_at")) {
  if (-not $task.PSObject.Properties.Name.Contains($k)) { throw "task missing field: $k" }
}
$cancel = Invoke-JsonAllowHttpError -Method "POST" -Uri ("$AccelUrl/tasks/{0}/cancel" -f $sub.task_id) -ContentType "application/json" -Body "{}" -TimeoutSec 20
if (-not $cancel.body) { throw "cancel returned empty response" }
foreach($k in @("ok","task_id","status")) {
  if (-not $cancel.body.PSObject.Properties.Name.Contains($k)) { throw "cancel missing field: $k" }
}

Ok "rust api contract tests passed"
