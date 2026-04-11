param(
  [string]$OpenApiFile = "",
  [string]$RustClientFile = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $OpenApiFile) {
  $OpenApiFile = Join-Path $root "contracts\rust\openapi.v2.yaml"
}
if (-not $RustClientFile) {
  $RustClientFile = Join-Path $root "apps\glue-python\aiwf\rust_client.py"
}
if (-not (Test-Path $OpenApiFile)) { throw "openapi file not found: $OpenApiFile" }
if (-not (Test-Path $RustClientFile)) { throw "rust client file not found: $RustClientFile" }

$openapi = Get-Content $OpenApiFile -Raw
$client = Get-Content $RustClientFile -Raw

$requiredPaths = @(
  "/health",
  "/metrics",
  "/admin/reload_runtime_config",
  "/operators/compute_metrics",
  "/operators/transform_rows_v2",
  "/operators/transform_rows_v3",
  "/operators/transform_rows_v2/stream",
  "/operators/postprocess_rows_v1",
  "/operators/quality_check_v1",
  "/operators/quality_check_v2",
  "/operators/aggregate_pushdown_v1",
  "/operators/plugin_exec_v1",
  "/operators/transform_rows_v2/submit",
  "/tasks/{task_id}",
  "/tasks/{task_id}/cancel",
  "/operators/text_preprocess_v2",
  "/operators/join_rows_v1",
  "/operators/normalize_schema_v1",
  "/operators/entity_extract_v1",
  "/operators/aggregate_rows_v1",
  "/operators/rules_compile_v1",
  "/operators/rules_package_v1/publish",
  "/operators/rules_package_v1/get",
  "/operators/load_rows_v1",
  "/operators/save_rows_v1",
  "/operators/stream_window_v2",
  "/operators/parquet_io_v2",
  "/workflow/run"
)
foreach($p in $requiredPaths){
  if ($openapi -notmatch [regex]::Escape($p)) {
    throw "openapi missing required path: $p"
  }
}

$requiredClientFns = @(
  "def health(",
  "def get_metrics(",
  "def reload_runtime_config(",
  "def compute_metrics(",
  "def transform_rows_v2(",
  "def transform_rows_v3(",
  "def transform_rows_v2_stream(",
  "def postprocess_rows_v1(",
  "def quality_check_v1(",
  "def quality_check_v2(",
  "def aggregate_pushdown_v1(",
  "def plugin_exec_v1(",
  "def submit_transform_rows_v2(",
  "def get_task(",
  "def cancel_task(",
  "def text_preprocess_v2(",
  "def join_rows_v1(",
  "def normalize_schema_v1(",
  "def entity_extract_v1(",
  "def aggregate_rows_v1(",
  "def rules_compile_v1(",
  "def rules_package_publish_v1(",
  "def rules_package_get_v1(",
  "def load_rows_v1(",
  "def save_rows_v1(",
  "def stream_window_v2(",
  "def parquet_io_v2(",
  "def workflow_run("
)
foreach($f in $requiredClientFns){
  if ($client -notmatch [regex]::Escape($f)) {
    throw "rust client missing required function: $f"
  }
}

if ($openapi -notmatch "components:\s*schemas:") {
  throw "openapi components.schemas section missing"
}
if ($openapi -notmatch "TaskState:") {
  throw "openapi schema missing: TaskState"
}
if ($openapi -notmatch "TransformRowsResp:") {
  throw "openapi schema missing: TransformRowsResp"
}
if ($openapi -notmatch "PostprocessRowsV1Req:") {
  throw "openapi schema missing: PostprocessRowsV1Req"
}
if ($openapi -notmatch "QualityCheckV2Req:") {
  throw "openapi schema missing: QualityCheckV2Req"
}
if ($openapi -notmatch "StreamWindowV2Req:") {
  throw "openapi schema missing: StreamWindowV2Req"
}
if ($openapi -notmatch "StreamWindowV2Resp:") {
  throw "openapi schema missing: StreamWindowV2Resp"
}
if ($openapi -notmatch "ParquetIoV2Req:") {
  throw "openapi schema missing: ParquetIoV2Req"
}
if ($openapi -notmatch "ParquetIoV2Resp:") {
  throw "openapi schema missing: ParquetIoV2Resp"
}

Ok "openapi and rust client sync check passed"
