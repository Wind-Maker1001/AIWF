param(
  [string]$OpenApiFile = "",
  [string]$OutPy = ""
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
if (-not $OpenApiFile) { $OpenApiFile = Join-Path $root "contracts\rust\openapi.v2.yaml" }
if (-not $OutPy) { $OutPy = Join-Path $root "apps\glue-python\aiwf\rust_client.generated.py" }

python (Join-Path $PSScriptRoot "generate_rust_client.py") $OpenApiFile $OutPy
if ($LASTEXITCODE -ne 0) { throw "generate rust client failed" }
Write-Host "[ OK ] generated $OutPy" -ForegroundColor Green
