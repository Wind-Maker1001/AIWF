param(
  [Parameter(Mandatory = $true)][string]$SpecFile
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Fail($m){ throw "[FAIL] $m" }

if (-not (Test-Path $SpecFile)) {
  Fail "spec file not found: $SpecFile"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Fail "python not found in PATH"
}

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$tmpPy = Join-Path $env:TEMP "aiwf_validate_preprocess.py"
$py = @'
import json
import os
import sys
from pathlib import Path

spec_file = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

from aiwf.preprocess import validate_preprocess_spec

text = spec_file.read_text(encoding="utf-8")
ext = spec_file.suffix.lower()
if ext in [".yaml", ".yml"]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        print(json.dumps({"ok": False, "errors": [f"yaml support requires pyyaml: {e}"], "warnings": []}, ensure_ascii=False))
        raise SystemExit(0)
    data = yaml.safe_load(text)
else:
    data = json.loads(text)

if not isinstance(data, dict):
    print(json.dumps({"ok": False, "errors": ["spec must be object"], "warnings": []}, ensure_ascii=False))
    raise SystemExit(0)

spec = data.get("preprocess") if isinstance(data.get("preprocess"), dict) else data
res = validate_preprocess_spec(spec)
print(json.dumps(res, ensure_ascii=False))
'@
Set-Content -Path $tmpPy -Value $py -Encoding UTF8

try {
  $raw = & python $tmpPy $SpecFile $repoRoot
  if ($LASTEXITCODE -ne 0) { Fail "python execution failed" }
  $res = $raw | ConvertFrom-Json
  if ($res.warnings) {
    foreach ($w in $res.warnings) { Warn $w }
  }
  if (-not $res.ok) {
    foreach ($e in $res.errors) { Write-Host " - $e" -ForegroundColor Red }
    Fail "preprocess spec validation failed"
  }
  Ok "preprocess spec validation passed"
}
finally {
  Remove-Item -Path $tmpPy -ErrorAction SilentlyContinue
}
