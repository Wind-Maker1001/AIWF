param(
  [string]$RuleFile,
  [switch]$ListTemplates
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Info($m){ Write-Host "[INFO] $m" -ForegroundColor Cyan }
function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }
function Fail($m){ throw "[FAIL] $m" }

$repoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$templatesDir = Join-Path $repoRoot "rules\templates"

if ($ListTemplates) {
  if (-not (Test-Path $templatesDir)) {
    Fail "templates directory not found: $templatesDir"
  }
  Info "available rule templates:"
  Get-ChildItem -Path $templatesDir -File | ForEach-Object { Write-Host " - $($_.FullName)" }
  exit 0
}

if (-not $RuleFile) {
  Fail "please provide -RuleFile <path> or use -ListTemplates"
}
if (-not (Test-Path $RuleFile)) {
  Fail "rule file not found: $RuleFile"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  Fail "python not found in PATH"
}

$pythonScript = @'
import json
import os
import sys
from pathlib import Path

rule_file = Path(sys.argv[1])
repo_root = Path(sys.argv[2])
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

try:
    from aiwf.flows.cleaning import validate_cleaning_rules
except Exception as e:
    print(json.dumps({"ok": False, "errors": [f"cannot import validate_cleaning_rules: {e}"], "warnings": []}, ensure_ascii=False))
    sys.exit(0)

text = rule_file.read_text(encoding="utf-8")
data = None

suffix = rule_file.suffix.lower()
if suffix in [".yaml", ".yml"]:
    try:
        import yaml  # type: ignore
    except Exception as e:
        print(json.dumps({"ok": False, "errors": [f"yaml support requires pyyaml: {e}"], "warnings": []}, ensure_ascii=False))
        sys.exit(0)
    data = yaml.safe_load(text)
else:
    try:
        data = json.loads(text)
    except Exception as e:
        print(json.dumps({"ok": False, "errors": [f"invalid json: {e}"], "warnings": []}, ensure_ascii=False))
        sys.exit(0)

if not isinstance(data, dict):
    print(json.dumps({"ok": False, "errors": ["rule file must contain an object"], "warnings": []}, ensure_ascii=False))
    sys.exit(0)

res = validate_cleaning_rules(data)
print(json.dumps(res, ensure_ascii=False))
'@

$tmpPy = Join-Path $env:TEMP "aiwf_validate_rules.py"
Set-Content -Path $tmpPy -Value $pythonScript -Encoding UTF8

try {
  $raw = & python $tmpPy $RuleFile $repoRoot
  if ($LASTEXITCODE -ne 0) {
    Fail "python validation script execution failed"
  }
  $res = $raw | ConvertFrom-Json
  if ($res.warnings -and $res.warnings.Count -gt 0) {
    foreach ($w in $res.warnings) { Warn $w }
  }
  if (-not $res.ok) {
    foreach ($e in $res.errors) { Write-Host " - $e" -ForegroundColor Red }
    Fail "rules validation failed"
  }
  Ok "rules validation passed"
}
finally {
  Remove-Item -Path $tmpPy -ErrorAction SilentlyContinue
}
