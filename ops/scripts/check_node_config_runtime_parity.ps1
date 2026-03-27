param(
  [string]$RepoRoot = "",
  [string[]]$RequiredParityNodeTypes = @(
    "ingest_files",
    "clean_md",
    "ds_refine",
    "load_rows_v2",
    "quality_check_v2",
    "lineage_v2",
    "manual_review",
    "ai_refine",
    "ai_strategy_v1",
    "ai_audit",
    "join_rows_v2",
    "join_rows_v3",
    "join_rows_v4",
    "aggregate_rows_v2",
    "aggregate_rows_v3",
    "aggregate_rows_v4",
    "window_rows_v1",
    "sql_chart_v1",
    "load_rows_v3",
    "quality_check_v3",
    "quality_check_v4",
    "office_slot_fill_v1",
    "optimizer_v1",
    "parquet_io_v2",
    "plugin_registry_v1",
    "transform_rows_v3",
    "lineage_v3",
    "rule_simulator_v1",
    "constraint_solver_v1",
    "udf_wasm_v2"
  )
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Normalize-ErrorItems($Values) {
  return @(
    @($Values | ForEach-Object {
      $path = [string]$_.path
      $code = [string]$_.code
      if ([string]::IsNullOrWhiteSpace($path) -and [string]::IsNullOrWhiteSpace($code)) {
        return $null
      }
      return "$($path.Trim())::$($code.Trim())"
    } | Where-Object { -not [string]::IsNullOrWhiteSpace($_) }) | Sort-Object -Unique
  )
}

if (-not $RepoRoot) {
  $RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
}
if (-not (Get-Command node -ErrorAction SilentlyContinue)) {
  throw "node not found in PATH"
}
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
  throw "python not found in PATH"
}

$nodeScript = @'
const fs = require("fs");
const path = require("path");
const { pathToFileURL } = require("url");

function uniqueSorted(values) {
  return Array.from(new Set((Array.isArray(values) ? values : []).map((item) => String(item || "").trim()).filter(Boolean))).sort();
}

function normalizeErrorItems(values) {
  return uniqueSorted((Array.isArray(values) ? values : []).map((item) => {
    const path = String(item?.path || "").trim();
    const code = String(item?.code || "").trim();
    return `${path}::${code}`;
  }));
}

function escapeRegExp(value) {
  return String(value || "").replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function loadFixturePayload(fixturePath) {
  const payload = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  if (String(payload?.schema_version || "").trim() !== "node_config_contract_fixtures.v1") {
    throw new Error(`unexpected fixture schema_version: ${String(payload?.schema_version || "").trim() || "<missing>"}`);
  }
  if (!Array.isArray(payload?.required_node_types) || payload.required_node_types.length === 0) {
    throw new Error("node config parity fixtures missing required_node_types");
  }
  if (!Array.isArray(payload?.nodes) || payload.nodes.length === 0) {
    throw new Error("node config parity fixtures missing nodes");
  }
  return payload;
}

function flattenFixtureCases(payload) {
  const entries = Array.isArray(payload?.nodes) ? payload.nodes : [];
  const cases = [];
  for (const entry of entries) {
    const nodeType = String(entry?.type || "").trim();
    if (!nodeType) {
      throw new Error("node config parity fixture node missing type");
    }
    const validCases = Array.isArray(entry?.valid_cases) ? entry.valid_cases : [];
    const invalidCases = Array.isArray(entry?.invalid_cases) ? entry.invalid_cases : [];
    if (validCases.length === 0 || invalidCases.length === 0) {
      throw new Error(`node config parity fixture node ${nodeType} requires both valid_cases and invalid_cases`);
    }
    for (const item of validCases) {
      const id = String(item?.id || "").trim();
      if (!id) throw new Error(`node config parity valid case missing id for ${nodeType}`);
      cases.push({
        id,
        node_type: nodeType,
        expected_ok: true,
        config: item?.config || {},
        expected_error_contains: [],
        expected_error_items: [],
      });
    }
    for (const item of invalidCases) {
      const id = String(item?.id || "").trim();
      if (!id) throw new Error(`node config parity invalid case missing id for ${nodeType}`);
      const expectedErrorContains = uniqueSorted(item?.expected_error_contains || []);
      const expectedErrorItems = Array.isArray(item?.expected_error_items) ? item.expected_error_items : [];
      if (expectedErrorContains.length === 0 && expectedErrorItems.length === 0) {
        throw new Error(`node config parity invalid case ${id} missing expected_error expectation`);
      }
      cases.push({
        id,
        node_type: nodeType,
        expected_ok: false,
        config: item?.config || {},
        expected_error_contains: expectedErrorContains,
        expected_error_items: expectedErrorItems,
      });
    }
  }
  return cases;
}

async function main() {
  const repoRoot = process.argv[2];
  const fixturePath = path.join(repoRoot, "contracts", "desktop", "node_config_contract_fixtures.v1.json");
  const errorContractPath = path.join(repoRoot, "contracts", "desktop", "node_config_validation_errors.v1.json");
  const payload = loadFixturePayload(fixturePath);
  const errorContract = JSON.parse(fs.readFileSync(errorContractPath, "utf8"));
  const declaredErrorCodes = uniqueSorted((Array.isArray(errorContract?.codes) ? errorContract.codes : []).map((item) => String(item?.code || "").trim()));
  const {
    WORKFLOW_SCHEMA_VERSION,
    assertWorkflowContract,
    NODE_CONFIG_VALIDATION_ERROR_CODES,
  } = await import(pathToFileURL(path.join(repoRoot, "apps", "dify-desktop", "renderer", "workflow", "workflow-contract.js")).href);
  const cases = flattenFixtureCases(payload);

  const results = cases.map((fixtureCase) => {
    let actualOk = false;
    let errors = [];
    let errorItems = [];
    try {
      const result = assertWorkflowContract({
        workflow_id: "wf_parity_fixture",
        version: WORKFLOW_SCHEMA_VERSION,
        nodes: [{ id: "n1", type: fixtureCase.node_type, config: fixtureCase.config }],
        edges: [],
      });
      actualOk = !!result?.ok;
    } catch (error) {
      errors = Array.isArray(error?.details?.errors)
        ? error.details.errors.map((item) => String(item || "").trim()).filter(Boolean)
        : [String(error?.message || error || "").trim()].filter(Boolean);
      errorItems = Array.isArray(error?.details?.error_items)
        ? error.details.error_items.map((item) => ({
            path: String(item?.path || "").trim(),
            code: String(item?.code || "").trim(),
          })).filter((item) => item.path || item.code)
        : [];
    }
    const missingExpectedErrors = fixtureCase.expected_error_contains.filter((item) => !new RegExp(escapeRegExp(item)).test(errors.join("\n")));
    const missingExpectedErrorItems = fixtureCase.expected_error_items.filter((expected) => !errorItems.some((item) => item.path === String(expected?.path || "").trim() && item.code === String(expected?.code || "").trim()));
    return {
      id: fixtureCase.id,
      node_type: fixtureCase.node_type,
      expected_ok: fixtureCase.expected_ok,
      actual_ok: actualOk,
      errors,
      error_items: errorItems,
      missing_expected_errors: missingExpectedErrors,
      missing_expected_error_items: missingExpectedErrorItems,
    };
  });

  console.log(JSON.stringify({
    fixture_path: fixturePath,
    error_contract_path: errorContractPath,
    required_node_types: uniqueSorted(payload.required_node_types),
    fixture_node_types: uniqueSorted(payload.nodes.map((item) => String(item?.type || "").trim())),
    declared_error_codes: declaredErrorCodes,
    js_validation_error_codes: uniqueSorted(NODE_CONFIG_VALIDATION_ERROR_CODES || []),
    fixture_expected_error_codes: uniqueSorted(cases.flatMap((item) => Array.isArray(item.expected_error_items) ? item.expected_error_items.map((errorItem) => String(errorItem?.code || "").trim()) : [])),
    valid_case_count: results.filter((item) => item.expected_ok).length,
    invalid_case_count: results.filter((item) => !item.expected_ok).length,
    results,
  }));
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : String(error));
  process.exit(1);
});
'@

$pythonScript = @'
import json
import os
import sys
from pathlib import Path


def unique_sorted(values):
    return sorted({str(value).strip() for value in values if str(value).strip()})


repo_root = Path(sys.argv[1]).resolve()
os.environ["AIWF_REPO_ROOT"] = str(repo_root)
sys.path.insert(0, str(repo_root / "apps" / "glue-python"))

from aiwf.node_config_contract_runtime import VALIDATION_ERROR_CODES, build_validation_error_items, validate_workflow_graph_node_configs

fixture_path = repo_root / "contracts" / "desktop" / "node_config_contract_fixtures.v1.json"
error_contract_path = repo_root / "contracts" / "desktop" / "node_config_validation_errors.v1.json"
payload = json.loads(fixture_path.read_text(encoding="utf-8"))
error_contract = json.loads(error_contract_path.read_text(encoding="utf-8"))
if str(payload.get("schema_version") or "").strip() != "node_config_contract_fixtures.v1":
    raise RuntimeError(f"unexpected fixture schema_version: {str(payload.get('schema_version') or '').strip() or '<missing>'}")

results = []
valid_case_count = 0
invalid_case_count = 0
for entry in payload.get("nodes", []):
    node_type = str(entry.get("type") or "").strip()
    for item in entry.get("valid_cases", []):
        valid_case_count += 1
        errors = validate_workflow_graph_node_configs(
            {
                "workflow_id": "wf_parity_fixture",
                "version": "workflow.v1",
                "nodes": [{"id": "n1", "type": node_type, "config": item.get("config") or {}}],
                "edges": [],
            },
            label_prefix="workflow",
        )
        results.append(
            {
                "id": str(item.get("id") or "").strip(),
                "node_type": node_type,
                "expected_ok": True,
                "actual_ok": len(errors) == 0,
                "errors": errors,
                "error_items": build_validation_error_items(errors),
                "missing_expected_errors": [],
                "missing_expected_error_items": [],
            }
        )
    for item in entry.get("invalid_cases", []):
        invalid_case_count += 1
        expected_error_contains = unique_sorted(item.get("expected_error_contains") or [])
        expected_error_items = item.get("expected_error_items") or []
        errors = validate_workflow_graph_node_configs(
            {
                "workflow_id": "wf_parity_fixture",
                "version": "workflow.v1",
                "nodes": [{"id": "n1", "type": node_type, "config": item.get("config") or {}}],
                "edges": [],
            },
            label_prefix="workflow",
        )
        error_items = build_validation_error_items(errors)
        missing_expected_errors = [expected for expected in expected_error_contains if expected not in "\n".join(errors)]
        missing_expected_error_items = [
            expected
            for expected in expected_error_items
            if not any(
                str(actual.get("path") or "").strip() == str(expected.get("path") or "").strip()
                and str(actual.get("code") or "").strip() == str(expected.get("code") or "").strip()
                for actual in error_items
            )
        ]
        results.append(
            {
                "id": str(item.get("id") or "").strip(),
                "node_type": node_type,
                "expected_ok": False,
                "actual_ok": len(errors) == 0,
                "errors": errors,
                "error_items": error_items,
                "missing_expected_errors": missing_expected_errors,
                "missing_expected_error_items": missing_expected_error_items,
            }
        )

print(
    json.dumps(
        {
            "fixture_path": str(fixture_path),
            "error_contract_path": str(error_contract_path),
            "required_node_types": unique_sorted(payload.get("required_node_types") or []),
            "fixture_node_types": unique_sorted([str(item.get("type") or "").strip() for item in payload.get("nodes", [])]),
            "declared_error_codes": unique_sorted([str(item.get("code") or "").strip() for item in error_contract.get("codes", [])]),
            "python_validation_error_codes": unique_sorted(VALIDATION_ERROR_CODES),
            "valid_case_count": valid_case_count,
            "invalid_case_count": invalid_case_count,
            "results": results,
        },
        ensure_ascii=False,
    )
)
'@

$nodeOutput = $nodeScript | node - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "node config runtime parity JS evaluation failed"
}
$pythonOutput = $pythonScript | python - $RepoRoot
if ($LASTEXITCODE -ne 0) {
  throw "node config runtime parity Python evaluation failed"
}

$nodeSummary = [string]$nodeOutput | ConvertFrom-Json
$pythonSummary = [string]$pythonOutput | ConvertFrom-Json
$fixturePath = Join-Path $RepoRoot "contracts\desktop\node_config_contract_fixtures.v1.json"
$fixtureSchemaPath = Join-Path $RepoRoot "contracts\desktop\node_config_contract_fixtures.schema.json"
$errorContractPath = Join-Path $RepoRoot "contracts\desktop\node_config_validation_errors.v1.json"
$errorContractSchemaPath = Join-Path $RepoRoot "contracts\desktop\node_config_validation_errors.schema.json"
$contractPath = Join-Path $RepoRoot "contracts\desktop\node_config_contracts.v1.json"

if (-not (Test-Path $fixturePath)) {
  throw "node config parity fixture file missing: $fixturePath"
}
if (-not (Test-Path $fixtureSchemaPath)) {
  throw "node config parity fixture schema file missing: $fixtureSchemaPath"
}
if (-not (Test-Path $errorContractPath)) {
  throw "node config validation error contract file missing: $errorContractPath"
}
if (-not (Test-Path $errorContractSchemaPath)) {
  throw "node config validation error contract schema file missing: $errorContractSchemaPath"
}
if (-not (Test-Path $contractPath)) {
  throw "node config contract file missing: $contractPath"
}

$fixturePayload = Get-Content $fixturePath -Raw | ConvertFrom-Json
$contractPayload = Get-Content $contractPath -Raw | ConvertFrom-Json
$requiredFixtureTypes = @($RequiredParityNodeTypes | Where-Object { -not [string]::IsNullOrWhiteSpace($_) } | Sort-Object -Unique)
$fixtureTypes = @($fixturePayload.nodes | ForEach-Object { [string]$_.type } | Where-Object { $_ } | Sort-Object -Unique)
$contractTypes = @($contractPayload.nodes | ForEach-Object { [string]$_.type } | Where-Object { $_ } | Sort-Object -Unique)
$fixtureTypesMissingFromContract = @($fixtureTypes | Where-Object { $contractTypes -notcontains $_ })
$missingRequiredFixtureTypes = @($requiredFixtureTypes | Where-Object { $fixtureTypes -notcontains $_ })
$declaredErrorCodes = @($nodeSummary.declared_error_codes | Where-Object { $_ } | Sort-Object -Unique)
$jsValidationErrorCodes = @($nodeSummary.js_validation_error_codes | Where-Object { $_ } | Sort-Object -Unique)
$pythonValidationErrorCodes = @($pythonSummary.python_validation_error_codes | Where-Object { $_ } | Sort-Object -Unique)
$fixtureExpectedErrorCodes = @($nodeSummary.fixture_expected_error_codes | Where-Object { $_ } | Sort-Object -Unique)
$fixtureUndeclaredErrorCodes = @($fixtureExpectedErrorCodes | Where-Object { $declaredErrorCodes -notcontains $_ })
$jsUndeclaredErrorCodes = @($jsValidationErrorCodes | Where-Object { $declaredErrorCodes -notcontains $_ })
$pythonUndeclaredErrorCodes = @($pythonValidationErrorCodes | Where-Object { $declaredErrorCodes -notcontains $_ })
$declaredMissingInJs = @($declaredErrorCodes | Where-Object { $jsValidationErrorCodes -notcontains $_ })
$declaredMissingInPython = @($declaredErrorCodes | Where-Object { $pythonValidationErrorCodes -notcontains $_ })

$jsResultsById = @{}
foreach ($result in @($nodeSummary.results)) {
  $jsResultsById[[string]$result.id] = $result
}
$pythonResultsById = @{}
foreach ($result in @($pythonSummary.results)) {
  $pythonResultsById[[string]$result.id] = $result
}

$caseIds = @($jsResultsById.Keys + $pythonResultsById.Keys | Sort-Object -Unique)
$jsExpectationFailures = @()
$pythonExpectationFailures = @()
$runtimeStatusDrift = @()
$runtimeErrorItemDrift = @()
$runtimeErrorDrift = @()
$missingJsCases = @()
$missingPythonCases = @()

foreach ($caseId in $caseIds) {
  $jsCase = if ($jsResultsById.ContainsKey($caseId)) { $jsResultsById[$caseId] } else { $null }
  $pythonCase = if ($pythonResultsById.ContainsKey($caseId)) { $pythonResultsById[$caseId] } else { $null }
  if ($null -eq $jsCase) {
    $missingJsCases += $caseId
    continue
  }
  if ($null -eq $pythonCase) {
    $missingPythonCases += $caseId
    continue
  }
  if (([bool]$jsCase.actual_ok -ne [bool]$jsCase.expected_ok) -or @($jsCase.missing_expected_errors).Count -gt 0 -or @($jsCase.missing_expected_error_items).Count -gt 0) {
    $jsExpectationFailures += $caseId
  }
  if (([bool]$pythonCase.actual_ok -ne [bool]$pythonCase.expected_ok) -or @($pythonCase.missing_expected_errors).Count -gt 0 -or @($pythonCase.missing_expected_error_items).Count -gt 0) {
    $pythonExpectationFailures += $caseId
  }
  if ([bool]$jsCase.actual_ok -ne [bool]$pythonCase.actual_ok) {
    $runtimeStatusDrift += $caseId
  }
  $jsErrorItems = Normalize-ErrorItems @($jsCase.error_items)
  $pythonErrorItems = Normalize-ErrorItems @($pythonCase.error_items)
  if ((ConvertTo-Json $jsErrorItems -Compress) -ne (ConvertTo-Json $pythonErrorItems -Compress)) {
    $runtimeErrorItemDrift += $caseId
  }
  $jsErrors = @($jsCase.errors | ForEach-Object { [string]$_ })
  $pythonErrors = @($pythonCase.errors | ForEach-Object { [string]$_ })
  if ((ConvertTo-Json $jsErrors -Compress) -ne (ConvertTo-Json $pythonErrors -Compress)) {
    $runtimeErrorDrift += $caseId
  }
}

$issues = @()
if ($missingRequiredFixtureTypes.Count -gt 0) {
  $issues += "missing required parity fixture node types: $($missingRequiredFixtureTypes -join ', ')"
}
if ($fixtureTypesMissingFromContract.Count -gt 0) {
  $issues += "parity fixture node types missing from node config contract: $($fixtureTypesMissingFromContract -join ', ')"
}
if ($fixtureUndeclaredErrorCodes.Count -gt 0) {
  $issues += "fixture expected error codes missing from validation error contract: $($fixtureUndeclaredErrorCodes -join ', ')"
}
if ($jsUndeclaredErrorCodes.Count -gt 0) {
  $issues += "js runtime error codes missing from validation error contract: $($jsUndeclaredErrorCodes -join ', ')"
}
if ($pythonUndeclaredErrorCodes.Count -gt 0) {
  $issues += "python runtime error codes missing from validation error contract: $($pythonUndeclaredErrorCodes -join ', ')"
}
if ($declaredMissingInJs.Count -gt 0) {
  $issues += "validation error contract codes missing from js runtime: $($declaredMissingInJs -join ', ')"
}
if ($declaredMissingInPython.Count -gt 0) {
  $issues += "validation error contract codes missing from python runtime: $($declaredMissingInPython -join ', ')"
}
if ($missingJsCases.Count -gt 0) {
  $issues += "js runtime parity cases missing: $($missingJsCases -join ', ')"
}
if ($missingPythonCases.Count -gt 0) {
  $issues += "python runtime parity cases missing: $($missingPythonCases -join ', ')"
}
if ($jsExpectationFailures.Count -gt 0) {
  $issues += "js runtime parity expectation failures: $($jsExpectationFailures -join ', ')"
}
if ($pythonExpectationFailures.Count -gt 0) {
  $issues += "python runtime parity expectation failures: $($pythonExpectationFailures -join ', ')"
}
if ($runtimeStatusDrift.Count -gt 0) {
  $issues += "js/python runtime parity status drift: $($runtimeStatusDrift -join ', ')"
}
if ($runtimeErrorItemDrift.Count -gt 0) {
  $issues += "js/python runtime parity structured error drift: $($runtimeErrorItemDrift -join ', ')"
}

$payload = [ordered]@{
  status = if ($issues.Count -gt 0) { "failed" } else { "passed" }
  fixturePath = $fixturePath
  fixtureSchemaPath = $fixtureSchemaPath
  errorContractPath = $errorContractPath
  errorContractSchemaPath = $errorContractSchemaPath
  contractPath = $contractPath
  fixtureTypeCount = @($fixtureTypes).Count
  requiredParityNodeTypes = @($requiredFixtureTypes)
  declaredErrorCodeCount = @($declaredErrorCodes).Count
  validCaseCount = [int]$nodeSummary.valid_case_count
  invalidCaseCount = [int]$nodeSummary.invalid_case_count
  caseCount = [int]$nodeSummary.valid_case_count + [int]$nodeSummary.invalid_case_count
  drift = [ordered]@{
    missingRequiredFixtureTypes = @($missingRequiredFixtureTypes)
    fixtureTypesMissingFromContract = @($fixtureTypesMissingFromContract)
    fixtureUndeclaredErrorCodes = @($fixtureUndeclaredErrorCodes)
    jsUndeclaredErrorCodes = @($jsUndeclaredErrorCodes)
    pythonUndeclaredErrorCodes = @($pythonUndeclaredErrorCodes)
    declaredMissingInJs = @($declaredMissingInJs)
    declaredMissingInPython = @($declaredMissingInPython)
    missingJsCases = @($missingJsCases)
    missingPythonCases = @($missingPythonCases)
    jsExpectationFailures = @($jsExpectationFailures)
    pythonExpectationFailures = @($pythonExpectationFailures)
    runtimeStatusDrift = @($runtimeStatusDrift)
    runtimeErrorItemDrift = @($runtimeErrorItemDrift)
    runtimeErrorDrift = @($runtimeErrorDrift)
  }
  issues = @($issues)
}

$payload | ConvertTo-Json -Depth 6 -Compress
if ($payload.status -ne "passed") {
  throw "node config runtime parity checks failed"
}

Ok "node config runtime parity checks passed"
