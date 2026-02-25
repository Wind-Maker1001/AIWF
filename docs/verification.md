# AIWF Verification Guide

This document lists the current verification commands for local development.

## 1. Unit/Module Checks

```powershell
# accel-rust
cd D:\AIWF\apps\accel-rust
cargo check
cargo test -q

# glue-python
cd D:\AIWF\apps\glue-python
python -m unittest discover -s tests -v
```

## 2. Standard End-to-End Smoke

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1
```

Expected high-level signals:
- `run_ok : True`
- `artifacts : 6`
- `[ OK ] smoke test finished`

## 3. Invalid Parquet Fallback Integration Test

Purpose:
- Force accel-rust to return an invalid parquet payload.
- Verify glue-python rejects that parquet and falls back safely.

Direct run:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\test_invalid_parquet_fallback.ps1
```

Or from smoke script:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\smoke_test.ps1 -WithInvalidParquetFallbackTest
```

Expected high-level signals:
- `accel_attempted : True`
- `accel_used_fallback : True`
- `[ OK ] invalid parquet fallback test passed`

## 4. Notes

- The fallback integration script starts temporary accel/glue processes and stops them automatically.
- It uses a dedicated temporary cargo `--target-dir` to avoid binary lock conflicts with long-running local accel processes.

## 5. One-Command Local CI Check

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

Optional skips:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipRustTests
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipPythonTests
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -SkipSmoke
```

## 6. GitHub Actions Workflows

- `.github/workflows/ci.yml`
  - Trigger: push / pull_request
  - Runner: `windows-latest` (GitHub-hosted)
  - Runs: `ci_check.ps1 -SkipSmoke` (unit/module checks only)

- `.github/workflows/full-integration-self-hosted.yml`
  - Trigger: manual (`workflow_dispatch`)
  - Runner: `self-hosted` + `windows`
  - Default: runs `ci_check.ps1 -SkipSmoke` (does not require SQL Server)
  - Optional: set input `run_full_integration=true` to run full `ci_check.ps1` including smoke + invalid parquet fallback integration test
  - Full integration mode should be used only on an environment where SQL Server and required local runtime prerequisites are available.

## 7. Rust Transform Engine Benchmark (row vs columnar)

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\bench_rust_transform.ps1 -Rows 50000 -Runs 3 -Warmup 1 -UpdateProfile
```

Outputs:
- `ops\logs\bench\rust_transform\<timestamp>\benchmark.json`
- `ops\logs\bench\rust_transform\<timestamp>\benchmark_report.md`
- latest shortcuts:
  - `ops\logs\bench\rust_transform\latest.json`
  - `ops\logs\bench\rust_transform\latest.md`
  - `ops\logs\bench\rust_transform\history.jsonl`
- learned profile:
  - `apps\accel-rust\conf\transform_engine_profile.json`
  - safety floors by default: `medium_rows>=20000`, `large_rows>=120000`
  - includes auto routing decision quality:
    - `auto_decision_hit_rate`
    - `auto_decision_hit_samples`

Optional benchmark gate:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_transform_bench_gate.ps1 -Rows 80000 -Runs 3 -Warmup 1 -MinSpeedup 1.02 -MinArrowSpeedup 1.00 -UpdateProfileOnPass
```

Gate behavior (current):
- `large` payload (`Rows >= large_rows_threshold` from profile): enforce strict speed gate
  - `columnar_v1 speedup >= MinSpeedup`
  - `columnar_arrow_v1 speedup >= MinArrowSpeedup`
- `non-large` payload: keep benchmark correctness/runability checks, skip strict speed assertions.

Rationale:
- medium-size data has high jitter across CPU scheduling / background load; hard speed assertions at this tier caused flaky false negatives.
- strict speed assertions are preserved for the large tier where columnar strategy is expected to dominate consistently.

Useful gate override:
- force arrow speed gate at any scale: `-EnforceArrowAlways`

## 8. Rust v2 Operator Quick Checks

```powershell
cd D:\AIWF

# join_rows_v2
Invoke-RestMethod -Uri "http://127.0.0.1:18082/operators/join_rows_v2" -Method Post -ContentType "application/json" -Body (@{
  run_id = "verify_join_v2"
  left_rows = @(@{id=1;k="a";lv=10}, @{id=2;k="b";lv=20})
  right_rows = @(@{rid=9;k="a";rv=99}, @{rid=8;k="c";rv=88})
  left_on = @("k")
  right_on = @("k")
  join_type = "full"
} | ConvertTo-Json -Depth 10)

# aggregate_rows_v2
Invoke-RestMethod -Uri "http://127.0.0.1:18082/operators/aggregate_rows_v2" -Method Post -ContentType "application/json" -Body (@{
  run_id = "verify_agg_v2"
  rows = @(@{g="x";amount=10}, @{g="x";amount=20}, @{g="x";amount=30})
  group_by = @("g")
  aggregates = @(
    @{op="stddev";field="amount";as="std"},
    @{op="percentile_p50";field="amount";as="p50"}
  )
} | ConvertTo-Json -Depth 10)

# schema_registry_v1 infer + get
Invoke-RestMethod -Uri "http://127.0.0.1:18082/operators/schema_registry_v1/infer" -Method Post -ContentType "application/json" -Body (@{
  name="orders";version="v1";rows=@(@{id="1";amount="12.3";active="true"})
} | ConvertTo-Json -Depth 10)
Invoke-RestMethod -Uri "http://127.0.0.1:18082/operators/schema_registry_v1/get" -Method Post -ContentType "application/json" -Body (@{
  name="orders";version="v1"
} | ConvertTo-Json -Depth 10)
```

Note:
- `load_rows_v2` for `pdf/docx/xlsx/image` currently returns metadata probe rows in Rust core.
- full rich extraction for those formats remains in glue-python ingest pipeline.

Release packaging now enforces this gate by default:

```powershell
cd D:\AIWF
powershell -ExecutionPolicy Bypass -File .\ops\scripts\release_productize.ps1 -Version 1.1.4 -RustBenchUpdateProfileOnPass
```

Optional release overrides:
- `-SkipRustTransformBenchGate`
- `-RustBenchRows/-RustBenchRuns/-RustBenchWarmup`
- `-RustBenchMinSpeedup/-RustBenchMinArrowSpeedup`
