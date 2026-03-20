# AIWF Project Review (2026-03-13)

## Scope

This review covers the full repository under `D:\AIWF`, including:

- source modules under `apps/`
- CI and operational scripts under `ops/`
- GitHub workflows under `.github/workflows/`
- docs, contracts, and SQL initialization assets

The review is based on:

- repository structure and source inspection
- local full CI execution
- generated acceptance, regression, and performance logs under `ops/logs/`

## Validation Summary

### Full CI

Executed:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Full
```

Observed result:

- first full run failed during desktop packaging because `electron.exe` in `dist\win-unpacked` was still held by a lingering process
- after killing lingering desktop processes and cleaning `dist` / `dist-lite`, the second full run passed end-to-end

Latest successful local full CI transcript:

- `ops/logs/ci/review_full_retry_20260313_223644.transcript.log`

### Coverage Included in the Successful Full Run

- developer tools and runtime dependency checks
- docs links, release evidence, OpenAPI/SDK sync, secret scan, encoding scan
- Rust, Java, and Python automated tests
- desktop unit/UI/packaged startup checks
- smoke + invalid parquet fallback integration
- regression quality + regression baseline gate
- desktop real-sample and finance-template acceptance
- Rust routing / async / transform / new-ops benchmark gates
- native WinUI smoke

## Repository Scale

Tracked source footprint snapshot:

- `apps/dify-desktop`: 165 code files / ~28,208 LOC
- `apps/accel-rust`: 184 code files / ~21,553 LOC
- `apps/glue-python`: 63 code files / ~10,335 LOC
- `ops`: 74 script files / ~8,325 LOC
- `apps/dify-native-winui`: 62 code files / ~6,679 LOC
- `apps/base-java`: 90 code files / ~4,945 LOC
- `apps/dify-console`: 2 code files / ~504 LOC

Tracked file counts by major area:

- `apps/accel-rust`: 188
- `apps/base-java`: 95
- `apps/glue-python`: 66
- `apps/dify-desktop`: 222
- `apps/dify-native-winui`: 72
- `apps/dify-console`: 4
- `ops`: 83
- `docs`: 32
- `contracts`: 7

## Overall Assessment

### Engineering State

AIWF is no longer an early prototype. It already behaves like a multi-runtime product engineering repository with:

- a clear control-plane / orchestration / acceleration split
- desktop delivery paths
- performance and regression gates
- contract and packaging awareness

Engineering-stage judgment:

- architecture maturity: **mid-to-late stage**
- productization maturity: **good, but not fully converged**
- low-maintenance platform maturity: **not there yet**

### Scorecard

These are engineering judgment scores derived from source structure, CI behavior, and runtime evidence:

- performance: **8.5 / 10**
- maintainability: **6.8 / 10**
- extensibility: **8.3 / 10**
- CI maturity: **8.0 / 10**
- overall engineering completion: **~82%**

## Module Review

### `apps/base-java`

Role:

- control plane
- orchestration API
- job / step / artifact persistence
- `glue-python` gateway

Strengths:

- layered API/service/repository split
- centralized API exception model
- control-plane behavior is relatively stable
- integration-oriented tests are present

Risks:

- repository layer grows through handwritten SQL and mapping
- `JobService` is trending toward orchestration concentration

Representative hotspots:

- `apps/base-java/src/main/java/com/aiwf/base/db/JobRepository.java`
- `apps/base-java/src/main/java/com/aiwf/base/service/JobService.java`
- `apps/base-java/src/main/java/com/aiwf/base/db/RuntimeTaskRepository.java`

Current completion:

- **~85%**

### `apps/glue-python`

Role:

- flow runtime
- cleaning / preprocess / ingest orchestration
- office artifact generation
- extension registry and capability exposure

Strengths:

- best extensibility story in the repo
- flow registry and extension conflict handling are already platform-like
- explicit handoff to Rust for heavy operators is in place

Risks:

- complexity is accumulating in a few large files
- ingest + office generation + preprocess all carry high branching and I/O surface

Representative hotspots:

- `apps/glue-python/aiwf/ingest.py`
- `apps/glue-python/aiwf/office_outputs.py`
- `apps/glue-python/aiwf/preprocess_registry.py`
- `apps/glue-python/aiwf/flows/cleaning.py`

Current completion:

- **~88%**

### `apps/accel-rust`

Role:

- high-performance operator runtime
- async task execution
- metrics, tracing, and benchmarked operator delivery

Strengths:

- strongest performance engineering in the repo
- benchmark gates are embedded into CI
- routing, transform, and async behavior are tracked with artifacts

Risks:

- highest cognitive complexity in the backend
- `main.rs`, task handlers, and catalog composition are already heavy
- test surface is broad enough that future debugging cost may rise

Representative hotspots:

- `apps/accel-rust/src/main.rs`
- `apps/accel-rust/src/http/handlers_core/tasks.rs`
- `apps/accel-rust/src/operator_catalog.rs`
- `apps/accel-rust/src/main_tests_part*.rs`

Current completion:

- **~84%**

### `apps/dify-desktop`

Role:

- current primary desktop delivery surface
- offline workflow builder and runner
- packaging, release, and acceptance chain

Strengths:

- strongest product completeness among frontends
- broad test coverage: node tests, workflow UI tests, packaged startup checks
- acceptance and release evidence are already operationalized

Risks:

- largest maintenance hotspot in the repository
- `main_ipc_workflow.js` and `renderer/workflow/app.js` are too large
- local JSON / JSONL persistence is flexible but adds schema-drift risk

Representative hotspots:

- `apps/dify-desktop/main_ipc_workflow.js`
- `apps/dify-desktop/renderer/workflow/app.js`
- `apps/dify-desktop/offline_outputs.js`
- `apps/dify-desktop/main_ipc.js`

Current completion:

- **~88%**

### `apps/dify-native-winui`

Role:

- native replacement track for the Electron shell
- WinUI canvas / runtime / result binding shell

Strengths:

- startup performance is already very good
- dedicated smoke, UIA, test, and perf projects exist
- migration is beyond bootstrap stage

Risks:

- still a migration track, not yet the sole frontend
- dual frontend maintenance cost remains real
- `MainWindow`-centric partial classes will keep growing if not split further

Representative hotspots:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.*.cs`

Current completion:

- **~60%**

### `apps/dify-console`

Role:

- lightweight FastAPI-backed helper console
- base health / run-cleaning auxiliary surface

Strengths:

- simple, cheap to maintain, easy to run

Risks:

- single-file architecture
- not suitable as a long-term primary frontend without decomposition

Representative hotspot:

- `apps/dify-console/app.py`

Current completion:

- **~70%** for helper-console scope

## Performance Evidence

### Desktop Routing Benchmark

Source:

- `ops/logs/route_bench/routing_bench_latest.json`

Highlights:

- `720` nodes / `1560` edges
- overall `98.94 ms/edge`
- worst scenario `108.403 ms/edge`
- `fallback_ratio = 0`
- trend window sample count `7`, gate passed

Assessment:

- route quality is strong and regression-aware

### Rust Async Task Benchmark

Source:

- `ops/logs/perf/async_tasks_baseline_latest.json`

Highlights:

- tasks: `12`
- rows/task: `600`
- submit latency p50: `152.527 ms`
- end-to-end latency p50: `844.977 ms`
- failed / cancelled / timeout: `0 / 0 / 0`

Assessment:

- async execution path is healthy and repeatably measured

### Rust Transform Benchmark

Source:

- `ops/logs/bench/rust_transform/20260313_224422/benchmark.json`

Highlights:

- rows: `120000`
- `columnar_arrow_v1` is current best engine
- speedup vs `row_v1`: `1.043x`
- auto decision hit rate: `1.0`

Assessment:

- transform engine selection and gate logic are not speculative; they are backed by repeated measurements

### Native WinUI Startup

Source:

- `ops/logs/smoke/native-winui/20260313-224253/startup.json`

Highlights:

- first window activated: `201.927 ms`
- main window ctor: `91.041 ms`
- canvas workspace init: `12.275 ms`
- canvas prewarm: `0.456 ms`

Assessment:

- native shell startup is already in strong shape

### Acceptance and Office Quality

Sources:

- `ops/logs/acceptance/desktop_real_sample/desktop_real_sample_latest.md`
- `ops/logs/acceptance/desktop_finance_template/desktop_finance_template_latest.md`

Highlights:

- real sample acceptance: office quality score `100`
- finance template acceptance: office quality score `100`

Assessment:

- real output quality is not only unit-tested; it is checked via artifact acceptance

## Maintainability Review

### Strengths

- architectural responsibilities are mostly sane across Java / Python / Rust
- testing is meaningful rather than decorative
- CI scripts encode operational knowledge
- contracts and reference docs exist

### Weaknesses

- a small number of very large files dominate risk
- desktop workflow runtime is the biggest maintenance hotspot
- scripting surface under `ops/` is powerful but large
- duplicated frontend investment across Electron and WinUI increases coordination cost

## Extensibility Review

### Strengths

- `glue-python` already behaves like an extension host
- `accel-rust` operator surface is contract-aware
- desktop workflow and chiplet model support further local feature growth

### Constraints

- cross-language contract drift is an ongoing risk
- schema evolution in JSON/JSONL local stores needs discipline
- frontend dual-track development taxes new feature rollout

## CI / Ops Review

### Strengths

- the repo has both fast and full verification paths
- self-hosted workflow actively cleans workspace and port state before execution
- acceptance, regression, smoke, and benchmark evidence are persisted to disk

### Risks

- GitHub-hosted quick CI skips desktop UI and packaged-startup checks
- full local CI can still be tripped by stale desktop processes if the environment is dirty

## Main Risks

1. oversized desktop workflow IPC and renderer files
2. growing Rust core-module complexity
3. expanding Python orchestration files with mixed concerns
4. dual maintenance across Electron and WinUI
5. hosted CI not representing real desktop delivery quality

## Recommended Next Actions

### Immediate

1. split `apps/dify-desktop/main_ipc_workflow.js`
2. split `apps/dify-desktop/renderer/workflow/app.js`
3. harden desktop packaged-startup checks against lingering process locks

### Near-Term

4. continue decomposing `glue-python` ingest / office / cleaning hotspots
5. continue decomposing `accel-rust` startup, catalog, and task-handler hotspots
6. define a clear WinUI migration boundary to avoid indefinite frontend duplication

### Medium-Term

7. expand system-level throughput evidence for Java + Python + SQL paths
8. formalize cross-language schema evolution rules for workflow/job payloads

## Final Judgment

AIWF is a strong engineering repository with real product substance:

- not a toy
- not fully converged
- already capable of sustained feature delivery

Its next phase should focus less on adding more raw capability and more on:

- reducing hotspot complexity
- tightening delivery confidence on desktop paths
- converging the frontend strategy
