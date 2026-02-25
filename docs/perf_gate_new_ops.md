# Rust New Ops Performance Gate

Gate script:

- `ops/scripts/check_rust_new_ops_bench_gate.ps1`

Bench test:

- `apps/accel-rust/src/main.rs` -> ignored test `benchmark_new_ops_gate`

## Run

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_rust_new_ops_bench_gate.ps1 -MaxColumnarMs 2500 -MaxStreamWindowMs 2500 -MaxSketchMs 2500
```

## CI

`ops/scripts/ci_check.ps1` now includes this gate by default unless:

- `-SkipRustNewOpsBenchGate`

