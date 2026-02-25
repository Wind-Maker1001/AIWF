# Rust Extension Ops v1

This document defines the operator usage, error codes, and a minimal runnable path for:

- `plugin_operator_v1`
- `columnar_eval_v1`
- `stream_window_v1`
- `stream_window_v2`
- `sketch_v1`
- `runtime_stats_v1`

Contract schema:

- `contracts/rust/operators_extension_v1.schema.json`

## Error Codes

- `AUTH_FAILED`: API key or bridge auth failed.
- `TIMEOUT`: upstream timeout.
- `UPSTREAM_4XX`: upstream request rejected.
- `UPSTREAM_5XX`: upstream server failure.
- `PLUGIN_NOT_ALLOWED`: plugin not in allowlist.
- `PLUGIN_SIGNATURE_INVALID`: plugin signature mismatch.
- `STREAM_WINDOW_INVALID`: invalid stream window arguments.
- `SKETCH_KIND_UNSUPPORTED`: unsupported sketch type.
- `BENCHMARK_GATE_FAILED`: performance gate failed.

## Minimal API Examples

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18082/operators/columnar_eval_v1" -ContentType "application/json" -Body (@{
  rows = @(@{id="1";k="A"}, @{id="2";k="B"})
  select_fields = @("id")
  filter_eq = @{k="A"}
  limit = 100
} | ConvertTo-Json -Depth 8)
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18082/operators/stream_window_v1" -ContentType "application/json" -Body (@{
  stream_key = "demo"
  event_time_field = "ts"
  window_ms = 60000
  rows = @(@{ts=1700000000000;value=1;g="x"})
  group_by = @("g")
  value_field = "value"
} | ConvertTo-Json -Depth 8)
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18082/operators/stream_window_v2" -ContentType "application/json" -Body (@{
  stream_key = "demo"
  event_time_field = "ts"
  window_type = "sliding"
  window_ms = 60000
  slide_ms = 10000
  rows = @(@{ts=1700000000000;value=1;g="x"})
  group_by = @("g")
  value_field = "value"
  emit_late_side = $true
} | ConvertTo-Json -Depth 8)
```

```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:18082/operators/sketch_v1" -ContentType "application/json" -Body (@{
  op = "create"
  kind = "topk"
  field = "topic"
  topk_n = 5
  rows = @(@{topic="a"}, @{topic="a"}, @{topic="b"})
} | ConvertTo-Json -Depth 8)
```

## Standard Workflow Template

Import:

- `examples/workflows/standard_pipeline_v1.json`

Pipeline:

- `ingest_files -> clean_md -> load_rows_v3 -> columnar_eval_v1 -> stream_window_v1 -> sketch_v1 -> explain_plan_v1 -> runtime_stats_v1 -> md_output`
