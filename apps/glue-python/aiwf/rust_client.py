from __future__ import annotations

from typing import Any, Dict, Optional
from aiwf.rust_client_support import (
    json_or_ok as _json_or_ok_impl,
    operator_get as _operator_get_impl,
    operator_post as _operator_post_impl,
    operator_url as _operator_url,
    request_json as _request_json_impl,
    request_text as _request_text_impl,
)

def _url(base: str, path: str) -> str:
    return _operator_url(base, path)


def post_json(path: str, payload: Dict[str, Any], base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return _operator_post_impl(path, payload, base_url=base_url, timeout=timeout)


def get_json(path: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return _operator_get_impl(path, base_url=base_url, timeout=timeout)


def _json_or_ok(response: Any, context: str) -> Dict[str, Any]:
    return _json_or_ok_impl(response, context)


def transform_rows_v2(
    *,
    rows: list[dict[str, Any]],
    rules: Optional[Dict[str, Any]] = None,
    quality_gates: Optional[Dict[str, Any]] = None,
    schema_hint: Optional[Dict[str, Any]] = None,
    trace_id: str = "",
    traceparent: str = "",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "rows": rows,
        "rules": rules or {},
        "quality_gates": quality_gates or {},
        "schema_hint": schema_hint or {},
        "trace_id": trace_id,
        "traceparent": traceparent,
    }
    return post_json("/operators/transform_rows_v2", payload, base_url=base_url, timeout=timeout)


def transform_rows_v2_stream(
    *,
    rows: Optional[list[dict[str, Any]]] = None,
    input_uri: str = "",
    output_uri: str = "",
    chunk_size: int = 2000,
    rules: Optional[Dict[str, Any]] = None,
    quality_gates: Optional[Dict[str, Any]] = None,
    checkpoint_key: str = "",
    resume: bool = False,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "rows": rows or [],
        "input_uri": input_uri,
        "output_uri": output_uri,
        "chunk_size": chunk_size,
        "rules": rules or {},
        "quality_gates": quality_gates or {},
        "checkpoint_key": checkpoint_key,
        "resume": resume,
    }
    return post_json("/operators/transform_rows_v2/stream", payload, base_url=base_url, timeout=timeout)


def text_preprocess_v2(
    *,
    text: str,
    title: str = "",
    remove_references: bool = True,
    remove_notes: bool = True,
    normalize_whitespace: bool = True,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "text": text,
        "title": title,
        "remove_references": remove_references,
        "remove_notes": remove_notes,
        "normalize_whitespace": normalize_whitespace,
    }
    return post_json("/operators/text_preprocess_v2", payload, base_url=base_url, timeout=timeout)


def join_rows_v1(
    *,
    left_rows: list[dict[str, Any]],
    right_rows: list[dict[str, Any]],
    left_on: str,
    right_on: str,
    join_type: str = "inner",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "left_rows": left_rows,
        "right_rows": right_rows,
        "left_on": left_on,
        "right_on": right_on,
        "join_type": join_type,
    }
    return post_json("/operators/join_rows_v1", payload, base_url=base_url, timeout=timeout)


def normalize_schema_v1(
    *,
    rows: list[dict[str, Any]],
    schema: Dict[str, Any],
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {"run_id": run_id, "rows": rows, "schema": schema}
    return post_json("/operators/normalize_schema_v1", payload, base_url=base_url, timeout=timeout)


def entity_extract_v1(
    *,
    text: str = "",
    rows: Optional[list[dict[str, Any]]] = None,
    text_field: str = "text",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "text": text,
        "rows": rows or [],
        "text_field": text_field,
    }
    return post_json("/operators/entity_extract_v1", payload, base_url=base_url, timeout=timeout)


def rules_compile_v1(
    *,
    dsl: str,
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    return post_json("/operators/rules_compile_v1", {"dsl": dsl}, base_url=base_url, timeout=timeout)


def rules_package_publish_v1(
    *,
    name: str,
    version: str,
    dsl: str = "",
    rules: Optional[Dict[str, Any]] = None,
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {"name": name, "version": version, "dsl": dsl, "rules": rules or {}}
    return post_json("/operators/rules_package_v1/publish", payload, base_url=base_url, timeout=timeout)


def rules_package_get_v1(
    *,
    name: str,
    version: str,
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {"name": name, "version": version}
    return post_json("/operators/rules_package_v1/get", payload, base_url=base_url, timeout=timeout)


def load_rows_v1(
    *,
    source_type: str,
    source: str,
    query: str = "",
    limit: int = 10000,
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {"source_type": source_type, "source": source, "query": query, "limit": limit}
    return post_json("/operators/load_rows_v1", payload, base_url=base_url, timeout=timeout)


def save_rows_v1(
    *,
    sink_type: str,
    sink: str,
    rows: list[dict[str, Any]],
    table: str = "",
    parquet_mode: str = "typed",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "sink_type": sink_type,
        "sink": sink,
        "rows": rows,
        "table": table,
        "parquet_mode": parquet_mode,
    }
    return post_json("/operators/save_rows_v1", payload, base_url=base_url, timeout=timeout)


def aggregate_rows_v1(
    *,
    rows: list[dict[str, Any]],
    group_by: list[str],
    aggregates: list[dict[str, Any]],
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "rows": rows,
        "group_by": group_by,
        "aggregates": aggregates,
    }
    return post_json("/operators/aggregate_rows_v1", payload, base_url=base_url, timeout=timeout)


def columnar_eval_v1(
    *,
    rows: list[dict[str, Any]],
    select_fields: Optional[list[str]] = None,
    filter_eq: Optional[Dict[str, Any]] = None,
    limit: int = 10000,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "rows": rows,
        "select_fields": select_fields or [],
        "filter_eq": filter_eq or {},
        "limit": limit,
    }
    return post_json("/operators/columnar_eval_v1", payload, base_url=base_url, timeout=timeout)


def stream_window_v2(
    *,
    stream_key: str,
    rows: list[dict[str, Any]],
    event_time_field: str,
    window_ms: int,
    window_type: str = "tumbling",
    slide_ms: int = 0,
    session_gap_ms: int = 0,
    watermark_ms: int = 0,
    allowed_lateness_ms: int = 0,
    group_by: Optional[list[str]] = None,
    value_field: str = "value",
    trigger: str = "on_watermark",
    emit_late_side: bool = True,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "stream_key": stream_key,
        "rows": rows,
        "event_time_field": event_time_field,
        "window_type": window_type,
        "window_ms": window_ms,
        "slide_ms": slide_ms or None,
        "session_gap_ms": session_gap_ms or None,
        "watermark_ms": watermark_ms or None,
        "allowed_lateness_ms": allowed_lateness_ms or None,
        "group_by": group_by or [],
        "value_field": value_field,
        "trigger": trigger,
        "emit_late_side": emit_late_side,
    }
    return post_json("/operators/stream_window_v2", payload, base_url=base_url, timeout=timeout)


def parquet_io_v2(
    *,
    op: str,
    path: str,
    rows: Optional[list[dict[str, Any]]] = None,
    parquet_mode: str = "typed",
    limit: int = 10000,
    columns: Optional[list[str]] = None,
    predicate_field: str = "",
    predicate_eq: Optional[Dict[str, Any]] = None,
    partition_by: Optional[list[str]] = None,
    compression: str = "snappy",
    recursive: bool = True,
    schema_mode: str = "additive",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "op": op,
        "path": path,
        "rows": rows or [],
        "parquet_mode": parquet_mode,
        "limit": limit,
        "columns": columns or [],
        "predicate_field": predicate_field or None,
        "predicate_eq": predicate_eq or None,
        "partition_by": partition_by or [],
        "compression": compression,
        "recursive": recursive,
        "schema_mode": schema_mode,
    }
    return post_json("/operators/parquet_io_v2", payload, base_url=base_url, timeout=timeout)


def sketch_v1(
    *,
    op: str,
    kind: str = "hll",
    state: Optional[Dict[str, Any]] = None,
    rows: Optional[list[dict[str, Any]]] = None,
    field: str = "value",
    topk_n: int = 5,
    merge_state: Optional[Dict[str, Any]] = None,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "op": op,
        "kind": kind,
        "state": state or {},
        "rows": rows or [],
        "field": field,
        "topk_n": topk_n,
        "merge_state": merge_state or {},
    }
    return post_json("/operators/sketch_v1", payload, base_url=base_url, timeout=timeout)


def runtime_stats_v1(
    *,
    op: str = "summary",
    operator: str = "",
    ok: Optional[bool] = None,
    error_code: str = "",
    duration_ms: int = 0,
    rows_in: int = 0,
    rows_out: int = 0,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "op": op,
        "operator": operator or None,
        "ok": ok,
        "error_code": error_code or None,
        "duration_ms": duration_ms or None,
        "rows_in": rows_in or None,
        "rows_out": rows_out or None,
    }
    return post_json("/operators/runtime_stats_v1", payload, base_url=base_url, timeout=timeout)


def plugin_operator_v1(
    *,
    plugin: str,
    op: str = "run",
    payload: Optional[Dict[str, Any]] = None,
    tenant_id: str = "",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    body = {
        "run_id": run_id,
        "tenant_id": tenant_id or None,
        "plugin": plugin,
        "op": op,
        "payload": payload or {},
    }
    return post_json("/operators/plugin_operator_v1", body, base_url=base_url, timeout=timeout)


def quality_check_v1(
    *,
    rows: list[dict[str, Any]],
    rules: Dict[str, Any],
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {"run_id": run_id, "rows": rows, "rules": rules}
    return post_json("/operators/quality_check_v1", payload, base_url=base_url, timeout=timeout)


def aggregate_pushdown_v1(
    *,
    source_type: str,
    source: str,
    from_: str,
    group_by: list[str],
    aggregates: list[dict[str, Any]],
    where_sql: str = "",
    limit: int = 10000,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "source_type": source_type,
        "source": source,
        "from": from_,
        "group_by": group_by,
        "aggregates": aggregates,
        "where_sql": where_sql,
        "limit": limit,
    }
    return post_json("/operators/aggregate_pushdown_v1", payload, base_url=base_url, timeout=timeout)


def workflow_run(
    *,
    steps: list[dict[str, Any]],
    context: Optional[Dict[str, Any]] = None,
    trace_id: str = "",
    traceparent: str = "",
    tenant_id: str = "",
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 60.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "steps": steps,
        "context": context or {},
        "trace_id": trace_id,
        "traceparent": traceparent,
        "tenant_id": tenant_id,
    }
    return post_json("/workflow/run", payload, base_url=base_url, timeout=timeout)


def workflow_reference_run_v1(
    *,
    workflow_definition: Dict[str, Any],
    version_id: str,
    published_version_id: str = "",
    job_id: str = "",
    actor: str = "",
    ruleset_version: str = "",
    trace_id: str = "",
    traceparent: str = "",
    tenant_id: str = "",
    run_id: str = "",
    job_context: Optional[Dict[str, Any]] = None,
    params: Optional[Dict[str, Any]] = None,
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 60.0,
) -> Dict[str, Any]:
    payload = {
        "workflow_definition": workflow_definition,
        "version_id": version_id,
        "published_version_id": published_version_id or version_id,
        "job_id": job_id,
        "actor": actor,
        "ruleset_version": ruleset_version,
        "trace_id": trace_id,
        "traceparent": traceparent,
        "tenant_id": tenant_id,
        "run_id": run_id,
        "job_context": job_context or {},
        "params": params or {},
    }
    import requests

    url = _url(base_url, "/operators/workflow_reference_run_v1")
    response = requests.post(url, json=payload, timeout=timeout)
    return _json_or_ok(response, "POST /operators/workflow_reference_run_v1")


def plugin_exec_v1(
    *,
    plugin: str,
    input: Dict[str, Any],
    run_id: str = "",
    tenant_id: str = "",
    trace_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 30.0,
) -> Dict[str, Any]:
    payload = {
        "plugin": plugin,
        "input": input,
        "run_id": run_id,
        "tenant_id": tenant_id,
        "trace_id": trace_id,
    }
    return post_json("/operators/plugin_exec_v1", payload, base_url=base_url, timeout=timeout)


def compute_metrics(
    *,
    text: str,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "text": text,
    }
    return post_json("/operators/compute_metrics", payload, base_url=base_url, timeout=timeout)


def submit_transform_rows_v2(
    *,
    rows: list[dict[str, Any]],
    rules: Optional[Dict[str, Any]] = None,
    quality_gates: Optional[Dict[str, Any]] = None,
    schema_hint: Optional[Dict[str, Any]] = None,
    run_id: str = "",
    base_url: str = "http://127.0.0.1:18082",
    timeout: float = 10.0,
) -> Dict[str, Any]:
    payload = {
        "run_id": run_id,
        "rows": rows,
        "rules": rules or {},
        "quality_gates": quality_gates or {},
        "schema_hint": schema_hint or {},
    }
    return post_json("/operators/transform_rows_v2/submit", payload, base_url=base_url, timeout=timeout)


def get_task(task_id: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return _request_json_impl("GET", base_url, f"/tasks/{task_id}", timeout=timeout)


def health(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return get_json("/health", base_url=base_url, timeout=timeout)


def reload_runtime_config(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return post_json("/admin/reload_runtime_config", {}, base_url=base_url, timeout=timeout)


def get_metrics(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> str:
    return _request_text_impl(base_url, "/metrics", timeout=timeout)


def cancel_task(task_id: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return _request_json_impl("POST", base_url, f"/tasks/{task_id}/cancel", timeout=timeout)
