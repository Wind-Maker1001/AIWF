from __future__ import annotations

from typing import Any, Dict, Optional


def _url(base: str, path: str) -> str:
    b = str(base or "http://127.0.0.1:18082").rstrip("/")
    p = str(path or "").strip()
    if not p.startswith("/"):
        p = "/" + p
    return b + p


def post_json(path: str, payload: Dict[str, Any], base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    import requests

    r = requests.post(_url(base_url, path), json=payload, timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"POST {path} -> {r.status_code} {r.text}")
    return r.json()


def get_json(path: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    import requests

    r = requests.get(_url(base_url, path), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {r.status_code} {r.text}")
    return r.json()


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
    import requests

    r = requests.get(_url(base_url, f"/tasks/{task_id}"), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"GET /tasks/{task_id} -> {r.status_code} {r.text}")
    return r.json()


def health(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return get_json("/health", base_url=base_url, timeout=timeout)


def reload_runtime_config(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    return post_json("/admin/reload_runtime_config", {}, base_url=base_url, timeout=timeout)


def get_metrics(base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> str:
    import requests

    r = requests.get(_url(base_url, "/metrics"), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"GET /metrics -> {r.status_code} {r.text}")
    return r.text


def cancel_task(task_id: str, base_url: str = "http://127.0.0.1:18082", timeout: float = 10.0) -> Dict[str, Any]:
    import requests

    r = requests.post(_url(base_url, f"/tasks/{task_id}/cancel"), timeout=timeout)
    if r.status_code >= 400:
        raise RuntimeError(f"POST /tasks/{task_id}/cancel -> {r.status_code} {r.text}")
    return r.json()
