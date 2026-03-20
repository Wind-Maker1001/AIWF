from __future__ import annotations

import hashlib
import os
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional

from aiwf.accel_client import run_cleaning_operator, transform_rows_v2_operator
from aiwf.flows.cleaning_transport import (
    base_artifact_upsert_impl,
    base_step_done_impl,
    base_step_fail_impl,
    base_step_start_impl,
    headers_from_params_impl,
    post_json_impl,
)


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def ensure_dirs(*paths: str) -> None:
    for item in paths:
        os.makedirs(item, exist_ok=True)


def utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def is_valid_parquet_file(path: str) -> bool:
    if not path or not os.path.isfile(path):
        return False
    try:
        size = os.path.getsize(path)
        if size < 8:
            return False
        with open(path, "rb") as handle:
            head = handle.read(4)
            handle.seek(-4, os.SEEK_END)
            tail = handle.read(4)
        return head == b"PAR1" and tail == b"PAR1"
    except Exception:
        return False


def headers_from_params(params: Dict[str, Any], *, env_api_key: Optional[str]) -> Dict[str, str]:
    return headers_from_params_impl(params, env_api_key=env_api_key)


def post_json(url: str, body: Dict[str, Any], headers: Dict[str, str]) -> None:
    return post_json_impl(url, body, headers)


def try_accel_cleaning(
    params: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
) -> Dict[str, Any]:
    return run_cleaning_operator(
        params=params,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
    )


def try_rust_transform_rows_v2(
    raw_rows: List[Dict[str, Any]],
    params: Dict[str, Any],
    *,
    rules_dict: Callable[[Dict[str, Any]], Dict[str, Any]],
    rule_param: Callable[[Dict[str, Any], str, Any], Any],
) -> Dict[str, Any]:
    rules = rules_dict(params)
    return transform_rows_v2_operator(
        raw_rows=raw_rows,
        params=params,
        rules=rules,
        quality_gates={
            "max_invalid_rows": rule_param(params, "max_invalid_rows", None),
            "min_output_rows": rule_param(params, "min_output_rows", None),
            "max_invalid_ratio": rule_param(params, "max_invalid_ratio", None),
            "required_fields": rule_param(params, "required_fields", None),
            "max_required_missing_ratio": rule_param(params, "max_required_missing_ratio", None),
        },
        schema_hint={"source": "glue-python.cleaning"},
    )


def base_step_start(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
    params: Dict[str, Any],
    headers: Dict[str, str],
    *,
    post_json_fn: Callable[[str, Dict[str, Any], Dict[str, str]], None],
) -> None:
    return base_step_start_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        params=params,
        headers=headers,
        post_json=post_json_fn,
    )


def base_step_done(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    output_hash: str,
    headers: Dict[str, str],
    *,
    post_json_fn: Callable[[str, Dict[str, Any], Dict[str, str]], None],
) -> None:
    return base_step_done_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        output_hash=output_hash,
        headers=headers,
        post_json=post_json_fn,
    )


def base_step_fail(
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    error: str,
    headers: Dict[str, str],
    *,
    post_json_fn: Callable[[str, Dict[str, Any], Dict[str, str]], None],
) -> None:
    return base_step_fail_impl(
        base_url=base_url,
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        error=error,
        headers=headers,
        post_json=post_json_fn,
    )


def base_artifact_upsert(
    base_url: str,
    job_id: str,
    actor: str,
    artifact_id: str,
    kind: str,
    path: str,
    sha256: str,
    extra_json: Optional[str],
    headers: Dict[str, str],
) -> None:
    return base_artifact_upsert_impl(
        base_url=base_url,
        job_id=job_id,
        actor=actor,
        artifact_id=artifact_id,
        kind=kind,
        path=path,
        sha256=sha256,
        extra_json=extra_json,
        headers=headers,
    )
