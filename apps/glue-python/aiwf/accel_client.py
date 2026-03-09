from __future__ import annotations

import os
from typing import Any, Dict, Optional


DEFAULT_ACCEL_BASE_URL = "http://127.0.0.1:18082"


def _operator_url(base_url: str, path: str) -> str:
    base = str(base_url or DEFAULT_ACCEL_BASE_URL).rstrip("/")
    endpoint = str(path or "").strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    return base + endpoint


def resolve_accel_base_url(params: Dict[str, Any]) -> str:
    return str(params.get("accel_url") or os.getenv("AIWF_ACCEL_URL") or DEFAULT_ACCEL_BASE_URL).rstrip("/")


def resolve_accel_timeout(
    params: Dict[str, Any],
    *,
    param_key: str,
    env_key: str,
    default: float,
) -> float:
    raw = params.get(param_key)
    if raw is None:
        raw = os.getenv(env_key)
    return float(raw if raw is not None else default)


def run_cleaning_operator(
    *,
    params: Dict[str, Any],
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
) -> Dict[str, Any]:
    import requests

    base_url = resolve_accel_base_url(params)
    timeout = resolve_accel_timeout(
        params,
        param_key="accel_timeout_seconds",
        env_key="AIWF_ACCEL_TIMEOUT",
        default=10.0,
    )
    url = _operator_url(base_url, "/operators/cleaning")
    payload = {
        "job_id": job_id,
        "step_id": step_id,
        "actor": actor,
        "ruleset_version": ruleset_version,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "job_root": params.get("job_root"),
        "params": params,
        "force_bad_parquet": bool(params.get("accel_force_bad_parquet", False)),
    }

    try:
        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code >= 400:
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": f"{response.status_code} {response.text}",
            }
        try:
            body = response.json()
        except Exception:
            body = {"raw": response.text[:500]}
        return {
            "attempted": True,
            "ok": True,
            "url": url,
            "response": body,
        }
    except Exception as exc:
        return {
            "attempted": True,
            "ok": False,
            "url": url,
            "error": str(exc),
        }


def transform_rows_v2_operator(
    *,
    raw_rows: list[dict[str, Any]],
    params: Dict[str, Any],
    rules: Dict[str, Any],
    quality_gates: Dict[str, Any],
    schema_hint: Dict[str, Any],
) -> Dict[str, Any]:
    import requests

    base_url = resolve_accel_base_url(params)
    timeout = resolve_accel_timeout(
        params,
        param_key="rust_v2_timeout_seconds",
        env_key="AIWF_RUST_V2_TIMEOUT",
        default=8.0,
    )
    url = _operator_url(base_url, "/operators/transform_rows_v2")
    payload = {
        "run_id": str(params.get("job_id") or ""),
        "rows": raw_rows,
        "rules": rules,
        "quality_gates": quality_gates,
        "schema_hint": schema_hint,
    }

    try:
        response = requests.post(url, json=payload, timeout=timeout)
        if response.status_code >= 400:
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": f"{response.status_code} {response.text}",
            }
        body = response.json()
        rows = body.get("rows") if isinstance(body, dict) else None
        quality = body.get("quality") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not isinstance(quality, dict):
            return {
                "attempted": True,
                "ok": False,
                "url": url,
                "error": "transform_rows_v2 invalid response shape",
                "response": body,
            }
        quality2 = dict(quality)
        quality2["rust_v2_used"] = True
        quality2["rust_v2_trace_id"] = str(body.get("trace_id") or "")
        return {
            "attempted": True,
            "ok": True,
            "url": url,
            "rows": rows,
            "quality": quality2,
            "response": body,
        }
    except Exception as exc:
        return {
            "attempted": True,
            "ok": False,
            "url": url,
            "error": str(exc),
        }
