from __future__ import annotations

from dataclasses import dataclass, field
import os
from typing import Any, Dict, Optional
from aiwf.accel_transport import DEFAULT_ACCEL_BASE_URL, operator_url


@dataclass(frozen=True)
class CleaningOperatorRequest:
    job_id: str
    step_id: str
    actor: str
    ruleset_version: str
    params: Dict[str, Any]
    input_uri: Optional[str] = None
    output_uri: Optional[str] = None
    job_root: Optional[str] = None
    force_bad_parquet: bool = False

    def to_payload(self) -> Dict[str, Any]:
        return {
            "job_id": self.job_id,
            "step_id": self.step_id,
            "actor": self.actor,
            "ruleset_version": self.ruleset_version,
            "input_uri": self.input_uri,
            "output_uri": self.output_uri,
            "job_root": self.job_root,
            "params": self.params,
            "force_bad_parquet": self.force_bad_parquet,
        }


@dataclass(frozen=True)
class TransformRowsV2OperatorRequest:
    run_id: str
    rows: list[dict[str, Any]] = field(default_factory=list)
    rules: Dict[str, Any] = field(default_factory=dict)
    quality_gates: Dict[str, Any] = field(default_factory=dict)
    schema_hint: Dict[str, Any] = field(default_factory=dict)

    def to_payload(self) -> Dict[str, Any]:
        return {
            "run_id": self.run_id,
            "rows": self.rows,
            "rules": self.rules,
            "quality_gates": self.quality_gates,
            "schema_hint": self.schema_hint,
        }


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


def _error_result(url: str, error: str, *, response: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    result = {
        "attempted": True,
        "ok": False,
        "url": url,
        "error": error,
    }
    if response is not None:
        result["response"] = response
    return result


def _success_result(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "attempted": True,
        "ok": True,
        "url": url,
        "response": body,
    }


def _post_operator_payload(url: str, payload: Dict[str, Any], *, timeout: float) -> Dict[str, Any]:
    import requests

    response = requests.post(url, json=payload, timeout=timeout)
    if response.status_code >= 400:
        return _error_result(url, f"{response.status_code} {response.text}")
    try:
        body = response.json()
    except Exception:
        body = {"raw": response.text[:500]}
    return _success_result(url, body if isinstance(body, dict) else {"value": body})


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
    base_url = resolve_accel_base_url(params)
    timeout = resolve_accel_timeout(
        params,
        param_key="accel_timeout_seconds",
        env_key="AIWF_ACCEL_TIMEOUT",
        default=10.0,
    )
    url = operator_url(base_url, "/operators/cleaning")
    request = CleaningOperatorRequest(
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        job_root=params.get("job_root"),
        params=params,
        force_bad_parquet=bool(params.get("accel_force_bad_parquet", False)),
    )

    try:
        return _post_operator_payload(url, request.to_payload(), timeout=timeout)
    except Exception as exc:
        return _error_result(url, str(exc))


def transform_rows_v2_operator(
    *,
    raw_rows: list[dict[str, Any]],
    params: Dict[str, Any],
    rules: Dict[str, Any],
    quality_gates: Dict[str, Any],
    schema_hint: Dict[str, Any],
) -> Dict[str, Any]:
    base_url = resolve_accel_base_url(params)
    timeout = resolve_accel_timeout(
        params,
        param_key="rust_v2_timeout_seconds",
        env_key="AIWF_RUST_V2_TIMEOUT",
        default=8.0,
    )
    url = operator_url(base_url, "/operators/transform_rows_v2")
    request = TransformRowsV2OperatorRequest(
        run_id=str(params.get("job_id") or ""),
        rows=raw_rows,
        rules=rules,
        quality_gates=quality_gates,
        schema_hint=schema_hint,
    )

    try:
        result = _post_operator_payload(url, request.to_payload(), timeout=timeout)
        if not result.get("ok"):
            return result
        body = result["response"]
        rows = body.get("rows") if isinstance(body, dict) else None
        quality = body.get("quality") if isinstance(body, dict) else None
        if not isinstance(rows, list) or not isinstance(quality, dict):
            return _error_result(url, "transform_rows_v2 invalid response shape", response=body)
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
        return _error_result(url, str(exc))
