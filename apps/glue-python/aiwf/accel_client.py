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


@dataclass(frozen=True)
class CleaningOperatorResponse:
    outputs: Dict[str, Any] = field(default_factory=dict)
    profile: Dict[str, Any] = field(default_factory=dict)
    office_generation_mode: Optional[str] = None
    office_generation_warning: Optional[str] = None
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_body(cls, body: Dict[str, Any]) -> "CleaningOperatorResponse":
        outputs = body.get("outputs") if isinstance(body.get("outputs"), dict) else {}
        profile = body.get("profile") if isinstance(body.get("profile"), dict) else {}
        return cls(
            outputs=outputs,
            profile=profile,
            office_generation_mode=(str(body.get("office_generation_mode")) if body.get("office_generation_mode") is not None else None),
            office_generation_warning=(
                str(body.get("office_generation_warning"))
                if body.get("office_generation_warning") is not None
                else None
            ),
            raw=dict(body),
        )

    def to_dict(self) -> Dict[str, Any]:
        return dict(self.raw)


@dataclass(frozen=True)
class TransformRowsV2OperatorResponse:
    rows: list[dict[str, Any]] = field(default_factory=list)
    quality: Dict[str, Any] = field(default_factory=dict)
    trace_id: str = ""
    raw: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_body(cls, body: Dict[str, Any]) -> "TransformRowsV2OperatorResponse":
        rows = body.get("rows")
        quality = body.get("quality")
        if not isinstance(rows, list) or not isinstance(quality, dict):
            raise ValueError("transform_rows_v2 invalid response shape")
        return cls(
            rows=rows,
            quality=quality,
            trace_id=str(body.get("trace_id") or ""),
            raw=dict(body),
        )


@dataclass(frozen=True)
class OperatorCallResult:
    attempted: bool
    ok: bool
    url: str
    error: Optional[str] = None
    response: Optional[Dict[str, Any]] = None

    def to_dict(self) -> Dict[str, Any]:
        payload: Dict[str, Any] = {
            "attempted": self.attempted,
            "ok": self.ok,
            "url": self.url,
        }
        if self.error is not None:
            payload["error"] = self.error
        if self.response is not None:
            payload["response"] = self.response
        return payload


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
    return OperatorCallResult(
        attempted=True,
        ok=False,
        url=url,
        error=error,
        response=response,
    ).to_dict()


def _success_result(url: str, body: Dict[str, Any]) -> Dict[str, Any]:
    return OperatorCallResult(
        attempted=True,
        ok=True,
        url=url,
        response=body,
    ).to_dict()


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
    job_context = params.get("job_context") if isinstance(params.get("job_context"), dict) else {}
    request = CleaningOperatorRequest(
        job_id=job_id,
        step_id=step_id,
        actor=actor,
        ruleset_version=ruleset_version,
        input_uri=input_uri,
        output_uri=output_uri,
        job_root=job_context.get("job_root") or params.get("job_root"),
        params=params,
        force_bad_parquet=bool(params.get("accel_force_bad_parquet", False)),
    )

    try:
        result = _post_operator_payload(url, request.to_payload(), timeout=timeout)
        if result.get("ok") and isinstance(result.get("response"), dict):
            response = CleaningOperatorResponse.from_body(result["response"])
            result["response"] = response.to_dict()
        return result
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
        try:
            response = TransformRowsV2OperatorResponse.from_body(body)
        except ValueError:
            return _error_result(url, "transform_rows_v2 invalid response shape", response=body)
        quality2 = dict(response.quality)
        quality2["rust_v2_used"] = True
        quality2["rust_v2_trace_id"] = response.trace_id
        return {
            "attempted": True,
            "ok": True,
            "url": url,
            "rows": response.rows,
            "quality": quality2,
            "response": response.raw,
        }
    except Exception as exc:
        return _error_result(url, str(exc))
