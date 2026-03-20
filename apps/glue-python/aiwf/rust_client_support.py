from __future__ import annotations

from typing import Any, Dict

from aiwf.accel_transport import (
    DEFAULT_ACCEL_BASE_URL,
    get_json as _get_json_impl,
    json_or_ok as _json_or_ok_impl,
    operator_url as _operator_url,
    post_json as _post_json_impl,
)


def operator_url(base: str, path: str) -> str:
    return _operator_url(base, path)


def operator_post(path: str, payload: Dict[str, Any], *, base_url: str, timeout: float) -> Dict[str, Any]:
    return _post_json_impl(path, payload, base_url=base_url or DEFAULT_ACCEL_BASE_URL, timeout=timeout)


def operator_get(path: str, *, base_url: str, timeout: float) -> Dict[str, Any]:
    return _get_json_impl(path, base_url=base_url or DEFAULT_ACCEL_BASE_URL, timeout=timeout)


def json_or_ok(response: Any, context: str) -> Dict[str, Any]:
    return _json_or_ok_impl(response, context)


def request_json(method: str, base_url: str, path: str, *, timeout: float) -> Dict[str, Any]:
    import requests

    method_upper = str(method or "GET").strip().upper()
    if method_upper == "GET":
        response = requests.get(operator_url(base_url, path), timeout=timeout)
    else:
        response = requests.post(operator_url(base_url, path), timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"{method_upper} {path} -> {response.status_code} {response.text}")
    return json_or_ok(response, f"{method_upper} {path}")


def request_text(base_url: str, path: str, *, timeout: float) -> str:
    import requests

    response = requests.get(operator_url(base_url, path), timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {response.status_code} {response.text}")
    return response.text
