from __future__ import annotations

from typing import Any, Dict


DEFAULT_ACCEL_BASE_URL = "http://127.0.0.1:18082"


def operator_url(base_url: str, path: str) -> str:
    base = str(base_url or DEFAULT_ACCEL_BASE_URL).rstrip("/")
    endpoint = str(path or "").strip()
    if not endpoint.startswith("/"):
        endpoint = "/" + endpoint
    return base + endpoint


def json_or_ok(response: Any, context: str) -> Dict[str, Any]:
    if not getattr(response, "content", b""):
        return {"ok": True}
    try:
        payload = response.json()
    except ValueError as exc:
        preview = str(getattr(response, "text", "") or "")[:200]
        raise RuntimeError(f"{context} returned invalid JSON: {preview}") from exc
    return payload if isinstance(payload, dict) else {"value": payload}


def post_json(path: str, payload: Dict[str, Any], *, base_url: str = DEFAULT_ACCEL_BASE_URL, timeout: float = 10.0) -> Dict[str, Any]:
    import requests

    url = operator_url(base_url, path)
    response = requests.post(url, json=payload, timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"POST {path} -> {response.status_code} {response.text}")
    return json_or_ok(response, f"POST {path}")


def get_json(path: str, *, base_url: str = DEFAULT_ACCEL_BASE_URL, timeout: float = 10.0) -> Dict[str, Any]:
    import requests

    url = operator_url(base_url, path)
    response = requests.get(url, timeout=timeout)
    if response.status_code >= 400:
        raise RuntimeError(f"GET {path} -> {response.status_code} {response.text}")
    return json_or_ok(response, f"GET {path}")
