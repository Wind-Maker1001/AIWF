from __future__ import annotations

from typing import Any, Dict, Optional


def headers_from_params_impl(params: Dict[str, Any], *, env_api_key: Optional[str]) -> Dict[str, str]:
    api_key = params.get("api_key") or env_api_key
    if api_key:
        return {"X-API-Key": str(api_key)}
    return {}


def post_json_impl(url: str, body: Dict[str, Any], headers: Dict[str, str]) -> None:
    import requests

    response = requests.post(url, json=body, headers=headers, timeout=30)
    if response.status_code >= 400:
        raise RuntimeError(f"POST {url} -> {response.status_code} {response.text}")


def base_step_start_impl(
    *,
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    ruleset_version: str,
    input_uri: Optional[str],
    output_uri: Optional[str],
    params: Dict[str, Any],
    headers: Dict[str, str],
    post_json: Any,
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/start?actor={actor}"
    body = {
        "ruleset_version": ruleset_version,
        "input_uri": input_uri,
        "output_uri": output_uri,
        "params": params or {},
    }
    post_json(url, body, headers)


def base_step_done_impl(
    *,
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    output_hash: str,
    headers: Dict[str, str],
    post_json: Any,
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/done?actor={actor}"
    body = {"output_hash": output_hash}
    post_json(url, body, headers)


def base_step_fail_impl(
    *,
    base_url: str,
    job_id: str,
    step_id: str,
    actor: str,
    error: str,
    headers: Dict[str, str],
    post_json: Any,
) -> None:
    url = f"{base_url}/api/v1/jobs/{job_id}/steps/{step_id}/fail?actor={actor}"
    body = {"error": error}
    post_json(url, body, headers)


def base_artifact_upsert_impl(
    *,
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
    import requests

    candidates = [
        f"{base_url}/api/v1/jobs/{job_id}/artifacts/upsert?actor={actor}",
        f"{base_url}/api/v1/jobs/{job_id}/artifacts?actor={actor}",
        f"{base_url}/api/v1/jobs/{job_id}/artifacts/register?actor={actor}",
    ]
    body = {
        "artifact_id": artifact_id,
        "kind": kind,
        "path": path,
        "sha256": sha256,
        "extra_json": extra_json,
    }

    last_err = None
    for url in candidates:
        try:
            response = requests.post(url, json=body, headers=headers, timeout=30)
            if response.status_code < 400:
                return
            last_err = f"{response.status_code} {response.text}"
        except Exception as exc:
            last_err = str(exc)

    raise RuntimeError(f"artifact upsert failed, tried {candidates}, last_err={last_err}")
