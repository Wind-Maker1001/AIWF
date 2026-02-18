from __future__ import annotations
import requests
from typing import Any, Dict

class BaseClient:
    def __init__(self, base_url: str, api_key: str | None = None):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key

    def _headers(self) -> Dict[str, str]:
        h = {"Content-Type": "application/json"}
        if self.api_key:
            h["X-API-Key"] = self.api_key
        return h

    def step_start(self, job_id: str, step_id: str, actor: str, payload: Dict[str, Any]):
        url = f"{self.base_url}/api/v1/jobs/{job_id}/steps/{step_id}/start"
        r = requests.post(url, params={"actor": actor}, json=payload, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json() if r.content else {"ok": True}

    def step_done(self, job_id: str, step_id: str, actor: str, payload: Dict[str, Any]):
        url = f"{self.base_url}/api/v1/jobs/{job_id}/steps/{step_id}/done"
        r = requests.post(url, params={"actor": actor}, json=payload, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json() if r.content else {"ok": True}

    def register_artifact(self, job_id: str, actor: str, artifact: Dict[str, Any]):
        url = f"{self.base_url}/api/v1/jobs/{job_id}/artifacts/register"
        r = requests.post(url, params={"actor": actor}, json=artifact, headers=self._headers(), timeout=30)
        r.raise_for_status()
        return r.json() if r.content else {"ok": True}
