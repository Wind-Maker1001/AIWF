import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient


def _load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


PROJECT_ROOT = Path(__file__).resolve().parents[1]
console_app = _load_module("aiwf_dify_console_app", PROJECT_ROOT / "app.py")


class DifyConsoleTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(console_app.app, raise_server_exceptions=False)

    def test_health(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])

    @patch.object(console_app.httpx, "Client")
    def test_base_health(self, client_cls):
        client = client_cls.return_value.__enter__.return_value
        client.get.side_effect = [
            type("Resp", (), {"status_code": 200, "text": "", "json": lambda self: {"status": "UP"}})(),
            type("Resp", (), {"status_code": 200, "text": "", "json": lambda self: {"ok": True}})(),
        ]
        r = self.client.get("/api/base_health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["actuator"]["status"], "UP")
        self.assertTrue(r.json()["dify_bridge"]["ok"])

    @patch.object(console_app.httpx, "Client")
    def test_base_health_rejects_upstream_non_2xx(self, client_cls):
        client = client_cls.return_value.__enter__.return_value
        client.get.side_effect = [
            type("Resp", (), {"status_code": 503, "text": "down", "json": lambda self: {"status": "DOWN"}})(),
            type("Resp", (), {"status_code": 200, "text": "", "json": lambda self: {"ok": True}})(),
        ]
        r = self.client.get("/api/base_health")
        self.assertEqual(r.status_code, 502)
        self.assertIn("503", r.json()["detail"])

    @patch.object(console_app.httpx, "Client")
    def test_base_health_rejects_invalid_upstream_json(self, client_cls):
        client = client_cls.return_value.__enter__.return_value

        class Resp:
            status_code = 200
            text = "oops"

            def json(self):
                raise ValueError("bad json")

        client.get.side_effect = [Resp(), Resp()]
        r = self.client.get("/api/base_health")
        self.assertEqual(r.status_code, 502)
        self.assertIn("invalid JSON", r.json()["detail"])

    @patch.object(console_app.httpx, "AsyncClient")
    def test_run_cleaning_proxy(self, async_client_cls):
        client = async_client_cls.return_value.__aenter__.return_value
        client.post.return_value = type(
            "Resp",
            (),
            {"status_code": 200, "json": lambda self: {"ok": True, "job_id": "j1"}},
        )()
        r = self.client.post(
            "/api/run_cleaning",
            json={"owner": "dify", "actor": "dify", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])
        self.assertEqual(r.json()["job_id"], "j1")

    def test_run_cleaning_rejects_invalid_json_body(self):
        r = self.client.post(
            "/api/run_cleaning",
            content="{",
            headers={"Content-Type": "application/json"},
        )
        self.assertEqual(r.status_code, 400)
        self.assertIn("valid JSON", r.json()["detail"])

    @patch.object(console_app.httpx, "AsyncClient")
    def test_run_cleaning_rejects_invalid_upstream_json(self, async_client_cls):
        client = async_client_cls.return_value.__aenter__.return_value

        class Resp:
            status_code = 200

            def json(self):
                raise ValueError("bad upstream json")

        client.post.return_value = Resp()
        r = self.client.post(
            "/api/run_cleaning",
            json={"owner": "dify", "actor": "dify", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(r.status_code, 502)
        self.assertIn("invalid JSON", r.json()["detail"])


if __name__ == "__main__":
    unittest.main()
