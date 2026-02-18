import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as console_app


class DifyConsoleTests(unittest.TestCase):
    def setUp(self):
        self.client = TestClient(console_app.app, raise_server_exceptions=False)

    def test_health(self):
        r = self.client.get("/health")
        self.assertEqual(r.status_code, 200)
        self.assertTrue(r.json()["ok"])

    @patch("app.httpx.Client")
    def test_base_health(self, client_cls):
        client = client_cls.return_value.__enter__.return_value
        client.get.side_effect = [
            type("Resp", (), {"json": lambda self: {"status": "UP"}})(),
            type("Resp", (), {"json": lambda self: {"ok": True}})(),
        ]
        r = self.client.get("/api/base_health")
        self.assertEqual(r.status_code, 200)
        self.assertEqual(r.json()["actuator"]["status"], "UP")
        self.assertTrue(r.json()["dify_bridge"]["ok"])

    @patch("app.httpx.AsyncClient")
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


if __name__ == "__main__":
    unittest.main()

