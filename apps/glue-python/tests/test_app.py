import unittest
import logging
from unittest.mock import patch

from fastapi.testclient import TestClient

import app as glue_app


class AppRouteTests(unittest.TestCase):
    def setUp(self):
        self._old_level = glue_app.log.level
        self._old_httpx_level = logging.getLogger("httpx").level
        glue_app.log.setLevel(logging.CRITICAL)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        self.client = TestClient(glue_app.app, raise_server_exceptions=False)

    def tearDown(self):
        glue_app.log.setLevel(self._old_level)
        logging.getLogger("httpx").setLevel(self._old_httpx_level)

    def test_health(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})

    def test_unknown_flow_returns_404(self):
        resp = self.client.post(
            "/jobs/job1/run/unknown",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 404)
        self.assertFalse(resp.json()["ok"])

    @patch("app.run_cleaning_flow")
    def test_cleaning_success_response_shape(self, run_cleaning_flow):
        run_cleaning_flow.return_value = {"ok": True, "custom": "value"}

        resp = self.client.post(
            "/jobs/job123/run/cleaning",
            json={"actor": "local", "ruleset_version": "v1", "params": {"x": 1}},
        )

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertEqual(payload["flow"], "cleaning")
        self.assertIn("seconds", payload)
        self.assertEqual(payload["custom"], "value")

    @patch("app.run_cleaning_flow")
    def test_cleaning_exposes_office_generation_fields(self, run_cleaning_flow):
        run_cleaning_flow.return_value = {
            "ok": True,
            "accel": {
                "attempted": True,
                "ok": True,
                "used_fallback": False,
                "validation_error": None,
                "office_generation_mode": "python",
                "office_generation_warning": None,
            },
        }

        resp = self.client.post(
            "/jobs/job-office/run/cleaning",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertEqual(payload["accel"]["office_generation_mode"], "python")
        self.assertIsNone(payload["accel"]["office_generation_warning"])
        self.assertIsNone(payload["accel"]["validation_error"])

    @patch("app.run_cleaning_flow", side_effect=RuntimeError("boom"))
    def test_internal_error_hides_traceback_by_default(self, _run_cleaning_flow):
        resp = self.client.post(
            "/jobs/job500/run/cleaning",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 500)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["error"], "internal server error")
        self.assertIn("error_id", payload)
        self.assertNotIn("traceback", payload)

    @patch.dict("os.environ", {"AIWF_DEBUG_ERRORS": "true"})
    @patch("app.run_cleaning_flow", side_effect=RuntimeError("boom"))
    def test_internal_error_exposes_traceback_in_debug(self, _run_cleaning_flow):
        resp = self.client.post(
            "/jobs/job500/run/cleaning",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 500)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertIn("traceback", payload)
        self.assertEqual(payload["exception"], "boom")


if __name__ == "__main__":
    unittest.main()
