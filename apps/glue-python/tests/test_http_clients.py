import unittest
from unittest.mock import patch

from aiwf import accel_client
from aiwf.base_client import BaseClient
from aiwf.flows.cleaning_transport import headers_from_params_impl
from aiwf import rust_client


class HttpClientTests(unittest.TestCase):
    def test_accel_cleaning_request_serializes_expected_fields(self):
        req = accel_client.CleaningOperatorRequest(
            job_id="job-1",
            step_id="cleaning",
            actor="local",
            ruleset_version="v1",
            input_uri="in",
            output_uri="out",
            job_root="D:/job",
            params={"rows": [{"id": 1}]},
            force_bad_parquet=True,
        )

        payload = req.to_payload()
        self.assertEqual(payload["job_id"], "job-1")
        self.assertEqual(payload["step_id"], "cleaning")
        self.assertEqual(payload["params"]["rows"][0]["id"], 1)
        self.assertTrue(payload["force_bad_parquet"])

    def test_accel_transform_rows_v2_request_serializes_expected_fields(self):
        req = accel_client.TransformRowsV2OperatorRequest(
            run_id="job-1",
            rows=[{"id": 1}],
            rules={"max_amount": 10},
            quality_gates={"min_output_rows": 1},
            schema_hint={"source": "test"},
        )

        payload = req.to_payload()
        self.assertEqual(payload["run_id"], "job-1")
        self.assertEqual(payload["rows"][0]["id"], 1)
        self.assertEqual(payload["rules"]["max_amount"], 10)
        self.assertEqual(payload["quality_gates"]["min_output_rows"], 1)

    def test_accel_cleaning_response_parses_known_fields(self):
        parsed = accel_client.CleaningOperatorResponse.from_body(
            {
                "outputs": {"cleaned_parquet": {"path": "x.parquet"}},
                "profile": {"rows": 1},
                "office_generation_mode": "python",
                "office_generation_warning": "warn",
            }
        )

        self.assertEqual(parsed.outputs["cleaned_parquet"]["path"], "x.parquet")
        self.assertEqual(parsed.profile["rows"], 1)
        self.assertEqual(parsed.office_generation_mode, "python")
        self.assertEqual(parsed.office_generation_warning, "warn")

    def test_accel_transform_response_rejects_invalid_shape(self):
        with self.assertRaisesRegex(ValueError, "invalid response shape"):
            accel_client.TransformRowsV2OperatorResponse.from_body({"rows": "bad", "quality": {}})

    def test_base_client_returns_ok_for_empty_success_body(self):
        client = BaseClient("http://base")

        class Resp:
            content = b""
            text = ""

            def raise_for_status(self):
                return None

        with patch("aiwf.base_client.requests.post", return_value=Resp()):
            result = client.step_start("job-1", "step-1", "actor", {"x": 1})

        self.assertEqual(result, {"ok": True})

    def test_base_client_raises_clear_error_for_invalid_json(self):
        client = BaseClient("http://base")

        class Resp:
            content = b"not-json"
            text = "not-json"

            def raise_for_status(self):
                return None

            def json(self):
                raise ValueError("bad json")

        with patch("aiwf.base_client.requests.post", return_value=Resp()):
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                client.step_done("job-1", "step-1", "actor", {"x": 1})

    def test_rust_client_returns_ok_for_empty_success_body(self):
        class Resp:
            status_code = 200
            content = b""
            text = ""

        with patch("requests.post", return_value=Resp()):
            result = rust_client.post_json("/health", {})

        self.assertEqual(result, {"ok": True})

    def test_rust_client_raises_clear_error_for_invalid_json(self):
        class Resp:
            status_code = 200
            content = b"oops"
            text = "oops"

            def json(self):
                raise ValueError("bad json")

        with patch("requests.get", return_value=Resp()):
            with self.assertRaisesRegex(RuntimeError, "invalid JSON"):
                rust_client.get_json("/health")

    def test_rust_client_get_task_returns_structured_success(self):
        class Resp:
            status_code = 200
            content = b'{"ok":true,"status":"done"}'
            text = '{"ok":true,"status":"done"}'

            def json(self):
                return {"ok": True, "status": "done"}

        with patch("requests.get", return_value=Resp()):
            result = rust_client.get_task("task-1")

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["status"], "done")

    def test_rust_client_cancel_task_returns_structured_success(self):
        class Resp:
            status_code = 200
            content = b'{"ok":true,"cancelled":true}'
            text = '{"ok":true,"cancelled":true}'

            def json(self):
                return {"ok": True, "cancelled": True}

        with patch("requests.post", return_value=Resp()):
            result = rust_client.cancel_task("task-1")

        self.assertEqual(result["ok"], True)
        self.assertEqual(result["cancelled"], True)

    def test_accel_client_run_cleaning_operator_returns_structured_success(self):
        class Resp:
            status_code = 200
            text = ""

            def json(self):
                return {"ok": True, "outputs": {}}

        with patch("requests.post", return_value=Resp()) as post:
            result = accel_client.run_cleaning_operator(
                params={},
                job_id="job-1",
                step_id="cleaning",
                actor="local",
                ruleset_version="v1",
                input_uri="in",
                output_uri="out",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["response"]["ok"], True)
        self.assertIn("/operators/cleaning", post.call_args.args[0])

    def test_accel_client_run_cleaning_operator_handles_invalid_json_body(self):
        class Resp:
            status_code = 200
            text = "not-json"

            def json(self):
                raise ValueError("bad json")

        with patch("requests.post", return_value=Resp()):
            result = accel_client.run_cleaning_operator(
                params={},
                job_id="job-1",
                step_id="cleaning",
                actor="local",
                ruleset_version="v1",
                input_uri="in",
                output_uri="out",
            )

        self.assertTrue(result["ok"])
        self.assertEqual(result["response"]["raw"], "not-json")

    def test_accel_client_transform_rows_v2_operator_validates_shape(self):
        class Resp:
            status_code = 200
            text = ""

            def json(self):
                return {"rows": "bad-shape", "quality": {}}

        with patch("requests.post", return_value=Resp()):
            result = accel_client.transform_rows_v2_operator(
                raw_rows=[{"id": 1}],
                params={},
                rules={},
                quality_gates={},
                schema_hint={},
            )

        self.assertFalse(result["ok"])
        self.assertIn("invalid response shape", result["error"])

    def test_callback_headers_ignore_user_supplied_api_key(self):
        headers = headers_from_params_impl({"api_key": "user-key"}, env_api_key="service-key")
        self.assertEqual(headers, {"X-API-Key": "service-key"})


if __name__ == "__main__":
    unittest.main()
