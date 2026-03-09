import importlib.util
from pathlib import Path
import os
import tempfile
import unittest
import logging
from unittest.mock import patch
import sys

from fastapi.testclient import TestClient

from aiwf import extensions
from aiwf.flows.registry import get_flow_registration, get_flow_runner, register_flow, unregister_flow
from aiwf.registry_events import clear_registry_events


def _load_module(module_name: str, module_path: Path):
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


PROJECT_ROOT = Path(__file__).resolve().parents[1]
glue_app = _load_module("aiwf_glue_python_app", PROJECT_ROOT / "app.py")


class AppRouteTests(unittest.TestCase):
    def setUp(self):
        self._old_level = glue_app.log.level
        self._old_httpx_level = logging.getLogger("httpx").level
        glue_app.log.setLevel(logging.CRITICAL)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        clear_registry_events()
        self.client = TestClient(glue_app.app, raise_server_exceptions=False)

    def tearDown(self):
        glue_app.log.setLevel(self._old_level)
        logging.getLogger("httpx").setLevel(self._old_httpx_level)

    def test_health(self):
        resp = self.client.get("/health")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.json(), {"ok": True})

    def test_capabilities_route_reports_registered_components(self):
        resp = self.client.get("/capabilities")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        caps = payload["capabilities"]
        self.assertIn("cleaning", caps["flows"])
        self.assertIn("txt", caps["input_formats"])
        self.assertIn("trim", caps["preprocess"]["field_transforms"])
        self.assertIn("extract", caps["preprocess"]["pipeline_stages"])
        self.assertIn("parquet_cleaned", caps["artifacts"]["core"])
        self.assertIn("xlsx_fin", caps["artifacts"]["office"])
        self.assertIn("parquet", caps["artifacts"]["selection_tokens"]["core"])
        self.assertIn("xlsx", caps["artifacts"]["selection_tokens"]["office"])
        self.assertEqual(caps["registry"]["default_conflict_policy"], "replace")
        cleaning_flow = next(item for item in caps["flow_details"] if item["name"] == "cleaning")
        self.assertTrue(cleaning_flow["source_module"].startswith("aiwf."))
        txt_reader = next(item for item in caps["input_format_details"] if item["input_format"] == "txt")
        self.assertTrue(txt_reader["source_module"].startswith("aiwf."))

    def test_unknown_flow_returns_404(self):
        resp = self.client.post(
            "/jobs/job1/run/unknown",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 404)
        self.assertFalse(resp.json()["ok"])

    @patch.object(glue_app, "_run_flow_with_runner")
    def test_cleaning_success_response_shape(self, run_flow_with_runner):
        run_flow_with_runner.return_value = {"ok": True, "custom": "value"}

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

    @patch.object(glue_app, "_run_flow_with_runner")
    def test_cleaning_exposes_office_generation_fields(self, run_flow_with_runner):
        run_flow_with_runner.return_value = {
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

    @patch.object(glue_app, "_run_flow_with_runner", side_effect=RuntimeError("boom"))
    def test_internal_error_hides_traceback_by_default(self, _run_flow_with_runner):
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
    @patch.object(glue_app, "_run_flow_with_runner", side_effect=RuntimeError("boom"))
    def test_internal_error_exposes_traceback_in_debug(self, _run_flow_with_runner):
        resp = self.client.post(
            "/jobs/job500/run/cleaning",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 500)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertIn("traceback", payload)
        self.assertEqual(payload["exception"], "boom")

    def test_custom_registered_flow_dispatches_without_editing_app(self):
        def run_custom_flow(**kwargs):
            params = kwargs.get("params") or {}
            return {"ok": True, "custom": True, "job_root": params.get("job_root")}

        register_flow("custom", runner=run_custom_flow, aliases=("custom-alias",))
        try:
            resp = self.client.post(
                "/jobs/job-custom/run/custom-alias",
                json={"actor": "local", "ruleset_version": "v1", "params": {}},
            )
        finally:
            unregister_flow("custom")

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["custom"])
        self.assertEqual(payload["flow"], "custom-alias")
        self.assertEqual(payload["job_root"], os.path.join(glue_app.settings.jobs_root, "job-custom"))

    def test_extension_loader_imports_configured_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            module_name = "aiwf_test_ext_module"
            module_path = os.path.join(tmp, f"{module_name}.py")
            with open(module_path, "w", encoding="utf-8") as f:
                f.write(
                    "from aiwf.flows.registry import register_flow\n"
                    "register_flow('ext_flow', runner=lambda **kwargs: {'ok': True, 'ext': True})\n"
                )

            sys.path.insert(0, tmp)
            unregister_flow("ext_flow")
            try:
                with patch.dict("os.environ", {"AIWF_EXT_MODULES": module_name}, clear=False):
                    extensions.reset_extension_state_for_tests()
                    status = extensions.load_extension_modules(force=True)
            finally:
                if tmp in sys.path:
                    sys.path.remove(tmp)

            self.assertIn(module_name, status["loaded"])
            caps = self.client.get("/capabilities").json()["capabilities"]
            self.assertIn("ext_flow", caps["flows"])
            ext_flow = next(item for item in caps["flow_details"] if item["name"] == "ext_flow")
            self.assertEqual(ext_flow["source_module"], module_name)
            unregister_flow("ext_flow")

    def test_extension_loader_force_reload_reexecutes_module_registration(self):
        with tempfile.TemporaryDirectory() as tmp:
            module_name = "aiwf_test_ext_reload_module"
            module_path = os.path.join(tmp, f"{module_name}.py")
            with open(module_path, "w", encoding="utf-8") as f:
                f.write(
                    "from aiwf.flows.registry import register_flow\n"
                    "register_flow('ext_reload_flow', runner=lambda **kwargs: {'ok': True, 'reloaded': True})\n"
                )

            sys.path.insert(0, tmp)
            unregister_flow("ext_reload_flow")
            try:
                with patch.dict("os.environ", {"AIWF_EXT_MODULES": module_name}, clear=False):
                    extensions.reset_extension_state_for_tests()
                    extensions.load_extension_modules(force=True)
                    unregister_flow("ext_reload_flow")
                    status = extensions.load_extension_modules(force=True)
            finally:
                if tmp in sys.path:
                    sys.path.remove(tmp)

            self.assertIn(module_name, status["loaded"])
            self.assertTrue(get_flow_runner("ext_reload_flow")()["reloaded"])
            unregister_flow("ext_reload_flow")

    def test_register_flow_keep_policy_preserves_existing_runner(self):
        def first_runner(**kwargs):
            return {"ok": True, "runner": "first"}

        def second_runner(**kwargs):
            return {"ok": True, "runner": "second"}

        register_flow("keep_flow", runner=first_runner)
        try:
            kept = register_flow("keep_flow", runner=second_runner, on_conflict="keep")
            self.assertEqual(kept.source_module, __name__)
            runner = get_flow_runner("keep_flow")
            self.assertIs(runner, first_runner)
        finally:
            unregister_flow("keep_flow")

    def test_register_flow_error_policy_raises(self):
        register_flow("error_flow", runner=lambda **kwargs: {"ok": True})
        try:
            with self.assertRaises(RuntimeError):
                register_flow("error_flow", runner=lambda **kwargs: {"ok": True}, on_conflict="error")
        finally:
            unregister_flow("error_flow")

    def test_capabilities_reports_registry_conflict_events(self):
        register_flow("warn_flow", runner=lambda **kwargs: {"ok": True})
        try:
            register_flow("warn_flow", runner=lambda **kwargs: {"ok": True}, on_conflict="warn")
            caps = self.client.get("/capabilities").json()["capabilities"]
            events = caps["registry"]["events"]
            match = [e for e in events if e["registry"] == "flow" and e["name"] == "warn_flow"]
            self.assertTrue(match)
            self.assertEqual(match[-1]["policy"], "warn")
        finally:
            unregister_flow("warn_flow")

    def test_register_flow_rejects_alias_that_matches_existing_flow_name(self):
        register_flow("alias-root", runner=lambda **kwargs: {"ok": True})
        try:
            with self.assertRaises(RuntimeError):
                register_flow("alias-child", runner=lambda **kwargs: {"ok": True}, aliases=("alias-root",))
        finally:
            unregister_flow("alias-root")
            unregister_flow("alias-child")

    def test_register_flow_reassigns_shared_alias_without_hiding_original_flow(self):
        def flow_a(**kwargs):
            return {"runner": "a"}

        def flow_b(**kwargs):
            return {"runner": "b"}

        register_flow("flow-a", runner=flow_a, aliases=("shared-alias",))
        try:
            register_flow("flow-b", runner=flow_b, aliases=("shared-alias",), on_conflict="warn")
            self.assertEqual(get_flow_runner("shared-alias")()["runner"], "b")
            self.assertEqual(get_flow_runner("flow-a")()["runner"], "a")
            self.assertEqual(get_flow_registration("flow-a").aliases, ())
        finally:
            unregister_flow("flow-a")
            unregister_flow("flow-b")

    @patch.object(glue_app, "make_base_client", return_value=None)
    @patch("aiwf.flows.cleaning.run_cleaning", return_value={"ok": True})
    def test_run_cleaning_flow_injects_default_job_root(self, run_cleaning, _make_base_client):
        req = glue_app.RunReq(actor="local", ruleset_version="v1", params={"x": 1})

        glue_app.run_cleaning_flow("job-root", req)

        kwargs = run_cleaning.call_args.kwargs
        self.assertEqual(kwargs["params"]["x"], 1)
        self.assertEqual(
            kwargs["params"]["job_root"],
            os.path.join(glue_app.settings.jobs_root, "job-root"),
        )
        self.assertEqual(
            kwargs["params"]["job_context"]["job_root"],
            os.path.join(glue_app.settings.jobs_root, "job-root"),
        )

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    @patch("aiwf.flows.cleaning.run_cleaning", return_value={"ok": True})
    def test_run_cleaning_flow_prefers_explicit_job_context_and_trace_id(self, run_cleaning, _make_base_client):
        req = glue_app.RunReq(
            actor="local",
            ruleset_version="v1",
            trace_id="trace-123",
            job_context={
                "job_root": r"D:\ctx\job",
                "stage_dir": r"D:\ctx\job\stage",
                "artifacts_dir": r"D:\ctx\job\artifacts",
                "evidence_dir": r"D:\ctx\job\evidence",
            },
            params={"job_root": r"D:\legacy\job", "x": 1},
        )

        glue_app.run_cleaning_flow("job-root", req)

        kwargs = run_cleaning.call_args.kwargs
        self.assertEqual(kwargs["params"]["job_root"], os.path.normpath(r"D:\ctx\job"))
        self.assertEqual(kwargs["params"]["job_context"]["stage_dir"], os.path.normpath(r"D:\ctx\job\stage"))
        self.assertEqual(kwargs["params"]["job_context"]["artifacts_dir"], os.path.normpath(r"D:\ctx\job\artifacts"))
        self.assertEqual(kwargs["params"]["job_context"]["evidence_dir"], os.path.normpath(r"D:\ctx\job\evidence"))
        self.assertEqual(kwargs["params"]["trace_id"], "trace-123")

    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_with_runner_propagates_runner_typeerror_without_retry(self, _make_base_client):
        calls = []

        def runner(**kwargs):
            calls.append(kwargs)
            raise TypeError("runner bug")

        req = glue_app.RunReq(actor="local", ruleset_version="v1", params={"x": 1})

        with self.assertRaisesRegex(TypeError, "runner bug"):
            glue_app._run_flow_with_runner("job-typeerror", req, runner)

        self.assertEqual(len(calls), 1)

    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_with_runner_supports_params_json_signature(self, _make_base_client):
        captured = {}

        def runner(job_id, actor, ruleset_version, s, base, params_json):
            captured["job_id"] = job_id
            captured["actor"] = actor
            captured["ruleset_version"] = ruleset_version
            captured["params_json"] = params_json
            return {"ok": True}

        req = glue_app.RunReq(actor="local", ruleset_version="v1", params={"x": 1})
        result = glue_app._run_flow_with_runner("job-json", req, runner)

        self.assertTrue(result["ok"])
        self.assertEqual(captured["job_id"], "job-json")
        self.assertEqual(captured["actor"], "local")
        self.assertEqual(captured["ruleset_version"], "v1")
        self.assertIn('"x": 1', captured["params_json"])

    def test_make_base_client_supports_timeout_keyword_signature(self):
        captured = {}

        class ClientWithTimeout:
            def __init__(self, base_url, *, api_key=None, timeout=30):
                captured["base_url"] = base_url
                captured["api_key"] = api_key
                captured["timeout"] = timeout

        with patch("aiwf.base_client.BaseClient", new=ClientWithTimeout):
            client = glue_app.make_base_client()

        self.assertIsInstance(client, ClientWithTimeout)
        self.assertEqual(captured["base_url"], glue_app.settings.base_url)
        self.assertEqual(captured["api_key"], glue_app.settings.api_key)
        self.assertEqual(captured["timeout"], glue_app.settings.timeout_seconds)

    def test_make_base_client_propagates_constructor_typeerror_without_retry(self):
        calls = []

        class BuggyClient:
            def __init__(self, base_url, api_key=None):
                calls.append((base_url, api_key))
                raise TypeError("constructor bug")

        with patch("aiwf.base_client.BaseClient", new=BuggyClient):
            with self.assertRaisesRegex(TypeError, "constructor bug"):
                glue_app.make_base_client()

        self.assertEqual(len(calls), 1)

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_route_executes_cleaning_end_to_end(self, _make_base_client):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")

            def write_valid_parquet(path, rows):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(b"PAR1dataPAR1")

            with patch("aiwf.flows.cleaning._base_step_start") as step_start, patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ) as artifact_upsert, patch("aiwf.flows.cleaning._base_step_done") as step_done, patch(
                "aiwf.flows.cleaning._base_step_fail"
            ) as step_fail, patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ), patch(
                "aiwf.flows.cleaning._require_local_parquet_dependencies"
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_parquet", side_effect=write_valid_parquet
            ):
                resp = self.client.post(
                    "/jobs/job-e2e/run/cleaning",
                    json={
                        "actor": "local",
                        "ruleset_version": "v1",
                        "params": {
                            "job_root": local_job_root,
                            "rows": [{"id": 1, "amount": 10.0}],
                            "office_outputs_enabled": False,
                        },
                    },
                )

            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertTrue(payload["ok"])
            self.assertEqual(payload["job_id"], "job-e2e")
            self.assertEqual(payload["flow"], "cleaning")
            self.assertGreaterEqual(len(payload["artifacts"]), 2)
            parquet_artifact = next(item for item in payload["artifacts"] if item["kind"] == "parquet")
            self.assertTrue(parquet_artifact["path"].startswith(local_job_root))
            step_start.assert_called_once()
            step_done.assert_called_once()
            step_fail.assert_not_called()
            self.assertGreaterEqual(artifact_upsert.call_count, 2)

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_route_prefers_job_context_over_legacy_job_root(self, _make_base_client):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "ctx-job")
            legacy_job_root = os.path.join(tmp, "legacy-job")

            def write_valid_parquet(path, rows):
                os.makedirs(os.path.dirname(path), exist_ok=True)
                with open(path, "wb") as f:
                    f.write(b"PAR1dataPAR1")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ), patch(
                "aiwf.flows.cleaning._require_local_parquet_dependencies"
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_parquet", side_effect=write_valid_parquet
            ):
                resp = self.client.post(
                    "/jobs/job-ctx-route/run/cleaning",
                    json={
                        "actor": "local",
                        "ruleset_version": "v1",
                        "trace_id": "trace-route-1",
                        "job_context": {
                            "job_root": local_job_root,
                            "stage_dir": os.path.join(local_job_root, "stage-x"),
                            "artifacts_dir": os.path.join(local_job_root, "artifacts-x"),
                            "evidence_dir": os.path.join(local_job_root, "evidence-x"),
                        },
                        "params": {
                            "job_root": legacy_job_root,
                            "rows": [{"id": 1, "amount": 10.0}],
                            "office_outputs_enabled": False,
                        },
                    },
                )

            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertTrue(payload["ok"])
            parquet_artifact = next(item for item in payload["artifacts"] if item["kind"] == "parquet")
            self.assertTrue(parquet_artifact["path"].startswith(local_job_root))
            self.assertFalse(parquet_artifact["path"].startswith(legacy_job_root))

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_route_reports_cleaning_failures_via_step_fail(self, _make_base_client):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")

            with patch("aiwf.flows.cleaning._base_step_start") as step_start, patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ) as artifact_upsert, patch("aiwf.flows.cleaning._base_step_done") as step_done, patch(
                "aiwf.flows.cleaning._base_step_fail"
            ) as step_fail, patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ), patch(
                "aiwf.flows.cleaning._require_local_parquet_dependencies"
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_parquet", side_effect=RuntimeError("disk full")
            ):
                resp = self.client.post(
                    "/jobs/job-e2e-fail/run/cleaning",
                    json={
                        "actor": "local",
                        "ruleset_version": "v1",
                        "params": {
                            "job_root": local_job_root,
                            "rows": [{"id": 1, "amount": 10.0}],
                            "office_outputs_enabled": False,
                        },
                    },
                )

            self.assertEqual(resp.status_code, 500)
            payload = resp.json()
            self.assertFalse(payload["ok"])
            self.assertEqual(payload["error"], "internal server error")
            step_start.assert_called_once()
            step_fail.assert_called_once()
            step_done.assert_not_called()
            artifact_upsert.assert_not_called()


if __name__ == "__main__":
    unittest.main()
