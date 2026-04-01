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
from aiwf.governance_surface import (
    GOVERNANCE_CONTROL_PLANE_ROLE,
    GOVERNANCE_SURFACE_META_ROUTE,
    list_governance_surface_entries,
    validate_governance_surface_entries,
)
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


def make_job_context(job_root: str) -> dict[str, str]:
    job_root = os.path.normpath(job_root)
    return {
        "job_root": job_root,
        "stage_dir": os.path.join(job_root, "stage"),
        "artifacts_dir": os.path.join(job_root, "artifacts"),
        "evidence_dir": os.path.join(job_root, "evidence"),
    }


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
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertIn("dependencies", payload)
        self.assertIn("ingest_sidecar", payload)
        self.assertEqual(payload["ingest_sidecar"]["contract"], "contracts/glue/ingest_extract.schema.json")
        self.assertEqual(payload["ingest_sidecar"]["supported_modalities"], ["txt", "docx", "pdf", "image", "xlsx"])

    def test_ingest_extract_route_returns_rows_and_quality_state(self):
        with patch.object(glue_app.ingest, "load_rows_from_file") as load_rows:
            load_rows.return_value = (
                [{"source_type": "image", "text": "claim"}],
                {
                    "input_format": "image",
                    "quality_blocked": False,
                    "quality_report": {"ok": True},
                    "image_blocks": [{"block_id": "img_blk_1"}],
                    "quality_metrics": {"ocr_confidence_avg": 0.95},
                    "engine_trace": [{"engine": "docling", "ok": True}],
                },
            )
            resp = self.client.post(
                "/ingest/extract",
                json={
                    "input_path": r"D:\data\sample.png",
                    "image_rules": {"ocr_confidence_avg_min": 0.8},
                },
            )

        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        self.assertFalse(payload["quality_blocked"])
        self.assertEqual(payload["contract"], "contracts/glue/ingest_extract.schema.json")
        self.assertEqual(len(payload["rows"]), 1)
        self.assertEqual(payload["file_results"][0]["path"], r"D:\data\sample.png")
        self.assertEqual(payload["file_results"][0]["rows"][0]["text"], "claim")
        self.assertEqual(payload["file_results"][0]["row_count"], 1)
        self.assertIn("image_blocks", payload["file_results"][0])
        self.assertIn("quality_report", payload["file_results"][0])
        self.assertIn("header_mapping", payload)
        self.assertIn("candidate_profiles", payload)
        self.assertIn("quality_decisions", payload)
        self.assertIn("blocked_reason_codes", payload)
        self.assertIn("sample_rows", payload)

    def test_capabilities_route_reports_registered_components(self):
        resp = self.client.get("/capabilities")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        caps = payload["capabilities"]
        self.assertEqual(caps["ingest_sidecar"]["contract"], "contracts/glue/ingest_extract.schema.json")
        self.assertEqual(caps["cleaning_spec_v2"]["contract"], "contracts/glue/cleaning_spec.v2.schema.json")
        self.assertIn("finance_statement", caps["cleaning_spec_v2"]["profiles"])
        self.assertIn("cleaning", caps["flows"])
        self.assertNotIn("workflow_reference", caps["flows"])
        self.assertIn("txt", caps["input_formats"])
        self.assertIn("dependencies", caps)
        self.assertIn("docling", caps["dependencies"])
        self.assertEqual(caps["input_domains"][0]["name"], "ingest")
        self.assertIn("trim", caps["preprocess"]["field_transforms"])
        self.assertEqual(caps["preprocess"]["field_transform_domains"][0]["name"], "preprocess")
        self.assertEqual(caps["preprocess"]["row_filter_domains"][0]["name"], "preprocess")
        self.assertIn("extract", caps["preprocess"]["pipeline_stages"])
        self.assertEqual(caps["preprocess"]["pipeline_stage_domains"][0]["name"], "preprocess")
        self.assertIn("parquet_cleaned", caps["artifacts"]["core"])
        self.assertEqual(caps["artifacts"]["core_domains"][0]["name"], "cleaning-core")
        self.assertIn("xlsx_fin", caps["artifacts"]["office"])
        self.assertEqual(caps["artifacts"]["office_domains"][0]["name"], "cleaning-office")
        self.assertEqual(caps["governance"]["quality_rule_sets"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["quality_rule_sets"]["schema_version"], "quality_rule_set.v1")
        self.assertEqual(caps["governance"]["surface_schema_version"], "governance_surface.v1")
        self.assertEqual(caps["governance"]["control_plane_status"], "effective_second_control_plane")
        self.assertEqual(caps["governance"]["control_plane_role"], GOVERNANCE_CONTROL_PLANE_ROLE)
        self.assertEqual(caps["governance"]["governance_state_control_plane_owner"], "glue-python")
        self.assertEqual(caps["governance"]["job_lifecycle_control_plane_owner"], "base-java")
        self.assertEqual(caps["governance"]["operator_semantics_authority_owner"], "accel-rust")
        self.assertEqual(caps["governance"]["workflow_authoring_surface_owner"], "dify-desktop")
        self.assertEqual(caps["governance"]["workflow_sandbox_rules"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["workflow_sandbox_rules"]["schema_version"], "workflow_sandbox_alert_rules.v1")
        self.assertEqual(caps["governance"]["workflow_sandbox_rules"]["control_plane_role"], GOVERNANCE_CONTROL_PLANE_ROLE)
        self.assertFalse(caps["governance"]["workflow_sandbox_rules"]["lifecycle_mutation_allowed"])
        self.assertEqual(
            caps["governance"]["workflow_sandbox_rules"]["owned_route_prefixes"],
            ["/governance/workflow-sandbox/rules", "/governance/workflow-sandbox/rule-versions"],
        )
        self.assertEqual(caps["governance"]["workflow_sandbox_autofix"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["workflow_sandbox_autofix"]["schema_version"], "workflow_sandbox_autofix_state.v1")
        self.assertEqual(caps["governance"]["workflow_apps"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["workflow_apps"]["schema_version"], "workflow_app_registry_entry.v1")
        self.assertEqual(caps["governance"]["workflow_versions"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["workflow_versions"]["schema_version"], "workflow_version_snapshot.v1")
        self.assertEqual(caps["governance"]["manual_reviews"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["manual_reviews"]["schema_version"], "manual_review_item.v1")
        self.assertEqual(caps["governance"]["run_baselines"]["owner"], "glue-python")
        self.assertEqual(caps["governance"]["run_baselines"]["schema_version"], "run_baseline_entry.v1")
        self.assertEqual(caps["governance_surface"]["schema_version"], "governance_surface.v1")
        self.assertEqual(caps["governance_surface"]["status"], "effective_second_control_plane")
        self.assertEqual(caps["governance_surface"]["control_plane_role"], GOVERNANCE_CONTROL_PLANE_ROLE)
        workflow_versions_surface = next(item for item in caps["governance_surface"]["items"] if item["capability"] == "workflow_versions")
        self.assertEqual(workflow_versions_surface["route_prefix"], "/governance/workflow-versions")
        self.assertEqual(workflow_versions_surface["owned_route_prefixes"], ["/governance/workflow-versions"])
        self.assertFalse(workflow_versions_surface["lifecycle_mutation_allowed"])
        self.assertEqual(workflow_versions_surface["job_lifecycle_control_plane_owner"], "base-java")
        self.assertEqual(caps["control_plane_boundary"]["status"], "effective_second_control_plane")
        self.assertEqual(caps["control_plane_boundary"]["control_plane_role"], GOVERNANCE_CONTROL_PLANE_ROLE)
        self.assertEqual(caps["control_plane_boundary"]["governance_state_control_plane_owner"], "glue-python")
        self.assertEqual(caps["control_plane_boundary"]["job_lifecycle_control_plane_owner"], "base-java")
        self.assertEqual(caps["control_plane_boundary"]["operator_semantics_authority_owner"], "accel-rust")
        self.assertEqual(caps["control_plane_boundary"]["meta_route"], GOVERNANCE_SURFACE_META_ROUTE)
        self.assertGreaterEqual(len(caps["control_plane_boundary"]["governance_surfaces"]), 7)
        self.assertIn("parquet", caps["artifacts"]["selection_tokens"]["core"])
        self.assertIn("xlsx", caps["artifacts"]["selection_tokens"]["office"])
        self.assertEqual(caps["registry"]["default_conflict_policy"], "replace")
        cleaning_flow = next(item for item in caps["flow_details"] if item["name"] == "cleaning")
        self.assertEqual(cleaning_flow["domain"], "cleaning")
        self.assertEqual(cleaning_flow["domain_metadata"]["backend"], "python")
        self.assertTrue(cleaning_flow["domain_metadata"]["builtin"])
        self.assertTrue(cleaning_flow["source_module"].startswith("aiwf."))
        cleaning_domain = next(item for item in caps["flow_domains"] if item["name"] == "cleaning")
        self.assertEqual(cleaning_domain["flow_names"], ["cleaning"])
        self.assertEqual(cleaning_domain["label"], "Cleaning")
        txt_reader = next(item for item in caps["input_format_details"] if item["input_format"] == "txt")
        self.assertTrue(txt_reader["source_module"].startswith("aiwf."))

    def test_governance_control_plane_route_reports_split_explicitly(self):
        resp = self.client.get("/governance/meta/control-plane")
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertTrue(payload["ok"])
        boundary = payload["boundary"]
        self.assertEqual(boundary["schema_version"], "governance_surface.v1")
        self.assertEqual(boundary["status"], "effective_second_control_plane")
        self.assertEqual(boundary["control_plane_role"], GOVERNANCE_CONTROL_PLANE_ROLE)
        self.assertEqual(boundary["governance_state_control_plane_owner"], "glue-python")
        self.assertEqual(boundary["job_lifecycle_control_plane_owner"], "base-java")
        self.assertEqual(boundary["operator_semantics_authority_owner"], "accel-rust")
        self.assertEqual(boundary["workflow_authoring_surface_owner"], "dify-desktop")
        self.assertEqual(boundary["meta_route"], GOVERNANCE_SURFACE_META_ROUTE)
        workflow_versions = next(item for item in boundary["governance_surfaces"] if item["capability"] == "workflow_versions")
        self.assertEqual(workflow_versions["route_prefix"], "/governance/workflow-versions")
        self.assertEqual(workflow_versions["owned_route_prefixes"], ["/governance/workflow-versions"])
        self.assertEqual(workflow_versions["state_owner"], "glue-python")
        self.assertEqual(workflow_versions["job_lifecycle_control_plane_owner"], "base-java")
        self.assertFalse(workflow_versions["lifecycle_mutation_allowed"])

    def test_governance_surface_entries_validate_cleanly(self):
        entries = list_governance_surface_entries()
        self.assertGreaterEqual(len(entries), 7)
        self.assertEqual(validate_governance_surface_entries(entries), [])
        self.assertTrue(all(item["control_plane_role"] == GOVERNANCE_CONTROL_PLANE_ROLE for item in entries))
        self.assertTrue(all(item["route_prefix"].startswith("/governance/") for item in entries))
        self.assertTrue(all(not item["lifecycle_mutation_allowed"] for item in entries))

    def test_unknown_flow_returns_404(self):
        resp = self.client.post(
            "/jobs/job1/run/unknown",
            json={"actor": "local", "ruleset_version": "v1", "params": {}},
        )
        self.assertEqual(resp.status_code, 404)
        self.assertFalse(resp.json()["ok"])

    def test_quality_rule_set_routes_store_backend_owned_sets(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.get("/governance/quality-rule-sets")
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["sets"], [])

                save_resp = self.client.put(
                    "/governance/quality-rule-sets/finance_default",
                    json={
                        "set": {
                            "name": "Finance Default",
                            "version": "v2",
                            "scope": "workflow",
                            "rules": {"required_columns": ["amount"]},
                        }
                    },
                )
                self.assertEqual(save_resp.status_code, 200)
                saved = save_resp.json()["set"]
                self.assertEqual(saved["id"], "finance_default")
                self.assertEqual(saved["owner"], "glue-python")
                self.assertEqual(saved["schema_version"], "quality_rule_set.v1")

                get_resp = self.client.get("/governance/quality-rule-sets/finance_default")
                self.assertEqual(get_resp.status_code, 200)
                self.assertEqual(
                    get_resp.json()["set"]["rules"],
                    {"required_columns": ["amount"]},
                )

                list_resp = self.client.get("/governance/quality-rule-sets")
                self.assertEqual(list_resp.status_code, 200)
                self.assertEqual(len(list_resp.json()["sets"]), 1)

                delete_resp = self.client.delete("/governance/quality-rule-sets/finance_default")
                self.assertEqual(delete_resp.status_code, 200)
                self.assertTrue(delete_resp.json()["ok"])

                missing_resp = self.client.get("/governance/quality-rule-sets/finance_default")
                self.assertEqual(missing_resp.status_code, 404)
                self.assertFalse(missing_resp.json()["ok"])

    def test_quality_rule_set_routes_reject_invalid_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.put(
                    "/governance/quality-rule-sets/not valid",
                    json={"set": {"rules": {}}},
                )
                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["error_code"], "governance_validation_invalid")
                self.assertEqual(payload["error_scope"], "quality_rule_set")
                self.assertEqual(payload["error_item_contract"], "contracts/desktop/node_config_validation_errors.v1.json")
                self.assertTrue(any(item["path"] == "set.id" for item in payload["error_items"]))

    def test_workflow_sandbox_rule_routes_store_backend_owned_rules_and_versions(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.get("/governance/workflow-sandbox/rules")
                self.assertEqual(resp.status_code, 200)
                self.assertEqual(resp.json()["provider"], "glue-python")
                self.assertEqual(resp.json()["rules"]["whitelist_codes"], [])

                save_resp = self.client.put(
                    "/governance/workflow-sandbox/rules",
                    json={
                        "rules": {
                            "whitelist_codes": ["sandbox_limit_exceeded:output"],
                            "whitelist_node_types": ["ai_refine"],
                            "mute_until_by_key": {
                                "ai_refine::*::*": "2026-03-22T00:00:00Z",
                            },
                        },
                        "meta": {"reason": "set_rules"},
                    },
                )
                self.assertEqual(save_resp.status_code, 200)
                saved = save_resp.json()
                self.assertTrue(saved["version_id"])
                self.assertEqual(saved["rules"]["whitelist_codes"], ["sandbox_limit_exceeded:output"])

                versions_resp = self.client.get("/governance/workflow-sandbox/rule-versions")
                self.assertEqual(versions_resp.status_code, 200)
                items = versions_resp.json()["items"]
                self.assertEqual(len(items), 1)
                self.assertEqual(items[0]["meta"]["reason"], "set_rules")

                rollback_resp = self.client.post(
                    f"/governance/workflow-sandbox/rule-versions/{items[0]['version_id']}/rollback"
                )
                self.assertEqual(rollback_resp.status_code, 200)
                self.assertTrue(rollback_resp.json()["version_id"])
                self.assertEqual(
                    rollback_resp.json()["rules"]["whitelist_node_types"],
                    ["ai_refine"],
                )

    def test_workflow_sandbox_rule_routes_reject_missing_version_on_rollback(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.post("/governance/workflow-sandbox/rule-versions/missing/rollback")
                self.assertEqual(resp.status_code, 404)
                self.assertFalse(resp.json()["ok"])

    def test_workflow_sandbox_autofix_routes_store_backend_owned_state(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                get_resp = self.client.get("/governance/workflow-sandbox/autofix-state")
                self.assertEqual(get_resp.status_code, 200)
                self.assertEqual(get_resp.json()["state"]["green_streak"], 0)

                put_resp = self.client.put(
                    "/governance/workflow-sandbox/autofix-state",
                    json={
                        "violation_events": [{"run_id": "run_1"}],
                        "forced_isolation_mode": "process",
                        "forced_until": "2026-03-22T01:00:00Z",
                        "last_actions": [{"ts": "2026-03-22T00:10:00Z", "actions": ["pause_queue"]}],
                        "green_streak": 2,
                    },
                )
                self.assertEqual(put_resp.status_code, 200)
                self.assertEqual(put_resp.json()["state"]["forced_isolation_mode"], "process")

                actions_resp = self.client.get("/governance/workflow-sandbox/autofix-actions?limit=20")
                self.assertEqual(actions_resp.status_code, 200)
                self.assertEqual(actions_resp.json()["forced_isolation_mode"], "process")
                self.assertEqual(len(actions_resp.json()["items"]), 1)

    def test_workflow_app_routes_store_backend_owned_registry_entries(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                empty_resp = self.client.get("/governance/workflow-apps")
                self.assertEqual(empty_resp.status_code, 200)
                self.assertEqual(empty_resp.json()["items"], [])

                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    return_value={
                        "ok": True,
                        "normalized_workflow_definition": {
                            "workflow_id": "wf_finance",
                            "version": "workflow.v1",
                            "nodes": [],
                            "edges": [],
                        },
                    },
                ):
                    version_resp = self.client.put(
                        "/governance/workflow-versions/ver_finance_a",
                        json={
                            "version": {
                            "workflow_name": "Finance Flow",
                            "workflow_definition": {
                                "workflow_id": "wf_finance",
                                "version": "workflow.v1",
                                "nodes": [],
                                "edges": [],
                                },
                            }
                        },
                    )
                self.assertEqual(version_resp.status_code, 200)

                save_resp = self.client.put(
                    "/governance/workflow-apps/finance_app",
                    json={
                        "app": {
                            "name": "Finance App",
                            "workflow_id": "wf_finance",
                            "published_version_id": "ver_finance_a",
                            "params_schema": {"region": {"type": "string"}},
                            "template_policy": {"version": 1, "governance": {"mode": "strict"}},
                        }
                    },
                )
                self.assertEqual(save_resp.status_code, 200)
                item = save_resp.json()["item"]
                self.assertEqual(item["app_id"], "finance_app")
                self.assertEqual(item["owner"], "glue-python")
                self.assertEqual(item["schema_version"], "workflow_app_registry_entry.v1")
                self.assertEqual(item["published_version_id"], "ver_finance_a")
                self.assertEqual(item["template_policy"]["version"], 1)
                self.assertNotIn("graph", item)
                self.assertNotIn("workflow_definition", item)

                get_resp = self.client.get("/governance/workflow-apps/finance_app")
                self.assertEqual(get_resp.status_code, 200)
                self.assertEqual(get_resp.json()["item"]["params_schema"]["region"]["type"], "string")
                self.assertNotIn("graph", get_resp.json()["item"])
                self.assertNotIn("workflow_definition", get_resp.json()["item"])

                list_resp = self.client.get("/governance/workflow-apps")
                self.assertEqual(list_resp.status_code, 200)
                self.assertEqual(len(list_resp.json()["items"]), 1)
                self.assertNotIn("graph", list_resp.json()["items"][0])
                self.assertNotIn("workflow_definition", list_resp.json()["items"][0])

    def test_workflow_app_routes_reject_missing_published_version_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.put(
                    "/governance/workflow-apps/bad_app",
                    json={
                        "app": {
                            "name": "Bad App",
                        }
                    },
                )
                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["error_code"], "governance_validation_invalid")
                self.assertEqual(payload["error_scope"], "workflow_app")
                self.assertTrue("published_version_id" in payload["error"])

    def test_workflow_app_routes_reject_unknown_published_version_reference(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.put(
                    "/governance/workflow-apps/bad_app_missing_version",
                    json={
                        "app": {
                            "name": "Bad App Missing Version",
                            "workflow_id": "wf_bad_app_contract",
                            "published_version_id": "ver_missing",
                        }
                    },
                )
                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["error_code"], "governance_validation_invalid")
                self.assertEqual(payload["error_scope"], "workflow_app")
                self.assertTrue("published_version_id not found" in payload["error"])

    def test_workflow_version_routes_store_and_compare_backend_owned_snapshots(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                empty_resp = self.client.get("/governance/workflow-versions")
                self.assertEqual(empty_resp.status_code, 200)
                self.assertEqual(empty_resp.json()["items"], [])

                version_a = {
                    "workflow_id": "wf_finance",
                    "version": "workflow.v1",
                    "nodes": [{"id": "n1", "type": "ingest_files"}],
                    "edges": [],
                }
                version_b = {
                    "workflow_id": "wf_finance",
                    "version": "workflow.v1",
                    "nodes": [{"id": "n1", "type": "ingest_files"}, {"id": "n2", "type": "quality_check_v3"}],
                    "edges": [{"from": "n1", "to": "n2"}],
                }

                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    side_effect=[
                        {"ok": True, "normalized_workflow_definition": version_a},
                        {"ok": True, "normalized_workflow_definition": version_b},
                    ],
                ):
                    put_a = self.client.put(
                        "/governance/workflow-versions/ver_a",
                        json={"version": {"workflow_name": "Finance Flow", "workflow_definition": version_a}},
                    )
                    self.assertEqual(put_a.status_code, 200)
                    self.assertEqual(put_a.json()["item"]["owner"], "glue-python")

                    put_b = self.client.put(
                        "/governance/workflow-versions/ver_b",
                        json={"version": {"workflow_name": "Finance Flow", "workflow_definition": version_b}},
                    )
                self.assertEqual(put_b.status_code, 200)

                list_resp = self.client.get("/governance/workflow-versions?workflow_name=Finance%20Flow")
                self.assertEqual(list_resp.status_code, 200)
                self.assertEqual(len(list_resp.json()["items"]), 2)
                self.assertNotIn("graph", list_resp.json()["items"][0])

                get_resp = self.client.get("/governance/workflow-versions/ver_b")
                self.assertEqual(get_resp.status_code, 200)
                self.assertNotIn("graph", get_resp.json()["item"])
                self.assertEqual(get_resp.json()["item"]["workflow_definition"]["nodes"][1]["type"], "quality_check_v3")

                compare_resp = self.client.post(
                    "/governance/workflow-versions/compare",
                    json={"version_a": "ver_a", "version_b": "ver_b"},
                )
                self.assertEqual(compare_resp.status_code, 200)
                compare = compare_resp.json()
                self.assertTrue(compare["ok"])
                self.assertEqual(compare["provider"], "glue-python")
                self.assertEqual(compare["summary"]["changed_nodes"], 1)
                self.assertEqual(compare["summary"]["added_edges"], 1)

    def test_workflow_version_routes_reject_invalid_graph_contract(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    side_effect=glue_app.WorkflowValidationFailure(
                        "workflow.version is required",
                        error_items=[{
                            "path": "workflow.version",
                            "code": "required",
                            "message": "workflow.version is required",
                        }],
                    ),
                ):
                    resp = self.client.put(
                        "/governance/workflow-versions/ver_bad",
                        json={"version": {"graph": {"workflow_id": "wf_only"}}},
                    )
                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["error_code"], "workflow_graph_invalid")
                self.assertEqual(payload["error_scope"], "workflow_version")
                self.assertEqual(payload["graph_contract"], "contracts/workflow/workflow.schema.json")
                self.assertEqual(payload["error_item_contract"], "contracts/desktop/node_config_validation_errors.v1.json")
                self.assertTrue(any(item["path"] == "workflow.version" and item["code"] == "required" for item in payload["error_items"]))

    def test_workflow_version_routes_reject_unregistered_node_types(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    side_effect=glue_app.WorkflowValidationFailure(
                        "workflow contains unregistered node types: unknown_future_node",
                        error_items=[{
                            "path": "workflow.nodes",
                            "code": "unknown_node_type",
                            "message": "workflow contains unregistered node types: unknown_future_node",
                        }],
                    ),
                ):
                    resp = self.client.put(
                        "/governance/workflow-versions/ver_bad_unknown_type",
                        json={
                            "version": {
                                "graph": {
                                    "workflow_id": "wf_bad_unknown_type",
                                    "version": "workflow.v1",
                                    "nodes": [{"id": "n1", "type": "unknown_future_node"}],
                                    "edges": [],
                                }
                            }
                        },
                    )
                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error_code"], "workflow_graph_invalid")
                self.assertEqual(payload["error_scope"], "workflow_version")
                self.assertTrue(any(item["path"] == "workflow.nodes" and item["code"] == "unknown_node_type" for item in payload["error_items"]))

    def test_workflow_version_routes_accept_node_config_semantics_owned_by_desktop(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                graph = {
                    "workflow_id": "wf_bad_version_contract",
                    "version": "workflow.v1",
                    "nodes": [
                        {
                            "id": "n1",
                            "type": "parquet_io_v2",
                            "config": {
                                "op": "read",
                                "path": "demo.parquet",
                                "predicate_eq": 1,
                            },
                        }
                    ],
                    "edges": [],
                }
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    return_value={"ok": True, "normalized_workflow_definition": graph},
                ):
                    resp = self.client.put(
                        "/governance/workflow-versions/ver_bad_contract",
                        json={"version": {"workflow_definition": graph}},
                    )
                self.assertEqual(resp.status_code, 200)
                payload = resp.json()
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["item"]["owner"], "glue-python")
                self.assertNotIn("graph", payload["item"])
                self.assertEqual(payload["item"]["workflow_definition"]["nodes"][0]["type"], "parquet_io_v2")
                self.assertEqual(payload["item"]["workflow_definition"]["nodes"][0]["config"]["predicate_eq"], 1)

    def test_workflow_version_routes_fail_closed_when_rust_validation_unavailable(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    side_effect=glue_app.WorkflowValidationUnavailable("workflow validation unavailable: connection refused"),
                ):
                    resp = self.client.put(
                        "/governance/workflow-versions/ver_unavailable",
                        json={
                            "version": {
                                "graph": {
                                    "workflow_id": "wf_unavailable",
                                    "version": "workflow.v1",
                                    "nodes": [{"id": "n1", "type": "ingest_files"}],
                                    "edges": [],
                                }
                            }
                        },
                    )
                self.assertEqual(resp.status_code, 503)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["provider"], "glue-python")
                self.assertEqual(payload["error_code"], "workflow_validation_unavailable")
                self.assertEqual(payload["error_scope"], "workflow_version")

    def test_manual_review_routes_enqueue_list_submit_and_history(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                empty_resp = self.client.get("/governance/manual-reviews")
                self.assertEqual(empty_resp.status_code, 200)
                self.assertEqual(empty_resp.json()["items"], [])

                enqueue_resp = self.client.post(
                    "/governance/manual-reviews/enqueue",
                    json={
                        "items": [
                            {
                                "run_id": "run_1",
                                "review_key": "gate_a",
                                "workflow_id": "wf_finance",
                                "node_id": "n7",
                                "created_at": "2026-03-21T00:00:00Z",
                            }
                        ]
                    },
                )
                self.assertEqual(enqueue_resp.status_code, 200)
                self.assertEqual(len(enqueue_resp.json()["items"]), 1)

                queue_resp = self.client.get("/governance/manual-reviews")
                self.assertEqual(queue_resp.status_code, 200)
                self.assertEqual(queue_resp.json()["items"][0]["review_key"], "gate_a")

                submit_resp = self.client.post(
                    "/governance/manual-reviews/submit",
                    json={
                        "run_id": "run_1",
                        "review_key": "gate_a",
                        "approved": True,
                        "reviewer": "alice",
                        "comment": "ok",
                    },
                )
                self.assertEqual(submit_resp.status_code, 200)
                self.assertEqual(submit_resp.json()["item"]["status"], "approved")
                self.assertEqual(submit_resp.json()["remaining"], 0)

                history_resp = self.client.get("/governance/manual-reviews/history?run_id=run_1&reviewer=ali")
                self.assertEqual(history_resp.status_code, 200)
                self.assertEqual(len(history_resp.json()["items"]), 1)
                self.assertEqual(history_resp.json()["items"][0]["reviewer"], "alice")

    def test_manual_review_submit_rejects_missing_task(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                resp = self.client.post(
                    "/governance/manual-reviews/submit",
                    json={
                        "run_id": "run_missing",
                        "review_key": "gate_x",
                        "approved": False,
                    },
                )
                self.assertEqual(resp.status_code, 400)
                self.assertFalse(resp.json()["ok"])

    def test_run_baseline_routes_store_backend_owned_baselines(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                empty = self.client.get("/governance/run-baselines")
                self.assertEqual(empty.status_code, 200)
                self.assertEqual(empty.json()["items"], [])

                put = self.client.put(
                    "/governance/run-baselines/base_1",
                    json={
                        "baseline": {
                            "name": "Base One",
                            "run_id": "run_1",
                            "workflow_id": "wf_finance",
                            "notes": "seed",
                        }
                    },
                )
                self.assertEqual(put.status_code, 200)
                self.assertEqual(put.json()["item"]["owner"], "glue-python")

                get_resp = self.client.get("/governance/run-baselines/base_1")
                self.assertEqual(get_resp.status_code, 200)
                self.assertEqual(get_resp.json()["item"]["run_id"], "run_1")

                list_resp = self.client.get("/governance/run-baselines")
                self.assertEqual(list_resp.status_code, 200)
                self.assertEqual(len(list_resp.json()["items"]), 1)

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

    def test_run_reference_success_response_shape(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                workflow_definition = {
                    "workflow_id": "cleaning",
                    "version": "workflow.v1",
                    "nodes": [],
                    "edges": [],
                }
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    return_value={"ok": True, "normalized_workflow_definition": workflow_definition},
                ), patch.object(glue_app, "_run_workflow_definition_reference", return_value={"ok": True, "custom": "value"}) as run_ref:
                    save_resp = self.client.put(
                        "/governance/workflow-versions/ver_cleaning_compat_001",
                        json={"version": {"workflow_name": "Cleaning Compat", "workflow_definition": workflow_definition}},
                    )
                    self.assertEqual(save_resp.status_code, 200)

                    resp = self.client.post(
                        "/jobs/job123/run-reference",
                        json={"actor": "local", "ruleset_version": "v1", "version_id": "ver_cleaning_compat_001", "params": {"x": 1}},
                    )

                self.assertEqual(resp.status_code, 200)
                payload = resp.json()
                self.assertTrue(payload["ok"])
                self.assertEqual(payload["job_id"], "job123")
                self.assertEqual(payload["version_id"], "ver_cleaning_compat_001")
                self.assertEqual(payload["published_version_id"], "ver_cleaning_compat_001")
                self.assertIn("seconds", payload)
                self.assertEqual(payload["custom"], "value")
                self.assertEqual(run_ref.call_count, 1)

    def test_legacy_run_flow_route_rejects_workflow_reference_bridge(self):
        resp = self.client.post(
            "/jobs/job123/run/workflow_reference",
            json={"actor": "local", "ruleset_version": "v1", "params": {"x": 1}},
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertEqual(payload["flow"], "workflow_reference")
        self.assertIn("retired", payload["error"])

    def test_run_reference_rejects_mismatched_published_version_id(self):
        resp = self.client.post(
            "/jobs/job123/run-reference",
            json={"actor": "local", "ruleset_version": "v1", "version_id": "ver_cleaning_compat_001", "published_version_id": "other"},
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertIn("must match version_id", payload["error"])

    def test_run_reference_rejects_unknown_version_reference(self):
        resp = self.client.post(
            "/jobs/job123/run-reference",
            json={"actor": "local", "ruleset_version": "v1", "version_id": "missing_ref"},
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertIn("unknown workflow version reference", payload["error"])

    def test_run_reference_rejects_legacy_payload_fields(self):
        resp = self.client.post(
            "/jobs/job123/run-reference",
            json={
                "actor": "local",
                "ruleset_version": "v1",
                "version_id": "ver_cleaning_compat_001",
                "workflow_definition": {"workflow_id": "wf_bad"},
            },
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertIn("must not include", payload["error"])

    def test_run_workflow_reference_looks_up_governance_version_store(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                workflow_definition = {
                    "workflow_id": "cleaning",
                    "version": "workflow.v1",
                    "nodes": [],
                    "edges": [],
                }
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    return_value={"ok": True, "normalized_workflow_definition": workflow_definition},
                ), patch.object(glue_app, "_run_workflow_definition_reference", return_value={"ok": True, "adapter": "cleaning"}) as adapter:
                    save_resp = self.client.put(
                        "/governance/workflow-versions/ver_cleaning_compat_001",
                        json={"version": {"workflow_name": "Cleaning Compat", "workflow_definition": workflow_definition}},
                    )
                    self.assertEqual(save_resp.status_code, 200)

                    out = glue_app._run_workflow_reference(
                        "job123",
                        glue_app.RunReferenceReq(
                            actor="local",
                            ruleset_version="v1",
                            version_id="ver_cleaning_compat_001",
                            params={"x": 1},
                        ),
                    )

                self.assertTrue(out["ok"])
                self.assertEqual(adapter.call_count, 1)
                version_item = adapter.call_args.args[2]
                self.assertEqual(version_item["version_id"], "ver_cleaning_compat_001")
                self.assertEqual(version_item["workflow_definition"]["workflow_id"], "cleaning")

    def test_run_workflow_definition_reference_calls_rust_execution_surface(self):
        version_item = {
            "version_id": "ver_cleaning_compat_001",
            "workflow_definition": {
                "workflow_id": "cleaning",
                "version": "workflow.v1",
                "nodes": [],
                "edges": [],
            },
        }
        with patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False), patch.object(
            glue_app,
            "workflow_reference_run_v1",
            return_value={
                "ok": True,
                "operator": "workflow_reference_run_v1",
                "execution": {
                    "operator": "workflow_run",
                    "status": "done",
                },
                "final_output": {
                    "operator": "cleaning",
                    "outputs": {
                        "cleaned_csv": {"path": "D:/tmp/job/stage/cleaned.csv", "sha256": "csv"},
                        "cleaned_parquet": {"path": "D:/tmp/job/stage/cleaned.parquet", "sha256": "parquet"},
                        "profile_json": {"path": "D:/tmp/job/evidence/profile.json", "sha256": "profile"},
                        "xlsx_fin": {"path": "D:/tmp/job/artifacts/fin.xlsx", "sha256": "xlsx"},
                        "audit_docx": {"path": "D:/tmp/job/artifacts/audit.docx", "sha256": "docx"},
                        "deck_pptx": {"path": "D:/tmp/job/artifacts/deck.pptx", "sha256": "pptx"},
                    },
                    "profile": {"rows": 1, "cols": 2},
                    "office_generation_mode": "rust",
                    "office_generation_warning": None,
                },
            },
        ) as rust_exec, patch.object(glue_app, "sha256_file", side_effect=lambda _path: "sha"), patch.object(
            glue_app, "base_step_start_impl", return_value=None
        ), patch.object(
            glue_app, "base_step_done_impl", return_value=None
        ), patch.object(
            glue_app, "base_step_fail_impl", return_value=None
        ), patch.object(
            glue_app, "base_artifact_upsert_impl", return_value=None
        ):
            out = glue_app._run_workflow_definition_reference(
                "job123",
                glue_app.RunReferenceReq(
                    actor="local",
                    ruleset_version="v1",
                    version_id="ver_cleaning_compat_001",
                    params={"rows": [{"id": 1, "amount": 10.0}]},
                    job_context=make_job_context(r"D:\tmp\job"),
                ),
                version_item,
            )

        self.assertTrue(out["ok"])
        self.assertEqual(out["workflow_definition_source"], "version_reference")
        self.assertEqual(out["version_id"], "ver_cleaning_compat_001")
        self.assertEqual(out["execution"]["operator"], "workflow_run")
        self.assertEqual(out["final_output"]["operator"], "cleaning")
        self.assertTrue(str(out["final_output"]["outputs"]["cleaned_parquet"]["path"]).endswith("cleaned.parquet"))
        self.assertEqual(rust_exec.call_count, 1)

    def test_run_reference_rejects_invalid_stored_workflow_definition(self):
        with tempfile.TemporaryDirectory() as tmp:
            with patch.dict("os.environ", {"AIWF_GOVERNANCE_ROOT": tmp}, clear=False):
                workflow_definition = {
                    "workflow_id": "cleaning",
                    "version": "workflow.v1",
                    "nodes": [],
                    "edges": [],
                }
                with patch.object(
                    glue_app,
                    "validate_workflow_definition_authoritatively",
                    side_effect=[
                        {"ok": True, "normalized_workflow_definition": workflow_definition},
                        glue_app.WorkflowValidationFailure(
                            "workflow contains unregistered node types: unknown_future_node",
                            error_items=[{
                                "path": "workflow.nodes",
                                "code": "unknown_node_type",
                                "message": "workflow contains unregistered node types: unknown_future_node",
                            }],
                        ),
                    ],
                ):
                    save_resp = self.client.put(
                        "/governance/workflow-versions/ver_cleaning_compat_001",
                        json={"version": {"workflow_name": "Cleaning Compat", "workflow_definition": workflow_definition}},
                    )
                    self.assertEqual(save_resp.status_code, 200)

                    resp = self.client.post(
                        "/jobs/job123/run-reference",
                        json={"actor": "local", "ruleset_version": "v1", "version_id": "ver_cleaning_compat_001"},
                    )

                self.assertEqual(resp.status_code, 400)
                payload = resp.json()
                self.assertFalse(payload["ok"])
                self.assertEqual(payload["error_code"], "workflow_graph_invalid")
                self.assertEqual(payload["error_scope"], "workflow_reference_run")

    def test_run_reference_rejects_version_item_missing_workflow_definition(self):
        with patch.object(glue_app, "get_workflow_version", return_value={"version_id": "ver_missing_def"}):
            resp = self.client.post(
                "/jobs/job123/run-reference",
                json={"actor": "local", "ruleset_version": "v1", "version_id": "ver_missing_def"},
            )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job123")
        self.assertIn("workflow_definition missing", payload["error"])

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

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    @patch("aiwf.flows.cleaning.run_cleaning", return_value={"ok": True})
    def test_run_flow_route_rejects_invalid_job_context_path_as_bad_request(self, run_cleaning, _make_base_client):
        resp = self.client.post(
            "/jobs/job-bad-ctx/run/cleaning",
            json={
                "actor": "local",
                "ruleset_version": "v1",
                "job_context": {
                    "job_root": r"D:\ctx\job",
                    "stage_dir": r"..\escape",
                },
                "params": {"x": 1},
            },
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job-bad-ctx")
        self.assertEqual(payload["flow"], "cleaning")
        self.assertIn("error", payload)
        run_cleaning.assert_not_called()

    def test_custom_registered_flow_dispatches_without_editing_app(self):
        def run_custom_flow(**kwargs):
            params = kwargs.get("params") or {}
            context = params.get("job_context") if isinstance(params.get("job_context"), dict) else {}
            return {"ok": True, "custom": True, "job_root": context.get("job_root")}

        with glue_app.runtime_catalog.activate():
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
            try:
                with glue_app.runtime_catalog.activate():
                    unregister_flow("ext_flow")
                    with patch.dict("os.environ", {"AIWF_EXT_MODULES": module_name}, clear=False):
                        extensions.reset_extension_state_for_tests()
                        status = extensions.load_extension_modules(force=True)
                        caps = self.client.get("/capabilities").json()["capabilities"]
            finally:
                if tmp in sys.path:
                    sys.path.remove(tmp)

            self.assertIn(module_name, status["loaded"])
            self.assertIn("ext_flow", caps["flows"])
            ext_flow = next(item for item in caps["flow_details"] if item["name"] == "ext_flow")
            self.assertEqual(ext_flow["source_module"], module_name)
            with glue_app.runtime_catalog.activate():
                unregister_flow("ext_flow")

    def test_run_flow_auto_loads_configured_extension_module(self):
        with tempfile.TemporaryDirectory() as tmp:
            module_name = "aiwf_test_ext_autoload_module"
            module_path = os.path.join(tmp, f"{module_name}.py")
            with open(module_path, "w", encoding="utf-8") as f:
                f.write(
                    "from aiwf.flows.registry import register_flow\n"
                    "register_flow(\n"
                    "    'ext_auto_flow',\n"
                    "    runner=lambda **kwargs: {'ok': True, 'autoloaded': True},\n"
                    "    domain='extension-auto',\n"
                    "    domain_metadata={'label': 'Extension Auto', 'backend': 'extension', 'builtin': False},\n"
                    ")\n"
                )

            sys.path.insert(0, tmp)
            try:
                with glue_app.runtime_catalog.activate():
                    unregister_flow("ext_auto_flow")
                    with patch.dict("os.environ", {"AIWF_EXT_MODULES": module_name}, clear=False):
                        extensions.reset_extension_state_for_tests()
                        resp = self.client.post(
                            "/jobs/job-auto/run/ext_auto_flow",
                            json={"actor": "local", "ruleset_version": "v1", "params": {}},
                        )
            finally:
                if tmp in sys.path:
                    sys.path.remove(tmp)

            self.assertEqual(resp.status_code, 200)
            payload = resp.json()
            self.assertTrue(payload["autoloaded"])
            with glue_app.runtime_catalog.activate():
                unregister_flow("ext_auto_flow")

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
            try:
                with glue_app.runtime_catalog.activate():
                    unregister_flow("ext_reload_flow")
                    with patch.dict("os.environ", {"AIWF_EXT_MODULES": module_name}, clear=False):
                        extensions.reset_extension_state_for_tests()
                        extensions.load_extension_modules(force=True)
                        unregister_flow("ext_reload_flow")
                        status = extensions.load_extension_modules(force=True)
            finally:
                if tmp in sys.path:
                    sys.path.remove(tmp)

            self.assertIn(module_name, status["loaded"])
            with glue_app.runtime_catalog.activate():
                self.assertTrue(get_flow_runner("ext_reload_flow")()["reloaded"])
                unregister_flow("ext_reload_flow")

    def test_register_flow_keep_policy_preserves_existing_runner(self):
        def first_runner(**kwargs):
            return {"ok": True, "runner": "first"}

        def second_runner(**kwargs):
            return {"ok": True, "runner": "second"}

        with glue_app.runtime_catalog.activate():
            register_flow("keep_flow", runner=first_runner)
            try:
                kept = register_flow("keep_flow", runner=second_runner, on_conflict="keep")
                self.assertEqual(kept.source_module, __name__)
                runner = get_flow_runner("keep_flow")
                self.assertIs(runner, first_runner)
            finally:
                unregister_flow("keep_flow")

    def test_register_flow_error_policy_raises(self):
        with glue_app.runtime_catalog.activate():
            register_flow("error_flow", runner=lambda **kwargs: {"ok": True})
            try:
                with self.assertRaises(RuntimeError):
                    register_flow("error_flow", runner=lambda **kwargs: {"ok": True}, on_conflict="error")
            finally:
                unregister_flow("error_flow")

    def test_capabilities_reports_registry_conflict_events(self):
        with glue_app.runtime_catalog.activate():
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
        with glue_app.runtime_catalog.activate():
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

        with glue_app.runtime_catalog.activate():
            register_flow("flow-a", runner=flow_a, aliases=("shared-alias",))
            try:
                register_flow("flow-b", runner=flow_b, aliases=("shared-alias",), on_conflict="warn")
                self.assertEqual(get_flow_runner("shared-alias")()["runner"], "b")
                self.assertEqual(get_flow_runner("flow-a")()["runner"], "a")
                self.assertEqual(get_flow_registration("flow-a").aliases, ())
            finally:
                unregister_flow("flow-a")
                unregister_flow("flow-b")

    def test_register_flow_domain_metadata_is_visible_on_aliases_and_capabilities(self):
        with glue_app.runtime_catalog.activate():
            register_flow(
                "domain-flow",
                runner=lambda **kwargs: {"ok": True},
                aliases=("domain-flow-alias",),
                domain="custom-domain",
                domain_metadata={
                    "label": "Custom Domain",
                    "backend": "extension",
                    "builtin": False,
                },
            )
            try:
                registration = get_flow_registration("domain-flow-alias")
                self.assertEqual(registration.domain, "custom-domain")
                self.assertEqual(dict(registration.domain_metadata)["label"], "Custom Domain")

                caps = self.client.get("/capabilities").json()["capabilities"]
                flow_detail = next(item for item in caps["flow_details"] if item["name"] == "domain-flow")
                self.assertEqual(flow_detail["domain"], "custom-domain")
                self.assertEqual(flow_detail["domain_metadata"]["backend"], "extension")
                flow_domain = next(item for item in caps["flow_domains"] if item["name"] == "custom-domain")
                self.assertEqual(flow_domain["flow_names"], ["domain-flow"])
                self.assertFalse(flow_domain["builtin"])
            finally:
                unregister_flow("domain-flow")

    @patch.object(glue_app, "make_base_client", return_value=None)
    @patch("aiwf.flows.cleaning.run_cleaning", return_value={"ok": True})
    def test_run_cleaning_flow_injects_default_job_root(self, run_cleaning, _make_base_client):
        req = glue_app.RunReq(actor="local", ruleset_version="v1", params={"x": 1})

        glue_app.run_cleaning_flow("job-root", req)

        kwargs = run_cleaning.call_args.kwargs
        self.assertEqual(kwargs["params"]["x"], 1)
        self.assertEqual(
            kwargs["params"]["job_context"]["job_root"],
            os.path.join(glue_app.settings.jobs_root, "job-root"),
        )
        self.assertNotIn("job_root", kwargs["params"])

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
            params={"x": 1},
        )

        glue_app.run_cleaning_flow("job-root", req)

        kwargs = run_cleaning.call_args.kwargs
        self.assertEqual(kwargs["params"]["job_context"]["job_root"], os.path.normpath(r"D:\ctx\job"))
        self.assertEqual(kwargs["params"]["job_context"]["stage_dir"], os.path.normpath(r"D:\ctx\job\stage"))
        self.assertEqual(kwargs["params"]["job_context"]["artifacts_dir"], os.path.normpath(r"D:\ctx\job\artifacts"))
        self.assertEqual(kwargs["params"]["job_context"]["evidence_dir"], os.path.normpath(r"D:\ctx\job\evidence"))
        self.assertEqual(kwargs["params"]["trace_id"], "trace-123")
        self.assertNotIn("job_root", kwargs["params"])
        self.assertNotIn("stage_dir", kwargs["params"])
        self.assertNotIn("artifacts_dir", kwargs["params"])
        self.assertNotIn("evidence_dir", kwargs["params"])

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
                        "job_context": make_job_context(local_job_root),
                        "params": {
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
    def test_run_flow_route_uses_explicit_job_context(self, _make_base_client):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "ctx-job")

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

    @patch.dict("os.environ", {"AIWF_ALLOW_EXTERNAL_JOB_ROOT": "true"}, clear=False)
    @patch.object(glue_app, "make_base_client", return_value=None)
    def test_run_flow_route_rejects_legacy_path_params(self, _make_base_client):
        resp = self.client.post(
            "/jobs/job-strict/run/cleaning",
            json={
                "actor": "local",
                "ruleset_version": "v1",
                "params": {
                    "job_root": r"D:\legacy\job",
                    "rows": [{"id": 1, "amount": 10.0}],
                },
            },
        )

        self.assertEqual(resp.status_code, 400)
        payload = resp.json()
        self.assertFalse(payload["ok"])
        self.assertEqual(payload["job_id"], "job-strict")
        self.assertEqual(payload["flow"], "cleaning")
        self.assertIn("legacy flow path params are no longer supported", payload["error"])

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
                        "job_context": make_job_context(local_job_root),
                        "params": {
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
