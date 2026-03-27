import json
from pathlib import Path
import unittest

from aiwf.node_config_contract_runtime import (
    build_validation_error_items,
    validate_workflow_graph_node_configs,
)


PROJECT_ROOT = Path(__file__).resolve().parents[3]
FIXTURE_PATH = PROJECT_ROOT / "contracts" / "desktop" / "node_config_contract_fixtures.v1.json"


def load_fixture_cases():
    payload = json.loads(FIXTURE_PATH.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "node_config_contract_fixtures.v1"
    cases = []
    for entry in payload.get("nodes", []):
        node_type = str(entry.get("type") or "").strip()
        for item in entry.get("valid_cases", []):
            cases.append(
                {
                    "id": str(item.get("id") or "").strip(),
                    "node_type": node_type,
                    "expected_ok": True,
                    "config": item.get("config") or {},
                    "expected_error_contains": [],
                }
            )
        for item in entry.get("invalid_cases", []):
            cases.append(
                {
                    "id": str(item.get("id") or "").strip(),
                    "node_type": node_type,
                    "expected_ok": False,
                    "config": item.get("config") or {},
                    "expected_error_contains": list(item.get("expected_error_contains") or []),
                    "expected_error_items": list(item.get("expected_error_items") or []),
                }
            )
    return payload, cases


class NodeConfigContractParityTests(unittest.TestCase):
    def test_python_runtime_matches_shared_node_config_fixtures(self):
        payload, cases = load_fixture_cases()
        self.assertEqual(payload["authority"], "contracts/desktop/node_config_contracts.v1.json")
        self.assertEqual(len(payload["required_node_types"]), 30)
        self.assertEqual(len(cases), 60)

        for case in cases:
            errors = validate_workflow_graph_node_configs(
                {
                    "workflow_id": "wf_parity_fixture",
                    "version": "workflow.v1",
                    "nodes": [{"id": "n1", "type": case["node_type"], "config": case["config"]}],
                    "edges": [],
                },
                label_prefix="workflow",
            )
            actual_ok = len(errors) == 0
            self.assertEqual(actual_ok, case["expected_ok"], case["id"])
            joined = "\n".join(errors)
            for expected in case["expected_error_contains"]:
                self.assertIn(expected, joined, case["id"])
            error_items = build_validation_error_items(errors)
            for expected_item in case.get("expected_error_items", []):
                self.assertTrue(
                    any(
                        item.get("path") == expected_item.get("path")
                        and item.get("code") == expected_item.get("code")
                        for item in error_items
                    ),
                    case["id"],
                )


if __name__ == "__main__":
    unittest.main()
