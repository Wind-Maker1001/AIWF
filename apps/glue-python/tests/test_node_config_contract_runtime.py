from pathlib import Path
import unittest

from aiwf.node_config_contract_runtime import (
    build_node_config_contract_runtime_summary,
    load_registered_workflow_node_types,
    resolve_node_config_contract_path,
    resolve_rust_operator_manifest_path,
)


class NodeConfigContractRuntimeTests(unittest.TestCase):
    def test_registered_workflow_node_types_come_from_authority_sources(self):
        registered = load_registered_workflow_node_types()

        self.assertIn("ai_refine", registered)
        self.assertIn("transform_rows_v3", registered)
        self.assertIn("compute_rust", registered)
        self.assertIn("md_output", registered)

    def test_runtime_summary_reports_canonical_sources_not_desktop_generated_artifacts(self):
        summary = build_node_config_contract_runtime_summary()

        self.assertEqual(
            summary["registered_workflow_node_type_authorities"],
            [
                "contracts/desktop/node_config_contracts.v1.json",
                "contracts/rust/operators_manifest.v1.json",
            ],
        )
        self.assertEqual(summary["runtime_only_node_types"], ["compute_rust", "md_output"])
        self.assertNotIn("workflow_node_catalog_contract.js", summary["rust_operator_manifest_path"])
        self.assertNotIn(".generated.js", summary["rust_operator_manifest_path"])
        self.assertEqual(Path(summary["contract_path"]), resolve_node_config_contract_path())
        self.assertEqual(Path(summary["rust_operator_manifest_path"]), resolve_rust_operator_manifest_path())


if __name__ == "__main__":
    unittest.main()
