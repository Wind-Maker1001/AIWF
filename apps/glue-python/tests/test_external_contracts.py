import json
from pathlib import Path
import unittest


ROOT = Path(__file__).resolve().parents[3]


class ExternalContractsTests(unittest.TestCase):
    def test_openapi_describes_transform_rows_v2_contracts(self):
        text = (ROOT / "contracts" / "rust" / "openapi.v2.yaml").read_text(encoding="utf-8")
        self.assertIn("TransformRowsRuleSet:", text)
        self.assertIn("TransformRowsQualityGates:", text)
        self.assertIn("TransformRowsSchemaHint:", text)
        self.assertIn("TransformRowsAudit:", text)
        self.assertIn("field_ops:", text)

    def test_operator_manifest_contains_transform_rows_contract_metadata(self):
        payload = json.loads((ROOT / "contracts" / "rust" / "operators_manifest.v1.json").read_text(encoding="utf-8"))
        transform = next(item for item in payload["operators"] if item["operator"] == "transform_rows_v2")
        contracts = transform.get("contracts") or {}
        self.assertEqual(contracts.get("request_schema"), "#/components/schemas/TransformRowsReq")
        self.assertEqual(contracts.get("response_schema"), "#/components/schemas/TransformRowsResp")
        self.assertEqual(contracts.get("rules_schema"), "#/components/schemas/TransformRowsRuleSet")
        self.assertEqual(contracts.get("schema_hint_schema"), "#/components/schemas/TransformRowsSchemaHint")
        self.assertEqual(contracts.get("audit_schema"), "#/components/schemas/TransformRowsAudit")


if __name__ == "__main__":
    unittest.main()
