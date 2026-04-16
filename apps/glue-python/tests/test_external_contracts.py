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

    def test_openapi_describes_postprocess_rows_and_quality_check_v2_metrics(self):
        text = (ROOT / "contracts" / "rust" / "openapi.v2.yaml").read_text(encoding="utf-8")
        self.assertIn("/operators/transform_rows_v3:", text)
        self.assertIn("/operators/postprocess_rows_v1:", text)
        self.assertIn("PostprocessRowsV1Req:", text)
        self.assertIn("/operators/quality_check_v2:", text)
        self.assertIn("QualityCheckV2Req:", text)
        self.assertIn("metrics:", text)

    def test_operator_manifest_marks_postprocess_rows_palette_hidden(self):
        payload = json.loads((ROOT / "contracts" / "rust" / "operators_manifest.v1.json").read_text(encoding="utf-8"))
        postprocess = next(item for item in payload["operators"] if item["operator"] == "postprocess_rows_v1")
        self.assertTrue(postprocess["published"])
        self.assertTrue(postprocess["workflow_exposable"])
        self.assertTrue(postprocess["desktop_exposable"])
        self.assertTrue(postprocess["palette_hidden"])

    def test_handwritten_rust_client_covers_current_published_transform_surface(self):
        text = (ROOT / "apps" / "glue-python" / "aiwf" / "rust_client.py").read_text(encoding="utf-8")
        self.assertIn("def transform_rows_v3(", text)
        self.assertIn('"/operators/transform_rows_v3"', text)
        self.assertIn("def postprocess_rows_v1(", text)
        self.assertIn('"/operators/postprocess_rows_v1"', text)
        self.assertIn("def quality_check_v2(", text)
        self.assertIn('"/operators/quality_check_v2"', text)

    def test_ingest_extract_contract_describes_auto_header_mapping_surface(self):
        payload = json.loads((ROOT / "contracts" / "glue" / "ingest_extract.schema.json").read_text(encoding="utf-8"))
        request_properties = payload["properties"]["request"]["properties"]
        self.assertEqual(request_properties["header_mapping_mode"]["enum"], ["strict", "auto"])
        self.assertIn("bank_statement", request_properties["canonical_profile"]["enum"])
        self.assertIn("customer_ledger", request_properties["canonical_profile"]["enum"])
        response_properties = payload["properties"]["response"]["properties"]
        header_mapping_item = response_properties["header_mapping"]["items"]["properties"]
        self.assertEqual(
            header_mapping_item["match_strategy"]["enum"],
            ["exact", "substring", "fuzzy", "fuzzy+value_affinity", "unresolved"],
        )
        self.assertIn("alternatives", header_mapping_item)
        candidate_item = response_properties["candidate_profiles"]["items"]["properties"]
        self.assertIn("avg_confidence", candidate_item)
        self.assertIn("required_coverage", candidate_item)
        self.assertIn("recommended", candidate_item)
        self.assertIn("recommended_template_id", candidate_item)
        self.assertEqual(candidate_item["signal_source"]["enum"], ["headers", "table_cells", "content"])
        self.assertEqual(response_properties["detected_structure"]["enum"], ["tabular", "text", "mixed", "unknown"])

    def test_cleaning_precheck_contract_describes_authoritative_precheck_surface(self):
        payload = json.loads((ROOT / "contracts" / "glue" / "cleaning_precheck.schema.json").read_text(encoding="utf-8"))
        request_properties = payload["properties"]["request"]["properties"]
        self.assertIn("cleaning_template", request_properties)
        self.assertEqual(request_properties["header_mapping_mode"]["enum"], ["strict", "auto"])
        self.assertEqual(request_properties["profile_mismatch_action"]["enum"], ["", "warn", "block"])
        response_properties = payload["properties"]["response"]["properties"]
        self.assertEqual(response_properties["precheck_action"]["enum"], ["allow", "warn", "block"])
        self.assertIn("recommended_template_id", response_properties)
        self.assertIn("predicted_zero_output_unexpected", response_properties)
        self.assertIn("blocking_reason_codes", response_properties)
        self.assertIn("issue_summary", response_properties)
        self.assertIn("suggested_repairs", response_properties)
        self.assertIn("header_ambiguities", response_properties)
        self.assertIn("duplicate_key_risk", response_properties)
        self.assertIn("review_required", response_properties)
        self.assertIn("review_items", response_properties)
        self.assertIn("contract", response_properties)


if __name__ == "__main__":
    unittest.main()
