import unittest

from aiwf.cleaning_spec_v2 import (
    CLEANING_SPEC_V2_VERSION,
    cleaning_spec_to_transform_components,
    compile_cleaning_params_to_spec,
    compile_preprocess_spec_to_spec,
    get_canonical_profile_registry,
    reason_codes_from_quality_errors,
)


class CleaningSpecV2Tests(unittest.TestCase):
    def test_compile_cleaning_params_to_spec_for_simple_rules(self):
        spec = compile_cleaning_params_to_spec(
            {
                "amount_round_digits": 2,
                "drop_negative_amount": True,
                "max_invalid_rows": 0,
            }
        )
        self.assertEqual(spec["schema_version"], CLEANING_SPEC_V2_VERSION)
        self.assertEqual(spec["transform"]["casts"]["id"], "int")
        self.assertEqual(spec["transform"]["casts"]["amount"], "float")
        self.assertTrue(any(item["op"] == "gte" for item in spec["transform"]["filters"]))
        self.assertEqual(spec["quality"]["gates"]["max_invalid_rows"], 0)

    def test_compile_preprocess_spec_to_spec_preserves_transform_ops(self):
        spec = compile_preprocess_spec_to_spec(
            {
                "header_map": {"Speaker": "speaker"},
                "field_transforms": [
                    {"field": "speaker", "op": "trim"},
                    {"field": "biz_date", "op": "parse_date"},
                ],
                "row_filters": [{"field": "speaker", "op": "exists"}],
            }
        )
        self.assertEqual(spec["schema"]["header_normalizer"], "preprocess")
        self.assertEqual(spec["transform"]["rename_map"]["Speaker"], "speaker")
        self.assertTrue(any(item["op"] == "trim" for item in spec["transform"]["field_ops"]))
        self.assertEqual(spec["transform"]["filters"][0]["op"], "exists")

    def test_transform_components_expand_auto_header_mapping(self):
        spec = compile_preprocess_spec_to_spec({})
        rules, quality_gates, schema_hint = cleaning_spec_to_transform_components(
            spec,
            input_rows=[{"Speaker Name": "Alice"}],
        )
        self.assertEqual(rules["rename_map"]["Speaker Name"], "speaker_name")
        self.assertIn("required_fields", quality_gates)
        self.assertEqual(schema_hint["schema_version"], CLEANING_SPEC_V2_VERSION)

    def test_reason_codes_from_quality_errors(self):
        codes = reason_codes_from_quality_errors(
            [
                "header_confidence=0.2 below header_confidence_min=0.8",
                "ocr_confidence_avg=0.1 below threshold",
                "required_columns missing: amount",
            ]
        )
        self.assertIn("header_low_confidence", codes)
        self.assertIn("ocr_low_confidence", codes)
        self.assertIn("required_fields_missing", codes)

    def test_cleaning_spec_uses_shared_profile_registry(self):
        registry = get_canonical_profile_registry()
        self.assertEqual(registry["customer_contact"]["unique_keys"], ["phone"])
        self.assertIn("header_aliases", registry["debate_evidence"])


if __name__ == "__main__":
    unittest.main()
