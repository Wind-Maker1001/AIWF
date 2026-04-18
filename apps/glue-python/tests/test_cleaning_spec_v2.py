import unittest

from aiwf.cleaning_spec_v2 import (
    CLEANING_SPEC_V2_VERSION,
    build_header_mapping,
    candidate_profiles_from_headers,
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
                "header_mapping_mode": "auto",
                "header_map": {"Speaker": "speaker"},
                "field_transforms": [
                    {"field": "speaker", "op": "trim"},
                    {"field": "biz_date", "op": "parse_date"},
                ],
                "row_filters": [{"field": "speaker", "op": "exists"}],
            }
        )
        self.assertEqual(spec["schema"]["header_normalizer"], "preprocess")
        self.assertEqual(spec["ingest"]["header_mapping_mode"], "auto")
        self.assertEqual(spec["transform"]["rename_map"]["Speaker"], "speaker")
        self.assertTrue(any(item["op"] == "trim" for item in spec["transform"]["field_ops"]))
        self.assertEqual(spec["transform"]["filters"][0]["op"], "exists")

    def test_compile_cleaning_spec_preserves_survivorship_and_advanced_rules(self):
        spec = compile_cleaning_params_to_spec(
            {
                "canonical_profile": "customer_ledger",
                "rules": {
                    "platform_mode": "generic",
                    "deduplicate_by": ["phone", "biz_date", "amount"],
                    "survivorship": {
                        "keys": ["phone", "biz_date", "amount"],
                        "prefer_non_null_fields": ["customer_name"],
                        "prefer_latest_fields": ["biz_date"],
                        "tie_breaker": "last",
                    },
                },
                "quality_rules": {
                    "advanced_rules": {
                        "outlier_zscore": [{"field": "amount", "threshold": 3.0}],
                        "anomaly_iqr": [{"field": "amount", "multiplier": 1.5}],
                        "bank_statement_semantics": {
                            "signed_amount_conflict_tolerance": 0.01,
                            "balance_continuity_tolerance": 0.05,
                            "block_on_semantic_conflicts": True,
                        },
                        "block_on_advanced_rules": True,
                    }
                },
            }
        )
        rules, _, _ = cleaning_spec_to_transform_components(spec)
        self.assertEqual(rules["survivorship"]["keys"], ["phone", "biz_date", "amount"])
        self.assertEqual(rules["survivorship"]["prefer_latest_fields"], ["biz_date"])
        self.assertEqual(spec["quality"]["advanced_rules"]["outlier_zscore"][0]["field"], "amount")
        self.assertEqual(
            spec["quality"]["advanced_rules"]["bank_statement_semantics"]["signed_amount_conflict_tolerance"],
            0.01,
        )
        self.assertTrue(
            spec["quality"]["advanced_rules"]["bank_statement_semantics"]["block_on_semantic_conflicts"]
        )
        self.assertTrue(spec["quality"]["advanced_rules"]["block_on_advanced_rules"])

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
        self.assertEqual(registry["customer_ledger"]["unique_keys"], ["phone", "biz_date", "amount"])
        self.assertEqual(registry["bank_statement"]["unique_keys"], ["account_no", "txn_date", "ref_no", "amount"])
        self.assertIn("header_aliases", registry["debate_evidence"])

    def test_compile_cleaning_params_to_spec_for_bank_statement_profile(self):
        spec = compile_cleaning_params_to_spec(
            {
                "canonical_profile": "bank_statement",
                "header_mapping_mode": "strict",
                "rules": {
                    "platform_mode": "generic",
                    "rename_map": {"\u8d26\u53f7": "account_no", "\u4ea4\u6613\u65e5\u671f": "txn_date"},
                    "casts": {"debit_amount": "float", "credit_amount": "float"},
                    "computed_fields": {"amount": "sub($credit_amount,$debit_amount)"},
                },
            }
        )
        self.assertEqual(spec["schema"]["canonical_profile"], "bank_statement")
        self.assertEqual(spec["ingest"]["header_mapping_mode"], "strict")
        self.assertEqual(spec["schema"]["unique_keys"], ["account_no", "txn_date", "ref_no", "amount"])
        self.assertEqual(spec["transform"]["default_values"]["currency"], "CNY")
        self.assertEqual(spec["transform"]["computed_fields"]["amount"], "sub($credit_amount,$debit_amount)")

    def test_compile_cleaning_params_to_spec_for_customer_ledger_profile(self):
        spec = compile_cleaning_params_to_spec(
            {
                "canonical_profile": "customer_ledger",
                "rules": {
                    "platform_mode": "generic",
                    "rename_map": {"Cust Name": "customer_name", "Amt": "amount", "Biz Dt": "biz_date"},
                    "casts": {"amount": "float", "biz_date": "string"},
                },
            }
        )
        self.assertEqual(spec["schema"]["canonical_profile"], "customer_ledger")
        self.assertEqual(spec["schema"]["unique_keys"], ["phone", "biz_date", "amount"])
        self.assertEqual(spec["transform"]["casts"]["amount"], "float")
        self.assertEqual(spec["transform"]["casts"]["biz_date"], "string")

    def test_build_header_mapping_in_auto_mode_reports_match_details(self):
        mapping = build_header_mapping(
            ["Acct No", "Posting Dt", "DR", "CR"],
            canonical_profile="bank_statement",
            header_mapping_mode="auto",
            sample_values_by_header={
                "Posting Dt": ["2026/03/01", "2026-03-02"],
                "DR": ["120.5", "0"],
                "CR": ["0", "300"],
            },
        )
        acct = next(item for item in mapping if item["raw_header"] == "Acct No")
        posting = next(item for item in mapping if item["raw_header"] == "Posting Dt")
        self.assertEqual(acct["canonical_field"], "account_no")
        self.assertTrue(acct["alternatives"])
        self.assertEqual(posting["canonical_field"], "txn_date")
        self.assertIn(posting["match_strategy"], {"exact", "fuzzy+value_affinity"})

    def test_candidate_profiles_from_headers_recommends_bank_template_in_auto_mode(self):
        candidates = candidate_profiles_from_headers(
            ["Acct No", "Posting Dt", "DR", "CR", "Bal", "Memo", "Ref No"],
            header_mapping_mode="auto",
            sample_values_by_header={
                "Posting Dt": ["2026/03/01", "2026-03-02"],
                "DR": ["120.5", "0"],
                "CR": ["0", "300"],
                "Bal": ["12500", "12800"],
            },
        )
        self.assertEqual(candidates[0]["profile"], "bank_statement")
        self.assertTrue(candidates[0]["recommended"])
        self.assertEqual(candidates[0]["recommended_template_id"], "bank_statement_v1")

    def test_candidate_profiles_distinguish_customer_contact_and_customer_ledger(self):
        contact_candidates = candidate_profiles_from_headers(
            ["Cust Name", "Mobile No", "City"],
            header_mapping_mode="auto",
            sample_values_by_header={
                "Cust Name": ["Alice", "Bob"],
                "Mobile No": ["13800138000", "13800138001"],
                "City": ["Shanghai", "Hangzhou"],
            },
        )
        self.assertEqual(contact_candidates[0]["profile"], "customer_contact")
        self.assertTrue(contact_candidates[0]["recommended"])
        self.assertEqual(contact_candidates[0]["recommended_template_id"], "customer_contact_v1")
        ledger_in_contact = next(item for item in contact_candidates if item["profile"] == "customer_ledger")
        self.assertFalse(ledger_in_contact["recommended"])

        ledger_candidates = candidate_profiles_from_headers(
            ["Cust Name", "Mobile No", "City", "Amt", "Biz Dt"],
            header_mapping_mode="auto",
            sample_values_by_header={
                "Cust Name": ["Alice", "Bob"],
                "Mobile No": ["13800138000", "13800138001"],
                "City": ["Shanghai", "Hangzhou"],
                "Amt": ["120", "300"],
                "Biz Dt": ["2026-03-01", "2026-03-02"],
            },
        )
        self.assertEqual(ledger_candidates[0]["profile"], "customer_ledger")
        self.assertTrue(ledger_candidates[0]["recommended"])
        self.assertEqual(ledger_candidates[0]["recommended_template_id"], "customer_ledger_v1")
        contact_in_ledger = next(item for item in ledger_candidates if item["profile"] == "customer_contact")
        self.assertFalse(contact_in_ledger["recommended"])


if __name__ == "__main__":
    unittest.main()
