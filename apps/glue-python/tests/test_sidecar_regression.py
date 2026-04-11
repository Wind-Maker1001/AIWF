import unittest

from aiwf.sidecar_regression import (
    _expected_reason_counts,
    _derive_default_rust_rules,
    compare_expected_quality,
    compare_expected_rows,
    compute_common_python_metrics,
    evaluate_consistency_report,
    validate_ingest_extract_contract,
)


class SidecarRegressionTests(unittest.TestCase):
    def test_validate_ingest_extract_contract_requires_stable_fields(self):
        errors = validate_ingest_extract_contract({"ok": True, "rows": []})
        self.assertTrue(any("file_results" in item for item in errors))
        self.assertTrue(any("engine_trace" in item for item in errors))
        self.assertTrue(any("header_mapping" in item for item in errors))

    def test_compare_expected_quality_checks_thresholds_and_blocked(self):
        payload = {
            "quality_blocked": False,
            "engine_trace": [{"engine": "calamine+openpyxl", "ok": True}],
            "table_cells": [{"id": 1}],
            "sheet_frames": [{"id": 1}, {"id": 2}],
            "file_results": [
                {
                    "quality_metrics": {
                        "header_confidence": 0.95,
                        "required_missing_ratio": 0.0,
                    }
                }
            ],
        }
        errors = compare_expected_quality(
            payload,
            {
                "quality_blocked": False,
                "header_confidence_min": 0.9,
                "required_missing_ratio_max": 0.0,
                "table_cells_min": 1,
                "sheet_frames_min": 2,
            },
            {"engine_ok_any_of": ["calamine+openpyxl", "docling"]},
        )
        self.assertEqual(errors, [])

    def test_compare_expected_quality_can_require_engine_path(self):
        payload = {
            "quality_blocked": False,
            "engine_trace": [{"engine": "tesseract", "ok": True}],
            "file_results": [{"quality_metrics": {}}],
        }
        errors = compare_expected_quality(
            payload,
            {"quality_blocked": False},
            {"engine_ok_any_of": ["paddleocr", "docling"]},
        )
        self.assertTrue(any("engine_ok_any_of" in item for item in errors))

    def test_compare_expected_quality_can_require_recommended_profile_and_template(self):
        payload = {
            "quality_blocked": False,
            "detected_structure": "tabular",
            "engine_trace": [{"engine": "docling", "ok": True}],
            "header_mapping": [
                {"raw_header": "Acct No", "canonical_field": "account_no"},
                {"raw_header": "Posting Dt", "canonical_field": "txn_date"},
            ],
            "candidate_profiles": [
                {
                    "profile": "bank_statement",
                    "recommended": True,
                    "recommended_template_id": "bank_statement_v1",
                    "signal_source": "table_cells",
                }
            ],
            "file_results": [{"quality_metrics": {"header_confidence": 0.91}}],
        }
        errors = compare_expected_quality(
            payload,
            {"quality_blocked": False, "header_confidence_min": 0.9},
            {
                "expected_detected_structure": "tabular",
                "expected_recommended_profile": "bank_statement",
                "expected_recommended_template_id": "bank_statement_v1",
                "expected_signal_source": "table_cells",
                "expected_header_mapping_fields": ["account_no", "txn_date"],
            },
        )
        self.assertEqual(errors, [])

    def test_compare_expected_rows_can_skip_ocr_row_compare(self):
        errors = compare_expected_rows(
            actual_rows=[{"text": "OCR line one"}],
            expected_rows=[],
            scenario={"skip_row_compare": True},
        )
        self.assertEqual(errors, [])

    def test_compare_expected_quality_can_reject_forbidden_recommended_profiles(self):
        payload = {
            "quality_blocked": False,
            "detected_structure": "text",
            "engine_trace": [{"engine": "tesseract", "ok": True}],
            "candidate_profiles": [
                {"profile": "debate_evidence", "recommended": True, "recommended_template_id": "debate_evidence_v1"},
                {"profile": "bank_statement", "recommended": True, "recommended_template_id": "bank_statement_v1"},
            ],
            "file_results": [{"quality_metrics": {"ocr_confidence_avg": 0.8}}],
        }
        errors = compare_expected_quality(
            payload,
            {"quality_blocked": False, "ocr_confidence_avg_min": 0.55},
            {
                "expected_detected_structure": "text",
                "expected_recommended_profile": "debate_evidence",
                "expected_recommended_template_id": "debate_evidence_v1",
                "forbidden_recommended_profiles": ["finance_statement", "bank_statement", "customer_contact"],
            },
        )
        self.assertTrue(any("forbidden recommended profiles" in item for item in errors))

    def test_compare_expected_quality_can_require_customer_templates(self):
        payload = {
            "quality_blocked": False,
            "detected_structure": "tabular",
            "engine_trace": [{"engine": "docling", "ok": True}],
            "header_mapping": [
                {"raw_header": "Cust Name", "canonical_field": "customer_name"},
                {"raw_header": "Mobile No", "canonical_field": "phone"},
                {"raw_header": "City", "canonical_field": "city"},
                {"raw_header": "Amt", "canonical_field": "amount"},
                {"raw_header": "Biz Dt", "canonical_field": "biz_date"},
            ],
            "candidate_profiles": [
                {
                    "profile": "customer_ledger",
                    "recommended": True,
                    "recommended_template_id": "customer_ledger_v1",
                    "signal_source": "table_cells",
                }
            ],
            "file_results": [{"quality_metrics": {"header_confidence": 0.91}}],
        }
        errors = compare_expected_quality(
            payload,
            {"quality_blocked": False, "header_confidence_min": 0.9},
            {
                "expected_detected_structure": "tabular",
                "expected_recommended_profile": "customer_ledger",
                "expected_recommended_template_id": "customer_ledger_v1",
                "expected_signal_source": "table_cells",
                "expected_header_mapping_fields": ["customer_name", "phone", "city", "amount", "biz_date"],
            },
        )
        self.assertEqual(errors, [])

    def test_compare_expected_rows_uses_subset_fields(self):
        actual = [
            {"customer_name": "Alice", "phone": "+8613800138000", "sheet_name": "S1", "ignored": 1},
            {"customer_name": "Bob", "phone": "+8613800138001", "sheet_name": "S2", "ignored": 2},
        ]
        expected = [
            {"customer_name": "Bob", "phone": "+8613800138001", "sheet_name": "S2"},
            {"customer_name": "Alice", "phone": "+8613800138000", "sheet_name": "S1"},
        ]
        errors = compare_expected_rows(actual, expected, {"expected_row_fields": ["customer_name", "phone", "sheet_name"]})
        self.assertEqual(errors, [])

    def test_compute_common_python_metrics_for_finance_profile(self):
        metrics = compute_common_python_metrics(
            [
                {"id": 1, "amount": 100000.0, "biz_date": "2026-03-01"},
                {"id": 2, "amount": 250000000.0, "biz_date": "2026-03-02"},
            ],
            {
                "canonical_profile": "finance_statement",
                "quality_rules": {
                    "required_fields": ["id", "amount"],
                    "unique_keys": ["id"],
                },
            },
        )
        self.assertEqual(metrics["required_missing_ratio"], 0.0)
        self.assertEqual(metrics["numeric_parse_rate"], 1.0)
        self.assertEqual(metrics["date_parse_rate"], 1.0)
        self.assertEqual(metrics["duplicate_key_ratio"], 0.0)

    def test_compute_common_python_metrics_for_bank_statement_profile(self):
        metrics = compute_common_python_metrics(
            [
                {"account_no": "6222", "txn_date": "2026-03-01", "ref_no": "A1", "amount": "100.00", "balance": "1000"},
                {"account_no": "6222", "txn_date": "2026-03-01", "ref_no": "A1", "amount": "100.00", "balance": "900"},
                {"account_no": "6222", "txn_date": "2026-03-02", "ref_no": "A2", "amount": "-50.00", "balance": "850"},
            ],
            {
                "canonical_profile": "bank_statement",
                "quality_rules": {
                    "required_fields": ["account_no", "txn_date"],
                },
            },
        )
        self.assertEqual(metrics["required_missing_ratio"], 0.0)
        self.assertEqual(metrics["numeric_parse_rate"], 1.0)
        self.assertEqual(metrics["date_parse_rate"], 1.0)
        self.assertEqual(metrics["duplicate_key_ratio"], 0.333333)

    def test_derive_default_rust_rules_for_bank_statement(self):
        rules = _derive_default_rust_rules(
            {
                "canonical_profile": "bank_statement",
                "quality_rules": {
                    "required_fields": ["account_no", "txn_date"],
                    "unique_keys": ["account_no", "txn_date", "ref_no", "amount"],
                },
            }
        )
        self.assertEqual(rules["required_fields"], ["account_no", "txn_date"])
        self.assertEqual(rules["deduplicate_by"], ["account_no", "txn_date", "ref_no", "amount"])
        self.assertEqual(rules["deduplicate_keep"], "last")
        self.assertEqual(rules["casts"]["debit_amount"], "float")
        self.assertEqual(rules["casts"]["credit_amount"], "float")
        self.assertEqual(rules["casts"]["amount"], "float")
        self.assertEqual(rules["casts"]["balance"], "float")
        self.assertEqual(rules["date_ops"], [{"field": "txn_date", "op": "parse_ymd", "as": "txn_date"}])

    def test_evaluate_consistency_report_fails_on_skipped_when_required(self):
        out = evaluate_consistency_report(
            [
                {"id": "case_ok", "status": "passed"},
                {"id": "case_skip", "status": "skipped"},
            ],
            require_accel=True,
        )
        self.assertFalse(out["ok"])
        self.assertEqual(out["failed"], [])
        self.assertEqual(out["skipped"], ["case_skip"])

    def test_expected_reason_counts_tracks_transform_rejections(self):
        counts = _expected_reason_counts(
            [
                {"id": "1", "amount": "bad"},
                {"id": "2", "amount": "10"},
                {"id": "2", "amount": "10"},
            ],
            {
                "casts": {"id": "int", "amount": "float"},
                "required_fields": ["id", "amount"],
                "deduplicate_by": ["id"],
                "deduplicate_keep": "last",
            },
        )
        self.assertEqual(counts["cast_failed"], 1)
        self.assertEqual(counts["duplicate_removed"], 1)


if __name__ == "__main__":
    unittest.main()
