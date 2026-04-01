import unittest

from aiwf.sidecar_regression import (
    _expected_reason_counts,
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
