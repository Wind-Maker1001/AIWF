import json
import os
import tempfile
import unittest
from unittest.mock import Mock, patch

from aiwf import preprocess


class PreprocessTests(unittest.TestCase):
    def test_validate_preprocess_spec(self):
        ok = preprocess.validate_preprocess_spec(
            {
                "header_map": {"Amt": "amount"},
                "amount_fields": ["amount"],
                "date_fields": ["biz_date"],
                "amount_round_digits": 2,
            }
        )
        self.assertTrue(ok["ok"])

        bad = preprocess.validate_preprocess_spec({"amount_round_digits": 100, "amount_fields": "x"})
        self.assertFalse(bad["ok"])
        warn = preprocess.validate_preprocess_spec({"unknown_key": 1})
        self.assertTrue(warn["ok"])
        self.assertTrue(any("unknown preprocess keys" in w for w in warn["warnings"]))
        bad2 = preprocess.validate_preprocess_spec({"on_file_error": "x"})
        self.assertFalse(bad2["ok"])
        bad3 = preprocess.validate_preprocess_spec({"generate_quality_report": "yes"})
        self.assertFalse(bad3["ok"])
        bad4 = preprocess.validate_preprocess_spec({"chunk_mode": "abc"})
        self.assertFalse(bad4["ok"])
        bad5 = preprocess.validate_preprocess_spec({"ocr_lang": 1})
        self.assertFalse(bad5["ok"])
        bad6 = preprocess.validate_preprocess_spec({"ocr_config": 1})
        self.assertFalse(bad6["ok"])
        bad7 = preprocess.validate_preprocess_spec({"ocr_preprocess": 1})
        self.assertFalse(bad7["ok"])
        bad8 = preprocess.validate_preprocess_spec({"ocr_preprocess": "bad"})
        self.assertFalse(bad8["ok"])
        bad9 = preprocess.validate_preprocess_spec({"pipeline": {"stages": []}})
        self.assertFalse(bad9["ok"])
        bad10 = preprocess.validate_preprocess_spec({"export_canonical_bundle": "yes"})
        self.assertFalse(bad10["ok"])
        bad11 = preprocess.validate_preprocess_spec({"field_transforms": [{"field": "text", "op": "missing_op"}]})
        self.assertFalse(bad11["ok"])
        bad12 = preprocess.validate_preprocess_spec({"row_filters": [{"field": "text", "op": "missing_filter"}]})
        self.assertFalse(bad12["ok"])

    def test_preprocess_passes_ocr_options_to_ingest(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "a.txt")
            out = os.path.join(tmp, "out.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write("placeholder")

            with patch("aiwf.preprocess.ingest.load_rows_from_files") as load_rows:
                load_rows.return_value = ([{"text": "x"}], {"input_format": "txt", "skipped_files": [], "failed_files": []})
                preprocess.preprocess_file(
                    src,
                    out,
                    {
                        "input_files": [src],
                        "output_format": "jsonl",
                        "ocr_enabled": True,
                        "ocr_lang": "chi_sim+eng",
                        "ocr_config": "--oem 1 --psm 6",
                        "ocr_preprocess": "adaptive",
                    },
                )

                _, kwargs = load_rows.call_args
                self.assertEqual(kwargs["ocr_lang"], "chi_sim+eng")
                self.assertEqual(kwargs["ocr_config"], "--oem 1 --psm 6")
                self.assertEqual(kwargs["ocr_preprocess"], "adaptive")

    def test_preprocess_csv_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.csv")
            dst = os.path.join(tmp, "cooked.csv")
            with open(src, "w", encoding="utf-8", newline="\n") as f:
                f.write("ID,Amt,Date\n")
                f.write("1,$1,200.56,2026/02/16\n")
                f.write("2,  99.1 ,2026-02-17\n")

            # fix comma issue in amount field by quoting in source
            with open(src, "w", encoding="utf-8", newline="\n") as f:
                f.write('ID,Amt,Date\n')
                f.write('1,"$1,200.56",2026/02/16\n')
                f.write('2,"  99.1 ",2026-02-17\n')

            res = preprocess.preprocess_csv_file(
                src,
                dst,
                {
                    "header_map": {"ID": "id", "Amt": "amount", "Date": "biz_date"},
                    "amount_fields": ["amount"],
                    "date_fields": ["biz_date"],
                    "date_input_formats": ["%Y/%m/%d", "%Y-%m-%d"],
                },
            )
            self.assertTrue(os.path.isfile(dst))
            self.assertEqual(res["summary"]["input_rows"], 2)
            self.assertEqual(res["summary"]["output_rows"], 2)

            rows, _ = preprocess._read_csv(dst)
            self.assertEqual(rows[0]["id"], "1")
            self.assertEqual(rows[0]["amount"], "1200.56")
            self.assertEqual(rows[0]["biz_date"], "2026-02-16")

    def test_preprocess_jsonl_with_transforms_and_filters(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"speaker": " Alice ", "text": "Visit https://x.com NOW", "score": "9.6"}) + "\n")
                f.write(json.dumps({"speaker": "bob", "text": "contact me a@b.com", "score": "3.1"}) + "\n")

            res = preprocess.preprocess_file(
                src,
                dst,
                {
                    "input_format": "jsonl",
                    "output_format": "jsonl",
                    "field_transforms": [
                        {"field": "speaker", "op": "trim"},
                        {"field": "speaker", "op": "lower"},
                        {"field": "text", "op": "remove_urls"},
                        {"field": "text", "op": "remove_emails"},
                        {"field": "text", "op": "collapse_whitespace"},
                        {"field": "score", "op": "parse_number"},
                        {"field": "score", "op": "round_number", "digits": 0},
                    ],
                    "row_filters": [{"field": "score", "op": "gte", "value": 5}],
                },
            )
            self.assertTrue(os.path.isfile(dst))
            self.assertEqual(res["output_format"], "jsonl")
            self.assertEqual(res["summary"]["output_rows"], 1)
            rows = preprocess._read_jsonl(dst)
            self.assertEqual(rows[0]["speaker"], "alice")
            self.assertNotIn("https://", rows[0]["text"])
            self.assertEqual(rows[0]["score"], 10.0)

    def test_preprocess_can_use_rust_v2_for_supported_transforms(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"Speaker": " Alice ", "biz_date": "2026/03/01"}) + "\n")

            fake_resp = Mock()
            fake_resp.status_code = 200
            fake_resp.json.return_value = {
                "ok": True,
                "rows": [{"speaker": "alice", "biz_date": "2026-03-01"}],
                "quality": {
                    "input_rows": 1,
                    "output_rows": 1,
                    "invalid_rows": 0,
                    "filtered_rows": 0,
                    "duplicate_rows_removed": 0,
                    "numeric_cells_total": 0,
                    "numeric_cells_parsed": 0,
                    "date_cells_total": 1,
                    "date_cells_parsed": 1,
                },
                "trace_id": "tp1",
                "audit": {"schema": "transform_rows_v2.audit.v1"},
            }
            with patch("requests.post", return_value=fake_resp):
                res = preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "use_rust_v2": True,
                        "field_transforms": [
                            {"field": "speaker", "op": "trim"},
                            {"field": "speaker", "op": "lower"},
                            {"field": "biz_date", "op": "parse_date"},
                        ],
                    },
                )
            rows = preprocess._read_jsonl(dst)
            self.assertEqual(rows[0]["speaker"], "alice")
            self.assertEqual(rows[0]["biz_date"], "2026-03-01")
            self.assertEqual(res["summary"]["cleaning_spec_version"], "cleaning_spec.v2")
            self.assertTrue(res["summary"]["rust_v2_used"])
            self.assertEqual(res["execution_mode"], "rust_v2")
            self.assertEqual(res["eligibility_reason"], "eligible")
            self.assertEqual(res["execution_audit"]["schema"], "transform_rows_v2.audit.v1")
            self.assertEqual(res["summary"]["execution_mode"], "rust_v2")

    def test_preprocess_rust_v2_blocked_by_chunk_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "a. b."}) + "\n")

            with patch("requests.post") as post:
                res = preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "use_rust_v2": True,
                        "chunk_mode": "sentence",
                    },
                )
            post.assert_not_called()
            self.assertEqual(res["execution_mode"], "python_legacy")
            self.assertEqual(res["eligibility_reason"], "chunk_mode_enabled")
            self.assertEqual(res["summary"]["execution_audit"]["schema"], "python_preprocess.audit.v1")

    def test_preprocess_rust_v2_blocked_by_conflict_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"claim_text": "support tax"}) + "\n")

            with patch("requests.post") as post:
                res = preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "use_rust_v2": True,
                        "detect_conflicts": True,
                    },
                )
            post.assert_not_called()
            self.assertEqual(res["execution_mode"], "python_legacy")
            self.assertEqual(res["eligibility_reason"], "detect_conflicts_enabled")

    def test_preprocess_rust_v2_blocked_by_standardize_evidence(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "claim"}) + "\n")

            with patch("requests.post") as post:
                res = preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "use_rust_v2": True,
                        "standardize_evidence": True,
                    },
                )
            post.assert_not_called()
            self.assertEqual(res["execution_mode"], "python_legacy")
            self.assertEqual(res["eligibility_reason"], "standardize_evidence_enabled")

    def test_preprocess_rust_v2_blocked_by_custom_transform(self):
        def prefix_transform(value, cfg):
            if value is None:
                return value, False
            return f"{cfg.get('prefix', '')}{value}", True

        preprocess.register_field_transform(
            "prefix_blocker",
            prefix_transform,
            domain="custom-preprocess",
            domain_metadata={"label": "Custom Preprocess", "backend": "extension", "builtin": False},
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                src = os.path.join(tmp, "raw.jsonl")
                dst = os.path.join(tmp, "cooked.jsonl")
                with open(src, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"speaker": "alice"}) + "\n")

                with patch("requests.post") as post:
                    res = preprocess.preprocess_file(
                        src,
                        dst,
                        {
                            "input_format": "jsonl",
                            "output_format": "jsonl",
                            "use_rust_v2": True,
                            "field_transforms": [{"field": "speaker", "op": "prefix_blocker", "prefix": "team-"}],
                        },
                    )
                post.assert_not_called()
                self.assertEqual(res["execution_mode"], "python_legacy")
                self.assertEqual(res["eligibility_reason"], "unsupported_field_transform")
        finally:
            preprocess.unregister_field_transform("prefix_blocker")

    def test_preprocess_rust_v2_blocked_by_custom_filter(self):
        def starts_with_filter(row, cfg):
            return str(row.get(str(cfg.get("field") or "")) or "").startswith(str(cfg.get("value") or ""))

        preprocess.register_row_filter(
            "starts_with_blocker",
            starts_with_filter,
            domain="custom-preprocess",
            domain_metadata={"label": "Custom Preprocess", "backend": "extension", "builtin": False},
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                src = os.path.join(tmp, "raw.jsonl")
                dst = os.path.join(tmp, "cooked.jsonl")
                with open(src, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"speaker": "alice"}) + "\n")

                with patch("requests.post") as post:
                    res = preprocess.preprocess_file(
                        src,
                        dst,
                        {
                            "input_format": "jsonl",
                            "output_format": "jsonl",
                            "use_rust_v2": True,
                            "row_filters": [{"field": "speaker", "op": "starts_with_blocker", "value": "a"}],
                        },
                    )
                post.assert_not_called()
                self.assertEqual(res["execution_mode"], "python_legacy")
                self.assertEqual(res["eligibility_reason"], "unsupported_row_filter")
        finally:
            preprocess.unregister_row_filter("starts_with_blocker")

    def test_preprocess_supports_registered_custom_transform_and_filter(self):
        def prefix_transform(value, cfg):
            if value is None:
                return value, False
            return f"{cfg.get('prefix', '')}{value}", True

        def starts_with_filter(row, cfg):
            field = str(cfg.get("field") or "").strip()
            if not field:
                return True
            return str(row.get(field) or "").startswith(str(cfg.get("value") or ""))

        preprocess.register_field_transform(
            "prefix",
            prefix_transform,
            domain="custom-preprocess",
            domain_metadata={"label": "Custom Preprocess", "backend": "extension", "builtin": False},
        )
        preprocess.register_row_filter(
            "starts_with",
            starts_with_filter,
            domain="custom-preprocess",
            domain_metadata={"label": "Custom Preprocess", "backend": "extension", "builtin": False},
        )
        try:
            with tempfile.TemporaryDirectory() as tmp:
                src = os.path.join(tmp, "raw.jsonl")
                dst = os.path.join(tmp, "cooked.jsonl")
                with open(src, "w", encoding="utf-8") as f:
                    f.write(json.dumps({"speaker": "alice"}) + "\n")
                    f.write(json.dumps({"speaker": "bob"}) + "\n")

                valid = preprocess.validate_preprocess_spec(
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "field_transforms": [{"field": "speaker", "op": "prefix", "prefix": "team-"}],
                        "row_filters": [{"field": "speaker", "op": "starts_with", "value": "team-a"}],
                    }
                )
                self.assertTrue(valid["ok"])

                res = preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "field_transforms": [{"field": "speaker", "op": "prefix", "prefix": "team-"}],
                        "row_filters": [{"field": "speaker", "op": "starts_with", "value": "team-a"}],
                    },
                )
                rows = preprocess._read_jsonl(dst)
                field_details = {item["op"]: item for item in preprocess.list_field_transform_details()}
                row_domains = preprocess.list_row_filter_domains()
        finally:
            preprocess.unregister_field_transform("prefix")
            preprocess.unregister_row_filter("starts_with")

        self.assertEqual(res["summary"]["output_rows"], 1)
        self.assertEqual(rows[0]["speaker"], "team-alice")
        self.assertEqual(field_details["prefix"]["domain"], "custom-preprocess")
        self.assertTrue(any(item["name"] == "custom-preprocess" for item in row_domains))

    def test_preprocess_with_input_files_txt_and_docx(self):
        with tempfile.TemporaryDirectory() as tmp:
            txt = os.path.join(tmp, "a.txt")
            docx = os.path.join(tmp, "b.docx")
            out = os.path.join(tmp, "out.jsonl")

            with open(txt, "w", encoding="utf-8") as f:
                f.write("claim one")
            from docx import Document  # type: ignore

            d = Document()
            d.add_paragraph("claim two")
            d.save(docx)

            res = preprocess.preprocess_file(
                txt,
                out,
                {
                    "input_files": [txt, docx],
                    "output_format": "jsonl",
                    "field_transforms": [{"field": "text", "op": "lower"}],
                },
            )
            self.assertEqual(res["summary"]["output_rows"], 2)
            rows = preprocess._read_jsonl(out)
            self.assertEqual(rows[0]["text"], "claim one")

    def test_preprocess_with_input_files_skip_image(self):
        with tempfile.TemporaryDirectory() as tmp:
            txt = os.path.join(tmp, "a.txt")
            img = os.path.join(tmp, "b.png")
            out = os.path.join(tmp, "out.jsonl")
            with open(txt, "w", encoding="utf-8") as f:
                f.write("evidence")
            with open(img, "wb") as f:
                f.write(b"fake")

            res = preprocess.preprocess_file(
                txt,
                out,
                {
                    "input_files": [txt, img],
                    "output_format": "jsonl",
                    "ocr_enabled": False,
                    "on_file_error": "skip",
                },
            )
            self.assertEqual(res["summary"]["output_rows"], 1)
            self.assertTrue(isinstance(res.get("skipped_files"), list))

    def test_preprocess_deduplicate_by(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"speaker": "alice", "claim_text": "A"}) + "\n")
                f.write(json.dumps({"speaker": "alice", "claim_text": "A"}) + "\n")
                f.write(json.dumps({"speaker": "bob", "claim_text": "B"}) + "\n")

            res = preprocess.preprocess_file(
                src,
                dst,
                {
                    "input_format": "jsonl",
                    "output_format": "jsonl",
                    "deduplicate_by": ["speaker", "claim_text"],
                    "deduplicate_keep": "first",
                },
            )
            self.assertEqual(res["summary"]["input_rows"], 3)
            self.assertEqual(res["summary"]["output_rows"], 2)
            self.assertEqual(res["summary"]["duplicate_rows_removed"], 1)

    def test_preprocess_with_input_files_xlsx_all_sheets(self):
        with tempfile.TemporaryDirectory() as tmp:
            xlsx = os.path.join(tmp, "a.xlsx")
            out = os.path.join(tmp, "out.jsonl")
            from openpyxl import Workbook  # type: ignore

            wb = Workbook()
            ws = wb.active
            ws.title = "S1"
            ws.append(["ClaimText"])
            ws.append(["claim one"])
            ws2 = wb.create_sheet("S2")
            ws2.append(["ClaimText"])
            ws2.append(["claim two"])
            wb.save(xlsx)

            res = preprocess.preprocess_file(
                xlsx,
                out,
                {
                    "input_files": [xlsx],
                    "output_format": "jsonl",
                    "xlsx_all_sheets": True,
                    "header_map": {"ClaimText": "claim_text"},
                },
            )
            self.assertEqual(res["summary"]["output_rows"], 2)
            rows = preprocess._read_jsonl(out)
            self.assertEqual({r["sheet_name"] for r in rows}, {"S1", "S2"})

    def test_preprocess_blocks_xlsx_input_when_quality_rules_fail(self):
        with tempfile.TemporaryDirectory() as tmp:
            xlsx = os.path.join(tmp, "a.xlsx")
            out = os.path.join(tmp, "out.jsonl")
            from openpyxl import Workbook  # type: ignore

            wb = Workbook()
            ws = wb.active
            ws.title = "S1"
            ws.append(["id", "note"])
            ws.append([1, "hello"])
            wb.save(xlsx)

            with self.assertRaisesRegex(RuntimeError, "required_columns"):
                preprocess.preprocess_file(
                    xlsx,
                    out,
                    {
                        "input_files": [xlsx],
                        "output_format": "jsonl",
                        "xlsx_rules": {"required_columns": ["id", "amount"]},
                    },
                )

    def test_preprocess_standardize_evidence_and_quality_report(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "Claim A", "author": "Alice", "source_url": "https://a"}) + "\n")
                f.write(json.dumps({"text": "Claim B", "author": "Bob", "source_url": "https://b"}) + "\n")

            res = preprocess.preprocess_file(
                src,
                dst,
                {
                    "input_format": "jsonl",
                    "output_format": "jsonl",
                    "standardize_evidence": True,
                    "generate_quality_report": True,
                    "quality_required_fields": ["claim_text", "source_url"],
                },
            )
            self.assertTrue(res["quality_report_path"])
            self.assertTrue(os.path.isfile(res["quality_report_path"]))
            rows = preprocess._read_jsonl(dst)
            self.assertIn("evidence_id", rows[0])
            self.assertIn("claim_text", rows[0])
            with open(res["quality_report_path"], "r", encoding="utf-8") as f:
                report = json.load(f)
            self.assertEqual(report["rows"], 2)
            self.assertEqual(report["required_field_missing"]["claim_text"], 0)

    def test_preprocess_export_canonical_bundle(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "论文主体第一段", "source_file": "a.pdf"}) + "\n")
                f.write(json.dumps({"text": "论文主体第二段", "source_file": "a.pdf"}) + "\n")

            res = preprocess.preprocess_file(
                src,
                dst,
                {
                    "input_format": "jsonl",
                    "output_format": "jsonl",
                    "export_canonical_bundle": True,
                    "canonical_title": "论文熟肉包",
                },
            )
            bundle = res.get("canonical_bundle") or {}
            self.assertTrue(bundle.get("bundle_dir"))
            self.assertTrue(os.path.isfile(bundle.get("markdown_path")))
            self.assertTrue(os.path.isfile(bundle.get("metadata_path")))
            self.assertTrue(os.path.isfile(bundle.get("lineage_path")))
            with open(bundle.get("metadata_path"), "r", encoding="utf-8") as f:
                meta = json.load(f)
            self.assertEqual(meta.get("row_count"), 2)

    def test_preprocess_rejects_relative_canonical_bundle_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "hello", "source_file": "a.txt"}) + "\n")

            with self.assertRaises(ValueError):
                preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "export_canonical_bundle": True,
                        "canonical_bundle_dir": r"..\bundle_outside",
                    },
                )

    def test_preprocess_rejects_absolute_quality_report_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            outside = os.path.join(tempfile.gettempdir(), "aiwf_quality_escape.json")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "hello", "source_file": "a.txt"}) + "\n")

            with self.assertRaises(ValueError):
                preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "generate_quality_report": True,
                        "quality_report_path": outside,
                    },
                )

    def test_preprocess_rejects_absolute_canonical_bundle_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            outside = os.path.join(tempfile.gettempdir(), "aiwf_bundle_escape")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "hello", "source_file": "a.txt"}) + "\n")

            with self.assertRaises(ValueError):
                preprocess.preprocess_file(
                    src,
                    dst,
                    {
                        "input_format": "jsonl",
                        "output_format": "jsonl",
                        "export_canonical_bundle": True,
                        "canonical_bundle_dir": outside,
                    },
                )

    def test_preprocess_chunk_and_conflict_detection(self):
        with tempfile.TemporaryDirectory() as tmp:
            src = os.path.join(tmp, "raw.jsonl")
            dst = os.path.join(tmp, "cooked.jsonl")
            with open(src, "w", encoding="utf-8") as f:
                f.write(json.dumps({"text": "Tax policy is good. We should support it."}) + "\n")
                f.write(json.dumps({"text": "Tax policy is bad. We should oppose it."}) + "\n")

            res = preprocess.preprocess_file(
                src,
                dst,
                {
                    "input_format": "jsonl",
                    "output_format": "jsonl",
                    "header_map": {"text": "claim_text"},
                    "chunk_mode": "sentence",
                    "chunk_field": "claim_text",
                    "detect_conflicts": True,
                    "conflict_text_field": "claim_text",
                    "conflict_positive_words": ["support", "good"],
                    "conflict_negative_words": ["oppose", "bad"],
                },
            )
            self.assertGreaterEqual(res["summary"]["chunked_rows_created"], 2)
            self.assertGreaterEqual(res["summary"]["conflict_rows_marked"], 2)
            rows = preprocess._read_jsonl(dst)
            self.assertTrue(any(r.get("conflict_flag") for r in rows))

    def test_preprocess_pipeline_extract_clean_structure_audit(self):
        with tempfile.TemporaryDirectory() as tmp:
            txt = os.path.join(tmp, "raw.txt")
            with open(txt, "w", encoding="utf-8") as f:
                f.write("Tax policy should be supported.\nTax policy should be opposed.")

            out_csv = os.path.join(tmp, "final.csv")
            res = preprocess.run_preprocess_pipeline(
                pipeline={
                    "stages": [
                        {"name": "extract", "config": {"input_files": [txt]}},
                        {"name": "clean", "config": {"field_transforms": [{"field": "text", "op": "trim"}]}},
                        {
                            "name": "structure",
                            "config": {
                                "header_map": {"text": "claim_text"},
                                "chunk_mode": "sentence",
                                "chunk_field": "claim_text",
                            },
                        },
                        {
                            "name": "audit",
                            "config": {
                                "detect_conflicts": True,
                                "conflict_text_field": "claim_text",
                                "conflict_positive_words": ["support"],
                                "conflict_negative_words": ["oppose"],
                            },
                        },
                    ]
                },
                job_root=tmp,
                stage_dir=os.path.join(tmp, "stage"),
                input_path=txt,
                final_output_path=out_csv,
            )
            self.assertEqual(res["mode"], "pipeline")
            self.assertEqual(len(res["stages"]), 4)
            self.assertTrue(os.path.isfile(out_csv))
            rows, _ = preprocess._read_csv(out_csv)
            self.assertGreaterEqual(len(rows), 2)

    def test_validate_preprocess_pipeline_rejects_unknown_stage(self):
        vr = preprocess.validate_preprocess_pipeline({"stages": [{"name": "missing_stage", "config": {}}]})
        self.assertFalse(vr["ok"])

    def test_write_rows_supports_bare_filename_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                out_format = preprocess._write_rows("rows.jsonl", [{"text": "hello"}], {})
                rows = preprocess._read_jsonl("rows.jsonl")
            finally:
                os.chdir(cwd)

        self.assertEqual(out_format, "jsonl")
        self.assertEqual(rows[0]["text"], "hello")

    def test_preprocess_pipeline_supports_registered_custom_stage(self):
        def prepare_uppercase_stage(context):
            cfg = dict(context.config)
            cfg.setdefault("output_format", "jsonl")
            transforms = list(cfg.get("field_transforms") or [])
            transforms.append({"field": "text", "op": "upper"})
            cfg["field_transforms"] = transforms
            return cfg

        preprocess.register_pipeline_stage("uppercase_stage", prepare_config=prepare_uppercase_stage)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                txt = os.path.join(tmp, "raw.txt")
                with open(txt, "w", encoding="utf-8") as f:
                    f.write("hello world")

                pipeline = {
                    "stages": [
                        {"name": "extract", "config": {"input_files": [txt]}},
                        {"name": "uppercase_stage", "config": {}},
                    ]
                }
                vr = preprocess.validate_preprocess_pipeline(pipeline)
                self.assertTrue(vr["ok"])

                out_csv = os.path.join(tmp, "final.csv")
                res = preprocess.run_preprocess_pipeline(
                    pipeline=pipeline,
                    job_root=tmp,
                    stage_dir=os.path.join(tmp, "stage"),
                    input_path=txt,
                    final_output_path=out_csv,
                )
                rows, _ = preprocess._read_csv(out_csv)
        finally:
            preprocess.unregister_pipeline_stage("uppercase_stage")

        self.assertEqual(res["stages"][1]["stage"], "uppercase_stage")
        self.assertEqual(rows[0]["text"], "HELLO WORLD")

    def test_preprocess_pipeline_final_output_respects_requested_format(self):
        with tempfile.TemporaryDirectory() as tmp:
            txt = os.path.join(tmp, "raw.txt")
            with open(txt, "w", encoding="utf-8") as f:
                f.write("hello world")

            out_jsonl = os.path.join(tmp, "final.jsonl")
            res = preprocess.run_preprocess_pipeline(
                pipeline={"stages": [{"name": "extract", "config": {"input_files": [txt]}}]},
                job_root=tmp,
                stage_dir=os.path.join(tmp, "stage"),
                input_path=txt,
                final_output_path=out_jsonl,
            )
            rows = preprocess._read_jsonl(out_jsonl)

        self.assertEqual(res["output_path"], out_jsonl)
        self.assertEqual(rows[0]["text"], "hello world")

    def test_preprocess_pipeline_rejects_final_output_path_escape(self):
        with tempfile.TemporaryDirectory() as tmp:
            txt = os.path.join(tmp, "raw.txt")
            with open(txt, "w", encoding="utf-8") as f:
                f.write("hello world")

            with self.assertRaises(ValueError):
                preprocess.run_preprocess_pipeline(
                    pipeline={"stages": [{"name": "extract", "config": {"input_files": [txt]}}]},
                    job_root=tmp,
                    stage_dir=os.path.join(tmp, "stage"),
                    input_path=txt,
                    final_output_path=r"..\outside.csv",
                )


if __name__ == "__main__":
    unittest.main()
