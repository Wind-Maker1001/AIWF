import json
import os
import tempfile
import unittest
from unittest.mock import patch

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


if __name__ == "__main__":
    unittest.main()
