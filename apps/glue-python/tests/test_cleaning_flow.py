import os
import tempfile
import unittest
from unittest.mock import patch, Mock

os.environ.setdefault("NUMEXPR_MAX_THREADS", "8")

from aiwf.flows import cleaning


class CleaningFlowTests(unittest.TestCase):
    def test_office_theme_settings(self):
        t = cleaning._office_theme_settings({"office_theme": "debate"})
        self.assertEqual(t["name"], "debate")
        self.assertTrue("report_title" in t)
        t_assignment = cleaning._office_theme_settings({"office_theme": "assignment"})
        self.assertEqual(t_assignment["name"], "assignment")
        self.assertTrue("report_title" in t_assignment)
        t_en = cleaning._office_theme_settings({"office_theme": "debate", "office_lang": "en"})
        self.assertIn("Debate", t_en["report_title"])
        self.assertEqual(cleaning._office_quality_mode({}), "high")
        self.assertEqual(cleaning._office_quality_mode({"office_quality_mode": "standard"}), "standard")

    def test_office_writers_produce_rich_outputs(self):
        with tempfile.TemporaryDirectory() as tmp:
            xlsx_path = os.path.join(tmp, "fin.xlsx")
            docx_path = os.path.join(tmp, "audit.docx")
            pptx_path = os.path.join(tmp, "deck.pptx")
            img_path = os.path.join(tmp, "summary_visual.png")
            rows = [{"id": 1, "amount": 10.2, "name": "alice"}, {"id": 2, "amount": 20.3, "name": "bob"}]
            profile = cleaning._build_profile(
                rows,
                {"input_rows": 2, "output_rows": 2, "invalid_rows": 0, "filtered_rows": 0, "duplicate_rows_removed": 0},
                "unit.test",
            )
            cleaning._write_profile_illustration_png(img_path, profile)
            cleaning._write_fin_xlsx(xlsx_path, rows, img_path)
            cleaning._write_audit_docx(docx_path, "job-x", profile, img_path)
            cleaning._write_deck_pptx(pptx_path, "job-x", profile, img_path)

            from openpyxl import load_workbook  # type: ignore
            from docx import Document  # type: ignore

            wb = load_workbook(xlsx_path, read_only=True, data_only=True)
            self.assertEqual(wb.sheetnames, ["detail", "summary"])
            wb.close()
            doc = Document(docx_path)
            self.assertTrue("报告" in doc.paragraphs[0].text or "Report" in doc.paragraphs[0].text)
            self.assertTrue(os.path.getsize(pptx_path) > 0)

            from pptx import Presentation  # type: ignore

            prs = Presentation(pptx_path)
            self.assertGreaterEqual(len(prs.slides), 5)
            sw = prs.slide_width
            sh = prs.slide_height
            for s in prs.slides:
                for shape in s.shapes:
                    self.assertLessEqual(shape.left + shape.width, sw)
                    self.assertLessEqual(shape.top + shape.height, sh)

    def test_office_writers_support_english(self):
        with tempfile.TemporaryDirectory() as tmp:
            docx_path = os.path.join(tmp, "audit_en.docx")
            rows = [{"id": 1, "amount": 10.2}]
            profile = cleaning._build_profile(
                rows,
                {"input_rows": 1, "output_rows": 1, "invalid_rows": 0, "filtered_rows": 0, "duplicate_rows_removed": 0},
                "unit.test",
            )
            cleaning._write_audit_docx(docx_path, "job-en", profile, params={"office_lang": "en", "office_theme": "academic"})
            from docx import Document  # type: ignore

            doc = Document(docx_path)
            self.assertEqual(doc.paragraphs[0].text, "Academic Data Processing Report")

    def test_validate_cleaning_rules_ok(self):
        res = cleaning.validate_cleaning_rules(
            {
                "rules": {
                    "platform_mode": "generic",
                    "casts": {"amount": "float"},
                    "filters": [{"field": "amount", "op": "gte", "value": 0}],
                    "max_invalid_ratio": 0.2,
                    "max_required_missing_ratio": 0.1,
                    "deduplicate_keep": "last",
                }
            }
        )
        self.assertTrue(res["ok"])
        self.assertEqual(res["errors"], [])

    def test_validate_cleaning_rules_error(self):
        res = cleaning.validate_cleaning_rules(
            {
                "rules": {
                    "platform_mode": "abc",
                    "amount_round_digits": 99,
                    "max_invalid_ratio": 2,
                    "max_required_missing_ratio": 3,
                    "filters": [{"op": "bad"}],
                }
            }
        )
        self.assertFalse(res["ok"])
        self.assertGreaterEqual(len(res["errors"]), 3)

    def test_validate_cleaning_rules_warn_unknown(self):
        res = cleaning.validate_cleaning_rules({"rules": {"unknown_x": 1}})
        self.assertTrue(res["ok"])
        self.assertTrue(any("unknown rule keys" in w for w in res["warnings"]))

    def test_quality_gates_fail_on_invalid_rows(self):
        quality = {"input_rows": 10, "output_rows": 8, "invalid_rows": 2, "filtered_rows": 0}
        with self.assertRaises(RuntimeError) as ctx:
            cleaning._apply_quality_gates(quality, {"max_invalid_rows": 1})
        self.assertIn("max_invalid_rows", str(ctx.exception))

    def test_quality_gates_fail_on_invalid_ratio(self):
        quality = {"input_rows": 10, "output_rows": 8, "invalid_rows": 2, "filtered_rows": 0}
        with self.assertRaises(RuntimeError) as ctx:
            cleaning._apply_quality_gates(quality, {"rules": {"max_invalid_ratio": "0.1"}})
        self.assertIn("max_invalid_ratio", str(ctx.exception))

    def test_quality_gates_pass(self):
        quality = {"input_rows": 10, "output_rows": 8, "invalid_rows": 1, "filtered_rows": 1}
        out = cleaning._apply_quality_gates(
            quality,
            {
                "rules": {
                    "max_invalid_rows": 2,
                    "max_filtered_rows": 2,
                    "min_output_rows": 5,
                    "max_invalid_ratio": "0.2",
                }
            },
        )
        self.assertTrue(out["evaluated"])

    def test_clean_rows_generic_rules_pipeline(self):
        raw_rows = [
            {"name": " Alice ", "amt": "12.4", "city": "shanghai"},
            {"name": "bob", "amt": "9.9", "city": "beijing"},
            {"name": "alice", "amt": "100.1", "city": "shanghai"},
        ]
        cleaned = cleaning._clean_rows(
            raw_rows,
            {
                "rules": {
                    "platform_mode": "generic",
                    "rename_map": {"amt": "amount"},
                    "casts": {"amount": "float"},
                    "trim_strings": True,
                    "lowercase_fields": ["name"],
                    "filters": [{"field": "amount", "op": "gte", "value": 10}],
                    "deduplicate_by": ["name"],
                    "deduplicate_keep": "first",
                    "sort_by": [{"field": "amount", "order": "desc"}],
                }
            },
        )
        self.assertEqual(cleaned["quality"]["output_rows"], 1)
        self.assertEqual(cleaned["rows"][0]["name"], "alice")
        self.assertEqual(cleaned["rows"][0]["amount"], 12.4)

    def test_try_accel_cleaning_sends_params_payload(self):
        fake_resp = Mock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {"ok": True}
        with patch("requests.post", return_value=fake_resp) as post:
            cleaning._try_accel_cleaning(
                params={"rows": [{"id": 1, "amount": 2}], "rules": {"max_amount": 10}},
                job_id="j1",
                step_id="cleaning",
                actor="local",
                ruleset_version="v1",
                input_uri="in",
                output_uri="out",
            )
            kwargs = post.call_args.kwargs
            sent = kwargs["json"]
            self.assertIn("params", sent)
            self.assertIn("rows", sent["params"])
            self.assertIn("rules", sent["params"])
    def test_clean_rows_supports_declarative_rules_block(self):
        raw_rows = [
            {"ID": "1", "AMT": "10.5"},
            {"ID": "1", "AMT": "11.5"},
            {"ID": "2", "AMT": "-1"},
        ]
        cleaned = cleaning._clean_rows(
            raw_rows,
            {
                "rules": {
                    "id_field": "ID",
                    "amount_field": "AMT",
                    "drop_negative_amount": True,
                    "deduplicate_by_id": True,
                    "deduplicate_keep": "first",
                    "amount_round_digits": 0,
                }
            },
        )
        self.assertEqual(cleaned["rows"], [{"id": 1, "amount": 11.0}])
        self.assertEqual(cleaned["quality"]["rule_hits"]["filtered_negative"], 1)

    def test_clean_rows_applies_filters_dedup_and_rounding(self):
        raw_rows = [
            {"id": "1", "amount": "100.126"},
            {"id": "2", "amount": "-5"},
            {"id": "bad", "amount": "3"},
            {"id": "1", "amount": "$120.225"},
            {"id": "3", "amount": "999"},
        ]
        cleaned = cleaning._clean_rows(
            raw_rows,
            {
                "drop_negative_amount": True,
                "max_amount": 500,
                "deduplicate_by_id": True,
                "deduplicate_keep": "last",
                "amount_round_digits": 2,
            },
        )
        rows = cleaned["rows"]
        quality = cleaned["quality"]

        self.assertEqual(rows, [{"id": 1, "amount": 120.23}])
        self.assertEqual(quality["input_rows"], 5)
        self.assertEqual(quality["output_rows"], 1)
        self.assertEqual(quality["invalid_rows"], 1)
        self.assertEqual(quality["filtered_rows"], 2)
        self.assertEqual(quality["duplicate_rows_removed"], 1)

    def test_load_rows_from_input_csv_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            csv_path = os.path.join(tmp, "in.csv")
            with open(csv_path, "w", encoding="utf-8", newline="\n") as f:
                f.write("id,amount\n10,88.1\n11,91.2\n")

            rows, source = cleaning._load_raw_rows({"input_csv_path": csv_path}, tmp)
            self.assertEqual(source, csv_path)
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["id"], "10")
            self.assertEqual(rows[1]["amount"], "91.2")

    def test_clean_rows_large_input_quality_counts(self):
        raw_rows = [{"id": i, "amount": i} for i in range(1, 2001)]
        raw_rows.append({"id": "bad", "amount": "1"})
        raw_rows.append({"id": 2, "amount": -3})
        cleaned = cleaning._clean_rows(
            raw_rows,
            {
                "drop_negative_amount": True,
                "max_amount": 1500,
            },
        )
        quality = cleaned["quality"]
        self.assertEqual(quality["input_rows"], 2002)
        self.assertEqual(quality["invalid_rows"], 1)
        self.assertEqual(quality["filtered_rows"], 501)
        self.assertEqual(quality["output_rows"], 1500)

    def test_is_valid_parquet_file_checks_magic_bytes(self):
        with tempfile.TemporaryDirectory() as tmp:
            ok_path = os.path.join(tmp, "ok.parquet")
            bad_path = os.path.join(tmp, "bad.parquet")

            with open(ok_path, "wb") as f:
                f.write(b"PAR1abcdPAR1")
            with open(bad_path, "wb") as f:
                f.write(b"PARQUET_PLACEHOLDER\n")

            self.assertTrue(cleaning._is_valid_parquet_file(ok_path))
            self.assertFalse(cleaning._is_valid_parquet_file(bad_path))
            self.assertFalse(cleaning._is_valid_parquet_file(os.path.join(tmp, "missing.parquet")))

    def test_run_cleaning_falls_back_when_accel_parquet_invalid(self):
        with tempfile.TemporaryDirectory() as tmp:
            accel_stage = os.path.join(tmp, "accel")
            os.makedirs(accel_stage, exist_ok=True)
            accel_parquet = os.path.join(accel_stage, "cleaned.parquet")
            with open(accel_parquet, "wb") as f:
                f.write(b"PARQUET_PLACEHOLDER\n")

            local_job_root = os.path.join(tmp, "job")

            def write_local_csv(path, rows):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("id,amount\n1,100\n2,200\n")
                return {"rows": 2, "cols": 2}

            def write_local_parquet(path, rows):
                with open(path, "wb") as f:
                    f.write(b"PAR1dataPAR1")

            def write_local_bin(path, *args, **kwargs):
                with open(path, "wb") as f:
                    f.write(b"BIN")

            def write_local_json(path, profile, params):
                with open(path, "w", encoding="utf-8") as f:
                    f.write("{}")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={
                    "attempted": True,
                    "ok": True,
                    "response": {
                        "outputs": {
                            "cleaned_csv": {"path": accel_parquet, "sha256": ""},
                            "cleaned_parquet": {"path": accel_parquet, "sha256": ""},
                            "profile_json": {"path": accel_parquet, "sha256": ""},
                            "xlsx_fin": {"path": accel_parquet, "sha256": ""},
                            "audit_docx": {"path": accel_parquet, "sha256": ""},
                            "deck_pptx": {"path": accel_parquet, "sha256": ""},
                        },
                        "profile": {"rows": 2, "cols": 2},
                    },
                },
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_csv", side_effect=write_local_csv
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_parquet", side_effect=write_local_parquet
            ), patch(
                "aiwf.flows.cleaning._write_fin_xlsx", side_effect=write_local_bin
            ), patch(
                "aiwf.flows.cleaning._write_audit_docx", side_effect=write_local_bin
            ), patch(
                "aiwf.flows.cleaning._write_deck_pptx", side_effect=write_local_bin
            ), patch(
                "aiwf.flows.cleaning._write_profile_json", side_effect=write_local_json
            ):
                out = cleaning.run_cleaning(
                    job_id="job-1",
                    actor="test",
                    params={"job_root": local_job_root},
                )

            self.assertTrue(out["ok"])
            self.assertTrue(out["accel"]["attempted"])
            self.assertTrue(out["accel"]["used_fallback"])
            self.assertIn("invalid parquet", str(out["accel"]["validation_error"]))
            parquet_artifact = [a for a in out["artifacts"] if a["kind"] == "parquet"][0]
            self.assertTrue(parquet_artifact["path"].startswith(local_job_root))
            self.assertNotEqual(parquet_artifact["path"], accel_parquet)

    def test_run_cleaning_fails_when_local_parquet_invalid_in_strict_mode(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")

            def write_bad_parquet(path, rows):
                with open(path, "wb") as f:
                    f.write(b"PARQUET_PLACEHOLDER\n")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ), patch(
                "aiwf.flows.cleaning._write_cleaned_parquet", side_effect=write_bad_parquet
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    cleaning.run_cleaning(
                        job_id="job-strict",
                        actor="test",
                        params={"job_root": local_job_root, "local_parquet_strict": True},
                    )
                self.assertIn("strict mode enabled", str(ctx.exception))

    def test_run_cleaning_fails_on_quality_gate(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ):
                with self.assertRaises(RuntimeError) as ctx:
                    cleaning.run_cleaning(
                        job_id="job-gate",
                        actor="test",
                        params={
                            "job_root": local_job_root,
                            "rows": [{"id": "bad", "amount": "1"}],
                            "max_invalid_rows": 0,
                        },
                    )
                self.assertIn("quality gate failed", str(ctx.exception))

    def test_run_cleaning_generic_mode_skips_accel(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                side_effect=AssertionError("accel should be skipped"),
            ):
                out = cleaning.run_cleaning(
                    job_id="job-generic",
                    actor="test",
                    params={
                        "job_root": local_job_root,
                        "rows": [{"name": "A", "amount": 12}],
                        "rules": {"platform_mode": "generic"},
                    },
                )
            self.assertTrue(out["ok"])
            self.assertFalse(out["accel"]["attempted"])

    def test_run_cleaning_with_preprocess_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")
            os.makedirs(local_job_root, exist_ok=True)
            in_csv = os.path.join(local_job_root, "raw.csv")
            with open(in_csv, "w", encoding="utf-8", newline="\n") as f:
                f.write('ID,Amt\n')
                f.write('1,"$10.20"\n')
                f.write('2," 11 "\n')

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ):
                out = cleaning.run_cleaning(
                    job_id="job-pre",
                    actor="test",
                    params={
                        "job_root": local_job_root,
                        "preprocess": {
                            "enabled": True,
                            "input_path": in_csv,
                            "header_map": {"ID": "id", "Amt": "amount"},
                            "amount_fields": ["amount"],
                        },
                    },
                )
            self.assertTrue(out["ok"])
            self.assertIn("preprocess", out["profile"])
            self.assertEqual(out["profile"]["preprocess"]["summary"]["input_rows"], 2)

    def test_run_cleaning_with_preprocess_pipeline_enabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            local_job_root = os.path.join(tmp, "job")
            os.makedirs(local_job_root, exist_ok=True)
            in_txt = os.path.join(local_job_root, "raw.txt")
            with open(in_txt, "w", encoding="utf-8") as f:
                f.write("ID:1 amount:10\nID:2 amount:11")

            with patch("aiwf.flows.cleaning._base_step_start"), patch(
                "aiwf.flows.cleaning._base_artifact_upsert"
            ), patch("aiwf.flows.cleaning._base_step_done"), patch(
                "aiwf.flows.cleaning._base_step_fail"
            ), patch(
                "aiwf.flows.cleaning._try_accel_cleaning",
                return_value={"attempted": True, "ok": False, "error": "accel unavailable"},
            ):
                out = cleaning.run_cleaning(
                    job_id="job-pre-pipeline",
                    actor="test",
                    params={
                        "job_root": local_job_root,
                        "preprocess": {
                            "enabled": True,
                            "input_path": in_txt,
                            "pipeline": {
                                "enabled": True,
                                "stages": [
                                    {"name": "extract", "config": {"input_files": [in_txt]}},
                                    {
                                        "name": "clean",
                                        "config": {
                                            "header_map": {"text": "amount"},
                                            "field_transforms": [{"field": "amount", "op": "extract_regex", "pattern": "amount:(\\d+)", "group": 1}],
                                        },
                                    },
                                ],
                            },
                        },
                    },
                )
            self.assertTrue(out["ok"])
            self.assertIn("preprocess", out["profile"])
            self.assertEqual(out["profile"]["preprocess"]["mode"], "pipeline")

    def test_clean_rows_use_rust_v2_when_enabled(self):
        fake_resp = Mock()
        fake_resp.status_code = 200
        fake_resp.json.return_value = {
            "ok": True,
            "rows": [{"id": 1, "amount": 10.0}],
            "quality": {"input_rows": 1, "output_rows": 1, "invalid_rows": 0, "filtered_rows": 0, "duplicate_rows_removed": 0},
            "trace_id": "t1",
        }
        with patch("requests.post", return_value=fake_resp):
            out = cleaning._clean_rows(
                [{"id": "1", "amount": "10"}],
                {"rules": {"use_rust_v2": True}},
            )
        self.assertEqual(out["rows"], [{"id": 1, "amount": 10.0}])
        self.assertTrue(out["quality"].get("rust_v2_used"))
        self.assertEqual(out["quality"].get("rust_v2_trace_id"), "t1")

    def test_clean_rows_fallback_when_rust_v2_unavailable(self):
        with patch("requests.post", side_effect=RuntimeError("unreachable")):
            out = cleaning._clean_rows(
                [{"id": "1", "amount": "10"}],
                {"rules": {"use_rust_v2": True}},
            )
        self.assertEqual(out["rows"], [{"id": 1, "amount": 10.0}])
        self.assertFalse(out["quality"].get("rust_v2_used"))


if __name__ == "__main__":
    unittest.main()
