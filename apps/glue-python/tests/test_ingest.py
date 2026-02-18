import os
import tempfile
import unittest
from unittest.mock import patch

from aiwf import ingest


class IngestTests(unittest.TestCase):
    def test_ocr_try_modes_defaults_and_parse(self):
        with patch.dict(os.environ, {}, clear=True):
            self.assertEqual(ingest._ocr_try_modes(None), ["adaptive", "gray", "none"])
        with patch.dict(os.environ, {"AIWF_OCR_TRY_MODES": "gray,none"}, clear=False):
            self.assertEqual(ingest._ocr_try_modes(None), ["gray", "none"])
        self.assertEqual(ingest._ocr_try_modes("none"), ["none"])

    def test_ocr_extract_text_prefers_higher_signal_score(self):
        class DummyT:
            @staticmethod
            def image_to_string(img, lang=None, config=None):
                mapping = {
                    "adaptive": "abc",
                    "gray": "abc123中文",
                    "none": "ab",
                }
                return mapping.get(str(img), "")

        with patch("aiwf.ingest._preprocess_image_for_ocr") as pre:
            pre.side_effect = lambda image, mode: mode
            text = ingest._ocr_extract_text(DummyT(), object(), "eng+chi_sim", "--psm 6", ["adaptive", "gray", "none"])
            self.assertEqual(text, "abc123中文")

    def test_resolve_tesseract_cmd_prefers_env(self):
        with patch.dict(os.environ, {"TESSERACT_CMD": r"C:\custom\tesseract.exe"}, clear=False):
            with patch("aiwf.ingest.os.path.exists") as exists:
                exists.side_effect = lambda p: p == r"C:\custom\tesseract.exe"
                self.assertEqual(ingest._resolve_tesseract_cmd(), r"C:\custom\tesseract.exe")

    def test_resolve_tesseract_cmd_uses_known_candidates(self):
        with patch.dict(os.environ, {}, clear=True):
            with patch("aiwf.ingest.os.path.exists") as exists:
                exists.side_effect = lambda p: p == r"C:\Program Files\Tesseract-OCR\tesseract.exe"
                self.assertEqual(ingest._resolve_tesseract_cmd(), r"C:\Program Files\Tesseract-OCR\tesseract.exe")

    def test_read_txt(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "a.txt")
            with open(p, "w", encoding="utf-8") as f:
                f.write("line1\n\nline2")
            rows, meta = ingest.load_rows_from_file(p)
            self.assertEqual(meta["input_format"], "txt")
            self.assertEqual(len(rows), 2)
            self.assertEqual(rows[0]["text"], "line1")

    def test_read_xlsx(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "a.xlsx")
            from openpyxl import Workbook  # type: ignore

            wb = Workbook()
            ws = wb.active
            ws.title = "S1"
            ws.append(["id", "amount"])
            ws.append([1, 10.5])
            ws2 = wb.create_sheet("S2")
            ws2.append(["id", "amount"])
            ws2.append([2, 20.0])
            wb.save(p)

            rows, meta = ingest.load_rows_from_file(p)
            self.assertEqual(meta["input_format"], "xlsx")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["id"], 1)
            self.assertEqual(rows[0]["source_type"], "xlsx")
            self.assertEqual(rows[0]["sheet_name"], "S1")

            rows_all, _ = ingest.load_rows_from_file(p, xlsx_all_sheets=True)
            self.assertEqual(len(rows_all), 2)
            self.assertEqual({r["sheet_name"] for r in rows_all}, {"S1", "S2"})

    def test_read_docx(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "a.docx")
            from docx import Document  # type: ignore

            d = Document()
            d.add_paragraph("debate evidence paragraph")
            d.save(p)

            rows, meta = ingest.load_rows_from_file(p)
            self.assertEqual(meta["input_format"], "docx")
            self.assertGreaterEqual(len(rows), 1)
            self.assertIn("debate", rows[0]["text"])

    def test_skip_image_when_ocr_disabled(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = os.path.join(tmp, "a.png")
            with open(p, "wb") as f:
                f.write(b"not-a-real-image")
            rows, meta = ingest.load_rows_from_file(p, ocr_enabled=False)
            self.assertEqual(rows, [])
            self.assertEqual(meta["input_format"], "image")
            self.assertTrue(meta.get("skipped"))


if __name__ == "__main__":
    unittest.main()
