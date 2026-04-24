import unittest

from aiwf import preprocess


class PreprocessOpsTests(unittest.TestCase):
    def _apply(self, value, op):
        return preprocess._apply_field_transform(value, op, {})

    def test_extract_speaker_prefix_variants(self):
        value, changed = self._apply("Alice: Public transit reduces congestion.", "extract_speaker_prefix")
        self.assertEqual(value, "Alice")
        self.assertTrue(changed)

        value, changed = self._apply("Speaker B: Public transit reduces congestion.", "extract_speaker_prefix")
        self.assertEqual(value, "B")
        self.assertTrue(changed)

        value, changed = self._apply("正方一辩：我们支持公共交通。", "extract_speaker_prefix")
        self.assertEqual(value, "正方一辩")
        self.assertTrue(changed)

        value, changed = self._apply('Moderator: "Cleaner air" supports the affirmative case.', "extract_speaker_prefix")
        self.assertEqual(value, "Moderator")
        self.assertTrue(changed)

        value, changed = self._apply("Account No: 62220001", "extract_speaker_prefix")
        self.assertEqual(value, "")
        self.assertFalse(changed)

    def test_normalize_stance_bilingual_variants(self):
        value, changed = self._apply("正方一辩：我们支持公共交通。", "normalize_stance")
        self.assertEqual(value, "pro")
        self.assertTrue(changed)

        value, changed = self._apply("Speaker B: We oppose the policy.", "normalize_stance")
        self.assertEqual(value, "con")
        self.assertTrue(changed)

        value, changed = self._apply("Moderator: neutral framing only.", "normalize_stance")
        self.assertEqual(value, "neutral")
        self.assertTrue(changed)

        value, changed = self._apply("This line only reports facts.", "normalize_stance")
        self.assertEqual(value, "unknown")
        self.assertTrue(changed)

    def test_clean_citation_preserves_non_citation_text(self):
        value, changed = self._apply(
            "A claim [1] (Source: Example Report) https://example.com/x",
            "clean_citation",
        )
        self.assertEqual(value, "A claim https://example.com/x")
        self.assertTrue(changed)

    def test_strip_ocr_noise_and_collapse_duplicate_lines(self):
        value, changed = self._apply("Page 3", "strip_ocr_noise")
        self.assertEqual(value, "")
        self.assertTrue(changed)

        value, changed = self._apply("Time:\x01 4.11\x01 13:30\x01", "strip_ocr_noise")
        self.assertEqual(value, "Time: 4.11 13:30")
        self.assertTrue(changed)

        value, changed = self._apply("Alphaaaaa", "strip_ocr_noise")
        self.assertEqual(value, "Alpha")
        self.assertTrue(changed)

        value, changed = self._apply("Alpha\nAlpha\nBeta", "collapse_duplicate_lines")
        self.assertEqual(value, "Alpha\nBeta")
        self.assertTrue(changed)

    def test_detect_quote_only_distinguishes_quote_rows(self):
        value, changed = self._apply('Reporter: "Cleaner air" supports the case.', "detect_quote_only")
        self.assertEqual(value, "quote")
        self.assertTrue(changed)

        value, changed = self._apply("Alice: Public transit reduces congestion.", "detect_quote_only")
        self.assertEqual(value, "claim")
        self.assertTrue(changed)


if __name__ == "__main__":
    unittest.main()
