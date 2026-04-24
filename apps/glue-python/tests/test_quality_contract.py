import unittest

from aiwf.canonical_profiles import get_profile_registry
from aiwf.flows.cleaning_config import to_int
from aiwf.quality_contract import analyze_header_mapping, canonicalize_header, normalize_value_for_field


class QualityContractTests(unittest.TestCase):
    def test_canonicalize_header_supports_chinese_aliases(self):
        field, confidence, matched = canonicalize_header("\u5ba2\u6237\u540d\u79f0")
        self.assertEqual(field, "customer_name")
        self.assertGreaterEqual(confidence, 0.88)
        self.assertTrue(matched)

    def test_canonicalize_header_supports_multirow_chinese_headers(self):
        field, confidence, _matched = canonicalize_header(
            "\u6536\u6b3e\u4fe1\u606f \u91d1\u989d\uff08\u4e07\u5143\uff09",
            {"canonical_profile": "finance_statement"},
        )
        self.assertEqual(field, "amount")
        self.assertGreaterEqual(confidence, 0.88)

    def test_normalize_value_for_field_handles_chinese_amount_units(self):
        self.assertEqual(
            normalize_value_for_field("12.5", "amount", raw_header="\u91d1\u989d\uff08\u4e07\u5143\uff09"),
            125000.0,
        )
        self.assertEqual(normalize_value_for_field("2\u4ebf\u5143", "amount"), 200000000.0)

    def test_normalize_value_for_field_handles_chinese_dates(self):
        self.assertEqual(normalize_value_for_field("2026\u5e743\u67081\u65e5", "biz_date"), "2026-03-01")
        self.assertEqual(normalize_value_for_field("20260302", "published_at"), "2026-03-02")

    def test_normalize_value_for_field_handles_phone_fallback(self):
        self.assertEqual(normalize_value_for_field("138 0013 8000", "phone"), "+8613800138000")

    def test_canonicalize_header_supports_bank_statement_aliases(self):
        field, confidence, _matched = canonicalize_header(
            "\u5bf9\u65b9\u6237\u540d",
            {"canonical_profile": "bank_statement"},
        )
        self.assertEqual(field, "counterparty_name")
        self.assertGreaterEqual(confidence, 0.88)

    def test_canonicalize_header_strict_mode_keeps_bank_abbrev_unresolved(self):
        field, confidence, _matched = canonicalize_header(
            "Bal",
            {"canonical_profile": "bank_statement", "header_mapping_mode": "strict"},
        )
        self.assertNotEqual(field, "balance")
        self.assertLess(confidence, 0.88)

    def test_canonicalize_header_auto_mode_resolves_bank_abbrev(self):
        field, confidence, _matched = canonicalize_header(
            "Acct No",
            {"canonical_profile": "bank_statement", "header_mapping_mode": "auto"},
        )
        self.assertEqual(field, "account_no")
        self.assertGreaterEqual(confidence, 0.82)

    def test_canonicalize_header_auto_mode_keeps_unknown_header_unresolved(self):
        details = analyze_header_mapping("Value", {"header_mapping_mode": "auto"})
        self.assertFalse(details["resolved"])
        self.assertEqual(details["match_strategy"], "unresolved")

    def test_canonicalize_header_auto_mode_uses_value_affinity(self):
        details = analyze_header_mapping(
            "Posting Dt",
            {"canonical_profile": "bank_statement", "header_mapping_mode": "auto"},
            sample_values=["2026/03/01", "2026-03-02"],
        )
        self.assertTrue(details["resolved"])
        self.assertEqual(details["canonical_field"], "txn_date")
        self.assertIn(details["match_strategy"], {"exact", "fuzzy+value_affinity"})

    def test_normalize_value_for_field_handles_bank_statement_numeric_and_date_fields(self):
        self.assertEqual(normalize_value_for_field("1,234.50", "debit_amount"), 1234.5)
        self.assertEqual(normalize_value_for_field("5.6", "balance", raw_header="\u4f59\u989d\uff08\u4e07\u5143\uff09"), 56000.0)
        self.assertEqual(normalize_value_for_field("2026\u5e743\u67082\u65e5", "txn_date"), "2026-03-02")

    def test_quality_contract_uses_shared_profile_registry(self):
        registry = get_profile_registry()
        self.assertEqual(registry["finance_statement"]["required_fields"], ["id", "amount"])
        self.assertEqual(registry["bank_statement"]["required_fields"], ["account_no", "txn_date"])
        self.assertEqual(registry["customer_ledger"]["required_fields"], ["customer_name", "phone", "amount", "biz_date"])
        self.assertIn("speaker_role", registry["debate_evidence"]["string_fields"])
        self.assertIn("argument_role", registry["debate_evidence"]["string_fields"])
        self.assertIn("source_path", registry["debate_evidence"]["required_fields"])
        field, confidence, _matched = canonicalize_header(
            "\u672c\u671f\u91d1\u989d",
            {"canonical_profile": "finance_statement"},
        )
        self.assertEqual(field, "amount")
        self.assertGreaterEqual(confidence, 0.88)

    def test_canonicalize_header_supports_debate_extended_aliases(self):
        field, confidence, _matched = canonicalize_header(
            "发言角色",
            {"canonical_profile": "debate_evidence"},
        )
        self.assertEqual(field, "speaker_role")
        self.assertGreaterEqual(confidence, 0.82)

    def test_normalize_value_for_field_handles_customer_ledger_amount_and_date(self):
        self.assertEqual(normalize_value_for_field("1,200", "amount"), 1200.0)
        self.assertEqual(normalize_value_for_field("2026\u5e743\u67081\u65e5", "biz_date"), "2026-03-01")

    def test_normalize_value_for_field_handles_accounting_negative_notation(self):
        self.assertEqual(normalize_value_for_field("(1,234.50)", "amount"), -1234.5)
        self.assertEqual(normalize_value_for_field("\uff081,234.50\uff09", "amount"), -1234.5)
        self.assertEqual(normalize_value_for_field("1,234.50-", "amount"), -1234.5)
        self.assertEqual(normalize_value_for_field("\u22121,234.50", "amount"), -1234.5)

    def test_normalize_value_for_field_handles_directional_amount_text(self):
        self.assertEqual(normalize_value_for_field("\u501f 120.50", "amount"), -120.5)
        self.assertEqual(normalize_value_for_field("\u8d37 300.00", "amount"), 300.0)
        self.assertEqual(normalize_value_for_field("DR 120.50", "amount"), -120.5)
        self.assertEqual(normalize_value_for_field("CR 300.00", "amount"), 300.0)

    def test_normalize_value_for_field_handles_currency_and_unit_mixed_amount_text(self):
        self.assertEqual(normalize_value_for_field("\uffe51.2\u4e07\u5143", "amount"), 12000.0)
        self.assertEqual(normalize_value_for_field("USD 1,234.50", "amount"), 1234.5)

    def test_normalize_value_for_field_preserves_large_integer_ids(self):
        self.assertEqual(normalize_value_for_field("9007199254740993", "id"), 9007199254740993)

    def test_to_int_preserves_large_integer_strings(self):
        self.assertEqual(to_int("9007199254740993"), 9007199254740993)


if __name__ == "__main__":
    unittest.main()
