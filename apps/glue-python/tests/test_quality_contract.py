import unittest

from aiwf.canonical_profiles import get_profile_registry
from aiwf.quality_contract import canonicalize_header, normalize_value_for_field


class QualityContractTests(unittest.TestCase):
    def test_canonicalize_header_supports_chinese_aliases(self):
        field, confidence, matched = canonicalize_header("客户名称")
        self.assertEqual(field, "customer_name")
        self.assertGreaterEqual(confidence, 0.88)
        self.assertTrue(matched)

    def test_canonicalize_header_supports_multirow_chinese_headers(self):
        field, confidence, _matched = canonicalize_header(
            "收款信息 金额（万元）",
            {"canonical_profile": "finance_statement"},
        )
        self.assertEqual(field, "amount")
        self.assertGreaterEqual(confidence, 0.88)

    def test_normalize_value_for_field_handles_chinese_amount_units(self):
        self.assertEqual(
            normalize_value_for_field("12.5", "amount", raw_header="金额（万元）"),
            125000.0,
        )
        self.assertEqual(normalize_value_for_field("2亿元", "amount"), 200000000.0)

    def test_normalize_value_for_field_handles_chinese_dates(self):
        self.assertEqual(normalize_value_for_field("2026年3月1日", "biz_date"), "2026-03-01")
        self.assertEqual(normalize_value_for_field("20260302", "published_at"), "2026-03-02")

    def test_normalize_value_for_field_handles_phone_fallback(self):
        self.assertEqual(normalize_value_for_field("138 0013 8000", "phone"), "+8613800138000")

    def test_quality_contract_uses_shared_profile_registry(self):
        registry = get_profile_registry()
        self.assertEqual(registry["finance_statement"]["required_fields"], ["id", "amount"])
        field, confidence, _matched = canonicalize_header(
            "鏀舵淇℃伅 閲戦锛堜竾鍏冿級",
            {"canonical_profile": "finance_statement"},
        )
        self.assertEqual(field, "amount")
        self.assertGreaterEqual(confidence, 0.88)


if __name__ == "__main__":
    unittest.main()
