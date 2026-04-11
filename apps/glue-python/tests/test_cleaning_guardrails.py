import os
import tempfile
import unittest
from unittest.mock import patch

from aiwf.cleaning_spec_v2 import build_header_mapping
from aiwf.flows.cleaning_errors import CleaningGuardrailError
from aiwf.flows import cleaning


def make_job_context(job_root: str) -> dict[str, str]:
    job_root = os.path.normpath(job_root)
    return {
        "job_root": job_root,
        "stage_dir": os.path.join(job_root, "stage"),
        "artifacts_dir": os.path.join(job_root, "artifacts"),
        "evidence_dir": os.path.join(job_root, "evidence"),
    }


def with_job_context(job_root: str, **params):
    out = dict(params)
    out["job_context"] = make_job_context(job_root)
    return out


class CleaningGuardrailTests(unittest.TestCase):
    def test_prepare_cleaning_params_resolves_template_metadata_and_fallback_headers(self):
        finance = cleaning._prepare_cleaning_params({"cleaning_template": "finance_report_v1"})
        self.assertEqual(finance["template_expected_profile"], "finance_statement")
        self.assertFalse(finance["blank_output_expected"])
        self.assertEqual(finance["rules"]["rename_map"]["Amt"], "amount")

        bank = cleaning._prepare_cleaning_params({"cleaning_template": "bank_statement_v1"})
        self.assertEqual(bank["rules"]["rename_map"]["Acct No"], "account_no")
        self.assertEqual(bank["rules"]["rename_map"]["Posting Dt"], "txn_date")
        self.assertEqual(bank["rules"]["rename_map"]["DR"], "debit_amount")

        customer = cleaning._prepare_cleaning_params({"cleaning_template": "customer_contact_v1"})
        self.assertEqual(customer["rules"]["rename_map"]["Cust Name"], "customer_name")
        self.assertEqual(customer["rules"]["rename_map"]["Mobile No"], "phone")

    def test_build_header_mapping_supports_clean_utf8_bank_aliases(self):
        mapping = build_header_mapping(
            ["账号", "交易日期", "借方金额", "贷方金额", "余额"],
            canonical_profile="bank_statement",
            header_mapping_mode="strict",
        )
        fields = {item["raw_header"]: item["canonical_field"] for item in mapping}
        self.assertEqual(fields["账号"], "account_no")
        self.assertEqual(fields["交易日期"], "txn_date")
        self.assertEqual(fields["借方金额"], "debit_amount")
        self.assertEqual(fields["贷方金额"], "credit_amount")
        self.assertEqual(fields["余额"], "balance")

    def test_run_cleaning_finance_template_cleans_amt_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_root = os.path.join(tmp, "job")
            csv_path = os.path.join(tmp, "finance.csv")
            with open(csv_path, "w", encoding="utf-8", newline="\n") as handle:
                handle.write("ID,Amt,currency,subject\n")
                handle.write("101,1000.25,cny,主营业务收入\n")
                handle.write("102,2300.50,CNY,销售费用\n")

            def write_valid_parquet(path, rows):
                with open(path, "wb") as handle:
                    handle.write(b"PAR1dataPAR1")

            with patch("aiwf.flows.cleaning._write_cleaned_parquet", side_effect=write_valid_parquet):
                out = cleaning.run_cleaning(
                    job_id="job-finance-template",
                    actor="test",
                    params=with_job_context(
                        job_root,
                        local_standalone=True,
                        office_outputs_enabled=False,
                        cleaning_template="finance_report_v1",
                        input_csv_path=csv_path,
                    ),
                )

        self.assertTrue(out["ok"])
        self.assertEqual(out["profile"]["rows"], 2)
        self.assertEqual(out["quality_summary"]["requested_profile"], "finance_statement")
        self.assertEqual(out["quality_summary"]["recommended_profile"], "finance_statement")
        self.assertFalse(out["quality_summary"]["profile_mismatch"])
        self.assertGreaterEqual(out["quality_summary"]["required_field_coverage"], 1.0)

    def test_run_cleaning_blocks_high_confidence_template_profile_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_root = os.path.join(tmp, "job")
            rows = [
                {
                    "claim_text": "Tax policy is good and should be supported.",
                    "speaker": "Alice",
                    "source_url": "https://example.com/report",
                }
            ]
            with self.assertRaisesRegex(CleaningGuardrailError, "profile mismatch blocked"):
                cleaning.run_cleaning(
                    job_id="job-profile-block",
                    actor="test",
                    params=with_job_context(
                        job_root,
                        local_standalone=True,
                        office_outputs_enabled=False,
                        cleaning_template="finance_report_v1",
                        rows=rows,
                    ),
                )

    def test_run_cleaning_blocks_zero_output_when_blank_not_expected(self):
        with tempfile.TemporaryDirectory() as tmp:
            job_root = os.path.join(tmp, "job")
            with self.assertRaises(CleaningGuardrailError) as ctx:
                cleaning.run_cleaning(
                    job_id="job-zero-output-block",
                    actor="test",
                    params=with_job_context(
                        job_root,
                        local_standalone=True,
                        office_outputs_enabled=False,
                        rows=[{"id": 1, "amount": 10.0}],
                        rules={
                            "use_rust_v2": False,
                            "filters": [{"field": "amount", "op": "gte", "value": 100}],
                            "allow_empty_output": False,
                        },
                    ),
                )
        self.assertEqual(ctx.exception.error_code, "zero_output_unexpected")
        self.assertIn("zero_output_unexpected", ctx.exception.reason_codes)


if __name__ == "__main__":
    unittest.main()
