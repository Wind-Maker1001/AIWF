# Bank Statement Template v1

## Purpose
`bank_statement_v1` normalizes bank statement and reconciliation-style tables into a stable transaction dataset.

Core fields:
- `account_no`
- `txn_date`
- `debit_amount`
- `credit_amount`
- `amount`
- `balance`
- `counterparty_name`
- `remark`
- `ref_no`
- `txn_type`

## Entry Points
- API payload: `params.cleaning_template = "bank_statement_v1"`
- Primary template: `rules/templates/bank_statement_v1.cleaning_spec_v2.json`
- Compatibility template: `rules/templates/generic_bank_statement_standardize.json`
- Template metadata:
  - `template_expected_profile = bank_statement`
  - `blank_output_expected = false`
  - template-driven runs default to `profile_mismatch_action = block`

## Runtime Semantics
- canonical profile: `bank_statement`
- `amount` is the normalized signed amount
- `debit_amount` and `credit_amount` are preserved side by side
- default currency: `CNY`
- unique key: `account_no + txn_date + ref_no + amount`

## Typical Inputs
- `xlsx`
- `csv`
- `jsonl`
- OCR/PDF tabular inputs that recover the same banking columns

Common header variants include:
- `Acct No`, `账号`, `账户号`
- `Posting Dt`, `交易日期`, `记账日期`
- `DR`, `借方金额`
- `CR`, `贷方金额`
- `Bal`, `余额`
- `Memo`, `备注`
- `Ref No`, `流水号`

## Quality Gates
- `max_required_missing_ratio = 0.0`
- `duplicate_key_ratio_max = 0.1`
- `numeric_parse_rate_min = 0.95`
- `date_parse_rate_min = 0.95`
- `allow_empty_output = false`

## Common Failures
- `account_no` missing: header mapping did not resolve to the account field
- `txn_date` parse failure: the date column uses a non-standard format
- `numeric_parse_rate` too low: amount/balance columns contain extra text or noise
- `duplicate_key_ratio` too high: the same transaction was exported multiple times
- `profile_mismatch_blocked`: the input is not bank-statement-shaped and should use another template

## Notes
- This template is separate from `finance_report_v1`; do not use it for balance sheet / income statement data.
- OCR/PDF support depends on the existing sidecar extraction path; the template itself does not add a separate banking OCR engine.
