# Customer Ledger Template v1

## Purpose
`customer_ledger_v1` normalizes lightweight customer ledger tables into a contact-plus-amount record set.

Core fields:
- `customer_name`
- `phone`
- `city`
- `amount`
- `biz_date`

## Entry Points
- API payload: `params.cleaning_template = "customer_ledger_v1"`
- Primary template: `rules/templates/customer_ledger_v1.cleaning_spec_v2.json`
- Compatibility template: `rules/templates/generic_customer_ledger_standardize.json`

## Runtime Semantics
- canonical profile: `customer_ledger`
- required fields: `customer_name`, `phone`, `amount`, `biz_date`
- unique key: `phone + biz_date + amount`

## Typical Inputs
- `xlsx`
- `csv`
- `jsonl`
- OCR/PDF tabular inputs that recover customer, amount, and date columns

## Quality Gates
- `max_required_missing_ratio = 0.0`
- `duplicate_key_ratio_max = 0.1`
- `numeric_parse_rate_min = 0.95`
- `date_parse_rate_min = 0.95`
- `allow_empty_output = false`

## Notes
- `customer_ledger` is a separate profile from `customer_contact`.
- If an input only contains contact fields without stable `amount + biz_date`, it should stay on `customer_contact`.
