# Customer Contact Template v1

## Purpose
`customer_contact_v1` normalizes customer contact tables into a stable contact list.

Core fields:
- `customer_name`
- `phone`
- `city`

## Entry Points
- API payload: `params.cleaning_template = "customer_contact_v1"`
- Primary template: `rules/templates/customer_contact_v1.cleaning_spec_v2.json`
- Compatibility template: `rules/templates/generic_customer_standardize.json`

## Runtime Semantics
- canonical profile: `customer_contact`
- required fields: `customer_name`, `phone`
- unique key: `phone`

## Typical Inputs
- `xlsx`
- `csv`
- `jsonl`
- OCR/PDF tabular inputs that recover the same contact columns

## Quality Gates
- `max_required_missing_ratio = 0.0`
- `duplicate_key_ratio_max = 0.1`
- `allow_empty_output = false`

## Notes
- This template is contact-only and does not model amount or business date.
- For contact tables that also include amount and business date, use `customer_ledger_v1`.
