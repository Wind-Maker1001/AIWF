# Cleaning Flow Data Rules

`glue-python` local cleaning runtime now uses `cleaning_spec.v2` as the single execution contract.
Legacy `params.rules`, `params.preprocess`, and legacy template JSON are still accepted, but they are compiled into `contracts/glue/cleaning_spec.v2.schema.json` before execution.

Current execution mainline:

- cleaning row transform: `transform_rows_v3`
- preprocess pipeline: `transform_rows_v3 -> postprocess_rows_v1 -> quality_check_v2 -> materialize`
- legacy `accel cleaning` remains compatibility-only for the old finance-oriented artifact path and is not the generic row-transform mainline
- ingest header mapping now supports `header_mapping_mode = strict|auto` and defaults to `strict`
- OCR/PDF inputs now emit `detected_structure = tabular|text|mixed|unknown`
- sidecar gold regression now includes real `image/pdf` OCR samples for both tabular and text paths

## Input Priority

1. `params.rows` (list of row objects)
2. `params.csv_text` (CSV string with header)
3. CSV file path from one of:
   - `params.input_csv_path`
   - `params.source_csv_path`
   - `params.csv_path`
   - `params.input_uri` (local path or `file://...`)
4. Built-in sample rows (only if no input is provided)

`glue-python` now compiles the effective cleaning config into `cleaning_spec.v2`, then passes normalized transform, postprocess, and quality intent to `accel-rust`.
This keeps Rust/Python output semantics aligned for the same request while still allowing Python to own input extraction and modality quality.

## Required Fields

Each row must provide:
- `id` (coercible to integer)
- `amount` (coercible to float, supports values like `"$1,200.50"` and `"1,200.50"`)

Rows with invalid `id` or `amount` are dropped.

## Configurable Cleaning Parameters

- `amount_round_digits` (default: `2`, range `0..6`)
  - Rounding mode: `HALF_UP` (financial-style)
- `drop_negative_amount` (default: `false`)
- `min_amount` (optional)
- `max_amount` (optional)
- `id_field` (default: `id`)
- `amount_field` (default: `amount`)
- `deduplicate_by_id` (default: `true`)
- `deduplicate_keep` (`first` or `last`, default: `last`)
- `sort_by_id` (default: `true`)
- `allow_empty_output` (default: `true`, if `false` then empty cleaned output raises an error)
- `local_parquet_strict` (default: `true`)
  - If `true`, local fallback parquet must pass parquet magic-byte validation, otherwise flow fails.
  - If `false`, local fallback may continue even if parquet is invalid (not recommended).

Quality gates (optional fail-fast thresholds):
- `max_invalid_rows`
- `max_filtered_rows`
- `min_output_rows`
- `max_invalid_ratio` (e.g. `0.1` means invalid rows must be <= 10%)

Office output theming:
- `office_theme`: `professional|academic|debate|assignment|debate_plus|business`
- `office_lang`: `zh|en` (default `zh`)
- `office_quality_mode`: `high|standard` (default `high`)
- `report_title`: override docx report title
- `cover_title`: override pptx cover title
- `office_max_rows`: max rows rendered into office files (default `5000`, for large dataset protection)
- theme definitions are decoupled at `rules/templates/office_themes.json` (override with env `AIWF_OFFICE_THEME_FILE`)

Environment fallback:
- `AIWF_GLUE_LOCAL_PARQUET_STRICT=true|false` (used when `params.local_parquet_strict` is not provided)

You can put legacy rules under either:
- top-level `params.<rule_key>`
- `params.rules.<rule_key>` (recommended declarative style)

Preferred modern entry:
- `params.cleaning_spec_v2`
- template file payloads with top-level `schema_version = "cleaning_spec.v2"`
- template registry metadata may additionally declare:
  - `template_expected_profile`
  - `blank_output_expected`

Template/profile guardrails:
- template-driven cleaning now resolves `params.cleaning_template` before execution
- `template_expected_profile` is compared with runtime profile recommendation before materialization
- default policy:
  - template-driven or `local_standalone`: `profile_mismatch_action = block`
  - general API calls: `profile_mismatch_action = warn`
- structured guardrail errors currently include:
  - `error_code`
  - `reason_codes`
  - `requested_profile`
  - `recommended_profile`
  - `profile_confidence`
  - `required_field_coverage`
  - `template_id`
  - `blank_output_expected`
  - `zero_output_unexpected`

Cleaning local execution mode:
- `AIWF_CLEANING_RUST_V2_MODE=off|shadow|default`
  - `off`: always return Python legacy result
  - `shadow`: return Python legacy result, but run Rust v2 compare in the background and emit `execution.shadow_compare`
  - `default`: prefer Rust v2 result, fallback to Python legacy only when Rust execution fails
- `AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT=true|false`
  - when `true`, `default` mode also emits a `shadow_compare` report instead of always skipping compare
- request-level override still has highest priority:
  - `params.rules.use_rust_v2 = true`
  - `params.rules.use_rust_v2 = false`
- release/readiness governance entry:
  - `ops/scripts/check_cleaning_rust_v2_rollout.ps1`
  - consumes `run_mode_audit.jsonl`, `execution.shadow_compare`, and `sidecar_python_rust_consistency_report.json`

Execution reporting:

- `quality_summary.engine_path.row_transform_engine`: row-level transform engine, currently `transform_rows_v3` or `python`
- `quality_summary.engine_path.postprocess_engine`: postprocess engine, currently `postprocess_rows_v1`, `python`, or `none`
- `quality_summary.engine_path.quality_gate_engine`: final quality gate engine, currently `quality_check_v2`, `transform_rows_v3+python_verify`, `python`, or `none`
- `quality_summary.engine_path.materialization_engine`: artifact writer engine, currently `python` or `legacy_accel_cleaning`
- `quality_summary.engine_path.legacy_cleaning_operator_used`: whether the legacy accel cleaning operator participated
- `execution_audit.stage_plan`: `preprocess_stage_plan.v1` stage-by-stage execution contract

Standard evidence and audit outputs:

- `quality_rule_set_id`: governance-owned quality gate selector merged into `quality_rules`
- `quality_summary.json`: normalized run-level quality/audit summary
- `rejections.jsonl`: sampled rejected rows from cast/required/filter/dedup paths
- `preprocess_stage_plan.v1`: fixed stage-plan schema embedded in preprocess execution audit
- `quality_summary.json` now also carries:
  - `requested_profile`
  - `recommended_profile`
  - `profile_confidence`
  - `profile_mismatch`
  - `required_field_coverage`
  - `blocking_reason_codes`
  - `blank_output_expected`
  - `zero_output_unexpected`

Published but palette-hidden operator:

- `postprocess_rows_v1` is a published Rust operator and desktop/runtime-exposable workflow node type
- it is marked `palette_hidden=true`, so it remains executable and contract-checked but does not appear in the default hand-authored palette

Generic rule keys (`params.rules`) for universal row cleaning:
- `platform_mode: "generic"`
- `rename_map: {"old":"new"}`
- `casts: {"field":"int|float|string|bool"}`
- `required_fields: ["field1", ...]`
- `default_values: {"field":"value"}`
- `include_fields` / `exclude_fields`
- `trim_strings` / `lowercase_fields` / `uppercase_fields`
- `filters: [{"field":"x","op":"gte","value":10}, ...]`
- `deduplicate_by: ["field1", ...]` + `deduplicate_keep`
- `sort_by: [{"field":"x","order":"asc|desc"}]`

Legacy rule templates:
- `rules/templates/generic_minimal.json`
- `rules/templates/generic_finance_strict.json`
- `rules/templates/generic_bank_statement_standardize.json`
- `rules/templates/generic_customer_standardize.json`
- `rules/templates/generic_customer_ledger_standardize.json`
- `rules/templates/preprocess_finance_basic.json` (raw-to-cooked CSV preprocessing template)
- `rules/templates/preprocess_debate_evidence.json` (raw-to-cooked debate evidence template)

Spec-first template example:
- `rules/templates/finance_report_v1.cleaning_spec_v2.json`
- `rules/templates/bank_statement_v1.cleaning_spec_v2.json`
- `rules/templates/customer_contact_v1.cleaning_spec_v2.json`
- `rules/templates/customer_ledger_v1.cleaning_spec_v2.json`

Bank statement template highlights:
- canonical profile: `bank_statement`
- normalized fields: `account_no`, `txn_date`, `debit_amount`, `credit_amount`, `amount`, `balance`, `counterparty_name`, `remark`, `ref_no`, `txn_type`
- `amount` is the normalized signed amount; `debit_amount` and `credit_amount` are preserved side by side

One-click mixed-format evidence ingest:
- `ops/scripts/ingest_evidence_pack.ps1`
  - supported inputs: `pdf/docx/txt/png/jpg/jpeg/bmp/tif/tiff/xlsx/xlsm`
  - controls: `-OcrEnabled`, `-XlsxAllSheets`, `-MaxRetries`, `-OnFileError skip|raise`
  - OCR quality knobs (preprocess spec): `ocr_lang` (e.g. `chi_sim+eng`), `ocr_config` (e.g. `--oem 1 --psm 6`), `ocr_preprocess` (`adaptive|gray|none`)

Preprocess-specific generic keys:
- `xlsx_all_sheets` (boolean, default `true`)
- `include_hidden_sheets` (boolean, default `false`)
- `ocr_lang` (string, optional; default uses env `AIWF_OCR_LANG` or `eng+chi_sim`)
- `ocr_config` (string, optional; default uses env `AIWF_OCR_CONFIG` or `--oem 1 --psm 6`)
- `ocr_preprocess` (string, optional; `adaptive|gray|none`, default env `AIWF_OCR_PREPROCESS` or `adaptive`)
- `ocr_try_modes` (env optional; default auto-fallback `adaptive,gray,none`)
- `deduplicate_by` (array of field names)
- `deduplicate_keep` (`first|last`, default `first`)
- `standardize_evidence` (boolean, output canonical evidence fields)
- `generate_quality_report` (boolean, writes `<output>.quality.json` by default)
- `quality_report_path` (optional custom report path)
- `chunk_mode` (`none|paragraph|sentence|fixed`)
- `chunk_field` (default `text` or `claim_text` in standardize mode)
- `chunk_max_chars` (used by `fixed` mode)
- `detect_conflicts` (boolean, marks `conflict_flag/conflict_topic/conflict_polarity`)
- `conflict_*` keys (topic/stance/text fields and positive/negative keyword lists)
- `canonical_profile` (`finance_statement|bank_statement|customer_contact|customer_ledger|debate_evidence`)
- `quality_rules` (top-level authority for shared quality gates)
- `image_rules` (image/OCR specific quality overrides)
- `xlsx_rules` (xlsx specific quality overrides)
- `sheet_profiles` (header alias/profile hints for workbook extraction)

Sidecar ingest contract:
- `contracts/glue/ingest_extract.schema.json`
- `header_mapping_mode: strict|auto`
  - `strict`: exact / substring / conservative fuzzy only
  - `auto`: stronger abbreviation matching, profile recommendation, and template recommendation for table inputs (`xlsx/csv/jsonl/image/pdf`)
  - OCR/PDF policy: tabular OCR/PDF uses `table_cells`/sheet structure for mapping; pure text OCR/PDF only recommends `debate_evidence`

Unified cleaning contract:
- `contracts/glue/cleaning_spec.v2.schema.json`

Rule validation script:
```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -ListTemplates
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_cleaning_rules.ps1 -RuleFile .\rules\templates\generic_finance_strict.json
powershell -ExecutionPolicy Bypass -File .\ops\scripts\validate_preprocess_spec.ps1 -SpecFile .\rules\templates\preprocess_debate_evidence.json
```

## Output Profile

The generated `profile.json` includes:
- `rows`, `cols`
- `sum_amount`, `min_amount`, `max_amount`, `avg_amount`
- `quality`:
  - `input_rows`
  - `output_rows`
  - `invalid_rows`
  - `filtered_rows`
  - `duplicate_rows_removed`
- `source` (input source trace)
