# Cleaning Flow Data Rules

`glue-python` local cleaning fallback now supports structured input loading and configurable cleaning rules.

## Input Priority

1. `params.rows` (list of row objects)
2. `params.csv_text` (CSV string with header)
3. CSV file path from one of:
   - `params.input_csv_path`
   - `params.source_csv_path`
   - `params.csv_path`
   - `params.input_uri` (local path or `file://...`)
4. Built-in sample rows (only if no input is provided)

`glue-python` now passes cleaned rows and rule config to `accel-rust` (`params` payload),
so accel/fallback output semantics are aligned for the same request.
For complex generic rules, flow defaults to local engine mode (`accel` is skipped).

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

You can put rules under either:
- top-level `params.<rule_key>`
- `params.rules.<rule_key>` (recommended declarative style)

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

Rule templates:
- `rules/templates/generic_minimal.json`
- `rules/templates/generic_finance_strict.json`
- `rules/templates/generic_customer_standardize.json`
- `rules/templates/preprocess_finance_basic.json` (raw-to-cooked CSV preprocessing template)
- `rules/templates/preprocess_debate_evidence.json` (raw-to-cooked debate evidence template)

One-click mixed-format evidence ingest:
- `ops/scripts/ingest_evidence_pack.ps1`
  - supported inputs: `pdf/docx/txt/png/jpg/jpeg/bmp/tif/tiff/xlsx/xlsm`
  - controls: `-OcrEnabled`, `-XlsxAllSheets`, `-MaxRetries`, `-OnFileError skip|raise`
  - OCR quality knobs (preprocess spec): `ocr_lang` (e.g. `chi_sim+eng`), `ocr_config` (e.g. `--oem 1 --psm 6`), `ocr_preprocess` (`adaptive|gray|none`)

Preprocess-specific generic keys:
- `xlsx_all_sheets` (boolean, default `false`)
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
