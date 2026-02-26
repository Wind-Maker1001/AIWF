# Dify HTTP Node Template (AIWF)

Use this template in Dify HTTP node to call AIWF cleaning bridge.

## Request

- Method: `POST`
- URL: `http://127.0.0.1:18080/api/v1/integrations/dify/run_cleaning`
- Headers:
  - `Content-Type: application/json`
  - `X-API-Key: <AIWF_API_KEY>` (only when enabled)

Body template:

```json
{
  "owner": "dify",
  "actor": "dify",
  "ruleset_version": "v1",
  "params": {
    "office_lang": "zh",
    "office_theme": "debate_plus",
    "office_quality_mode": "high",
    "input_csv_path": "{{input_csv_path}}",
    "input_pdf_dir": "{{input_pdf_dir}}",
    "topic": "{{topic}}"
  }
}
```

## Recommended Mapping

- `input_csv_path`: from upstream file parser / dataset loader node
- `input_pdf_dir`: from uploaded files extraction directory
- `topic`: from user prompt or form input

## Response Fields

- `ok`
- `job_id`
- `run`
- `steps`
- `artifacts`

Typical downstream usage:
- if `ok=false` -> branch to retry/alert node
- if `ok=true` -> use `artifacts` in report assembly nodes
