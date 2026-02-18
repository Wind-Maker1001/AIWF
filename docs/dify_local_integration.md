# Dify Local Integration

## Goal

Connect local Dify to AIWF with one HTTP call:
- create job
- run cleaning flow
- return artifacts/steps summary

Endpoint:
- `POST /api/v1/integrations/dify/run_cleaning`
- base URL default: `http://127.0.0.1:18080`

Health:
- `GET /api/v1/integrations/dify/health`

## 1. Start AIWF

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

## 2. Optional API Key

If you want Dify to call with auth, set in `ops/config/dev.env`:

```env
AIWF_API_KEY=YOUR_STRONG_KEY
```

Then restart `base-java`.
Dify request header must include: `X-API-Key: YOUR_STRONG_KEY`.

## 3. Test Bridge Endpoint

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_bridge_smoke.ps1
```

## 4. Configure Dify Workflow HTTP Node

Method: `POST`  
URL: `http://127.0.0.1:18080/api/v1/integrations/dify/run_cleaning`  
Headers:
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
    "input_csv_path": "D:\\AIWF\\examples\\input.csv"
  }
}
```

Theme options:
- `professional|academic|debate|assignment|debate_plus|business`

Response fields (use in downstream Dify nodes):
- `ok`
- `job_id`
- `run` (full cleaning run payload)
- `steps` (step list)
- `artifacts` (xlsx/docx/pptx/json paths)

## 5. Typical Dify Mapping

- LLM output to structured JSON (extract cleaning params)
- HTTP node sends to AIWF bridge
- Answer node summarizes:
  - `job_id`
  - cleaned row stats from `run.profile`
  - output files from `artifacts`
