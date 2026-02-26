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

## 3.1 Baseline (Health + Replay)

Use the baseline script for a full local integration sanity check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_integration_baseline.ps1
```

Dry-run mode (validate script wiring without sending HTTP):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_integration_baseline.ps1 -DryRun
```

Replay payload example file:
- `ops/config/dify_run_cleaning.payload.example.json`

Single-step scripts (optional):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_health_check.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_replay_run_cleaning.ps1
```

Production check (retry + timeout + alert log):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_integration_production_check.ps1 -MaxRetries 3 -TimeoutSec 180
```

Fallback policy options (desktop/base_api mode):
- `smart` (default): fallback for timeout/network/server/not-ok
- `smart_strict`: fallback only for timeout/network/5xx
- `always`: always fallback when primary path not successful
- `never`: disable fallback

Run with automatic offline fallback (primary Dify bridge fails -> local desktop offline cleaning):

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_run_with_offline_fallback.ps1
```

Disable fallback in production check:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\dify_integration_production_check.ps1 -EnableOfflineFallback:$false
```

HTTP node template doc:
- `docs/dify_workflow_http_node_template.md`

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
