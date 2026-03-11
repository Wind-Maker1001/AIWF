# Native IPC Bridge Contract

This file tracks the currently implemented WinUI bridge behavior, not the older draft proposal.

## Current Runtime Topology

- WinUI uses a bridge URL that defaults to `http://127.0.0.1:18081`
- health and flow execution go through the glue-python side of the local stack
- when the bridge URL points at port `18081`, the WinUI coordinator derives the paired base API URL on port `18080` to create or verify jobs

## Proven Endpoints

Current implemented endpoints used by the WinUI runtime code:

1. `GET /health`
2. `POST /jobs/{job_id}/run/{flow}`

Related control-plane endpoint used before execution:

1. `POST http://127.0.0.1:18080/api/v1/jobs/create?owner=native`
2. `GET http://127.0.0.1:18080/api/v1/jobs/{jobId}`

## Current Request Shape

`POST /jobs/{job_id}/run/{flow}` sends the same JSON body shape used by `glue-python`:

```json
{
  "actor": "native",
  "ruleset_version": "v1",
  "params": {
    "office_theme": "assignment",
    "office_lang": "zh",
    "report_title": "Example",
    "input_csv_path": "D:\\data\\input.csv"
  }
}
```

## Current Response Expectations

- health responses are read as plain JSON and shown in the WinUI result panel
- run responses are parsed from the `/jobs/{job_id}/run/{flow}` body
- successful runs may update the effective `job_id` in the UI when the coordinator auto-created a job first

## Notes

- the older draft endpoints such as `/run-cleaning`, `/precheck-cleaning`, and `/preview-debate-style` are no longer the active contract
- if a future local IPC layer replaces HTTP, this file should be updated from code, not from the old draft
