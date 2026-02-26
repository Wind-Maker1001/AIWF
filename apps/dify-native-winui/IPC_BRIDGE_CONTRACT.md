# Native IPC Bridge Contract (Draft v0)

## Goals

- Keep existing AIWF runtime/business logic reusable
- Let WinUI UI call runtime with a stable local contract
- Make later replacement of Electron main process incremental

## Transport (phase 1)

- Local HTTP on loopback (`http://127.0.0.1:<port>`)
- JSON request/response
- Optional local named-pipe transport in phase 2

## Endpoints (minimum set)

1. `GET /health`
2. `POST /run-cleaning`
3. `POST /precheck-cleaning`
4. `POST /preview-debate-style`
5. `GET /config`
6. `POST /config`
7. `POST /open-path`
8. `GET /sample-pool`
9. `POST /sample-pool/clear`

## Request envelope

```json
{
  "trace_id": "string",
  "payload": {}
}
```

## Response envelope

```json
{
  "ok": true,
  "error": "",
  "data": {}
}
```

## Error codes (starter)

- `invalid_request`
- `runtime_unavailable`
- `offline_fallback_applied`
- `internal_error`
