# Dify Adapter v1

Desktop runtime now uses a decoupled Dify adapter module:

- `apps/dify-desktop/dify_adapter.js`

Adapter responsibilities:

- Normalize request payload before posting to `/api/v1/integrations/dify/run_cleaning`
- Normalize response payload into stable fields (`ok`, `job_id`, `run_id`, `status`, `artifacts`, `quality`)
- Normalize upstream errors into stable codes:
  - `AUTH_FAILED`
  - `TIMEOUT`
  - `UPSTREAM_4XX`
  - `UPSTREAM_5XX`

This reduces coupling between GUI runtime and backend response shape.

