# AIWF

AIWF is a local-first workflow platform for turning raw files and tabular inputs into cleaned datasets, structured outputs, and Office deliverables.

## Repository Layout

- `apps/base-java`: control plane and orchestration APIs
- `apps/glue-python`: flow runtime and preprocessing service
- `apps/accel-rust`: operator runtime and HTTP operator endpoints
- `apps/dify-desktop`: Electron desktop app with offline engine and Workflow Studio
- `apps/dify-native-winui`: native WinUI bootstrap shell
- `ops/scripts`: startup, packaging, CI, smoke, and release scripts
- `docs`: active guides, reference docs, and historical snapshots

## Start Here

- Documentation index: [docs/quickstart.md](docs/quickstart.md)
- Backend quickstart: [docs/quickstart_backend.md](docs/quickstart_backend.md)
- Desktop offline quickstart: [docs/quickstart_desktop_offline.md](docs/quickstart_desktop_offline.md)
- Verification guide: [docs/verification.md](docs/verification.md)
- Cleaning rules: [docs/cleaning_rules.md](docs/cleaning_rules.md)

## Common Commands

Backend bootstrap:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\db_migrate.ps1 -SqlPassword "<YOUR_SA_PASSWORD>"
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
```

Desktop run or package:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_desktop.ps1 -BuildWin -BuildInstaller
```

Local verification:

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1 -CiProfile Quick
powershell -ExecutionPolicy Bypass -File .\ops\scripts\ci_check.ps1
```

If your local machine does not have SQL connectivity ready yet, add `-SkipSqlConnectivityGate` for a reduced local check.

## Key HTTP Endpoints

- `base-java`: `GET /actuator/health`
- `base-java`: `GET /api/v1/backend/capabilities`
- `base-java`: `POST /api/v1/tools/create_job`
- `base-java`: `POST /api/v1/jobs/{jobId}/run/{flow}`
- `base-java`: `POST /api/v1/integrations/dify/run_cleaning`
- `glue-python`: `GET /health`
- `glue-python`: `GET /capabilities`
- `accel-rust`: `GET /health`
- `accel-rust`: `GET /capabilities`

## Documentation Scope

Active onboarding and operations docs:

- [docs/quickstart.md](docs/quickstart.md)
- [docs/quickstart_backend.md](docs/quickstart_backend.md)
- [docs/quickstart_desktop_offline.md](docs/quickstart_desktop_offline.md)
- [docs/verification.md](docs/verification.md)
- [docs/dify_desktop_app.md](docs/dify_desktop_app.md)
- [docs/dify_local_integration.md](docs/dify_local_integration.md)

Historical context docs still exist in the repository, but they are not the primary entrypoint:

- `docs/archive/`
- `docs/*handoff*.md`
- `docs/*snapshot*.md`
- `docs/release_notes_v1.*.md`

## Notes

- The canonical backend flow is `create_job -> run flow -> query steps/artifacts`.
- `job_context` is the canonical flow path contract transported from `base-java` to `glue-python`.
- Legacy path fields under `params` are not supported.
- GitHub-hosted `Quick CI` and self-hosted `Full Integration (Self-Hosted)` are separate workflows; see [docs/verification.md](docs/verification.md) for the exact commands they run.
