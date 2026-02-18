# Legacy: Dify Standalone Web Frontend

This document is kept for compatibility only.
Primary user path is now desktop app:
- `docs/dify_desktop_app.md`

## When to use this legacy path

Use only if you explicitly need browser-based local UI and understand the old startup chain.

## Legacy startup

```powershell
powershell -ExecutionPolicy Bypass -File .\ops\scripts\restart_services.ps1
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_console.ps1 -CreateVenv
powershell -ExecutionPolicy Bypass -File .\ops\scripts\run_dify_console.ps1
```

Default URL:
- `http://127.0.0.1:18083`
