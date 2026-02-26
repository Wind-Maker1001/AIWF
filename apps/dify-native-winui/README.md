# AIWF Native Desktop (WinUI 3) - Bootstrap

This folder is the starting point for migrating the desktop GUI from Electron web UI to a native WinUI 3 shell.

## Current scope in this commit

- Native migration scaffold and architecture notes
- IPC bridge contract draft for integrating existing AIWF runtime services
- Initial WinUI 3 project skeleton files (not compiled in this environment)

## Why not fully runnable yet

The current machine does not have a .NET SDK installed, so project creation/build cannot be executed yet.

Required toolchain:

- .NET SDK 8.0+
- Windows App SDK / WinUI 3 workload (Visual Studio 2022 or equivalent)

## Planned migration order

1. Build native shell window + navigation
2. Port daily workflow pages first (run settings, queue, run status, artifacts)
3. Keep advanced/dev panels behind developer mode
4. Reuse existing backend/runtime APIs via local bridge
5. Replace Electron packaging with MSIX/installer pipeline
