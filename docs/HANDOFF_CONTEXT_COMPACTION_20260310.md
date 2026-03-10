# Handoff for Context Compaction Pickup — 2026-03-10

## Purpose
- This file records the active repo state and the full WinUI workflow that happened before context compaction.
- Use this as the primary pickup note for the next agent/session.

## Repo / Worktree State
- Main worktree: `D:\AIWF`
- Main branch currently checked out: `codex/rust-module-review-20260309`
- Main HEAD at capture time: `490d0ac`
- Main worktree has many unrelated uncommitted changes in Rust / Java / Desktop modules. Do **not** mix those with WinUI follow-up work.

### Additional worktrees
- Stable merged WinUI preview worktree: `D:\AIWF_master_preview`
- Current WinUI follow-up branch in that worktree: `codex/winui-node-flyout-layout-20260309`
- `D:\AIWF_master_preview` HEAD at capture time: `c5984c9`
- `D:\AIWF_master_preview` currently has **uncommitted** WinUI-only changes in:
  - `D:\AIWF_master_preview\apps\dify-native-winui\src\WinUI3Bootstrap\MainWindow.Canvas.NodePalette.cs`
  - `D:\AIWF_master_preview\apps\dify-native-winui\src\WinUI3Bootstrap\MainWindow.Setup.cs`
  - `D:\AIWF_master_preview\apps\dify-native-winui\src\WinUI3Bootstrap\MainWindow.xaml.cs`

## Already Completed / Merged WinUI Work
- WinUI PR was created as PR `#5` and merged into `master`.
- Squash merge commit on `master`: `c5984c9`
- The merged work covered:
  - run-flow fixes
  - result binding fixes
  - canvas scaffold fixes
  - artifact node sync fixes
  - pointer interaction fixes
  - snapshot write/read off UI thread
  - x64 smoke / UIA smoke stabilization
  - result panel UIA assertions

### Important merged commits before follow-up
- `3aaa4ba` — initial WinUI review fixes
- `42dffe1` — partial splitting and test expansion
- `00b5d49` — snapshot writes off UI thread
- `d552529` — use real x64 builds in smoke automation
- `2b05367` — load canvas snapshots off the UI thread
- `b634530` — assert result panel fields in UIA smoke

## Current Open WinUI Problem
- The unresolved issue is the **“添加节点” / Add Node** popup layout.
- The original `Flyout`-based implementation shows severe layout defects:
  - left side text clipping
  - horizontal scrollbar showing
  - popup being clipped by the canvas area / viewport
- Multiple incremental fixes were attempted and were not satisfactory.

## What Was Tried On the Add Node Popup
### Attempts that did **not** reliably solve it
- hard-coded width tuning inside flyout content
- forcing inner stack/scroller/grid widths
- resetting scroll offset on open
- hiding horizontal scrollbar recursively
- anchoring popup to button instead of `CanvasViewport`
- rebuilding flyout content before each open
- converting old grid to single-column inside the flyout

### Why the previous strategy was abandoned
- The bug appears to be caused by the combination of:
  - `Flyout`
  - popup presenter measurement
  - scrolling container reuse
  - clipping relative to the canvas hosting region
- Continued pixel-level tuning was not converging.

## Latest Follow-up Direction (Uncommitted)
- In `D:\AIWF_master_preview`, the implementation was switched away from “fix the old flyout incrementally”.
- The latest direction is:
  - replace the unstable add-node `Flyout` flow with a more stable dialog-like approach
  - move toward rebuilding the picker content each open
  - reduce reliance on flyout presenter scroll state

### Current uncommitted diff summary in `D:\AIWF_master_preview`
- `MainWindow.Canvas.NodePalette.cs`: substantial changes around add-node opening logic and popup behavior
- `MainWindow.Setup.cs`: add-node UI construction logic changed significantly
- `MainWindow.xaml.cs`: one additional field added for dialog state

## Important Warning
- The `D:\AIWF_master_preview` WinUI follow-up branch is **not yet in a clean, validated state**.
- The user explicitly tested the popup repeatedly and reported:
  - “还是不行”
- Therefore:
  - do **not** commit the current `D:\AIWF_master_preview` WinUI changes blindly
  - first inspect the current branch diff and decide whether to:
    1. continue the dialog replacement cleanly, or
    2. reset the branch back to `c5984c9` and implement a fresh `ContentDialog` / separate panel solution

## Recommended Next Action
### Best next step
- In `D:\AIWF_master_preview`, stop trying to salvage the current flyout container.
- Implement a **clean `ContentDialog`-based Add Node picker** (or a dedicated overlay panel).
- Keep it simple:
  - centered dialog
  - single-column scrollable list
  - grouped headers
  - close button
  - each item creates a node and closes the dialog

### Suggested cleanup before continuing
- Inspect current diff:
  - `git -C D:\AIWF_master_preview diff -- apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.NodePalette.cs apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Setup.cs apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- If the current branch feels too messy, reset it:
  - `git -C D:\AIWF_master_preview restore apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.NodePalette.cs`
  - `git -C D:\AIWF_master_preview restore apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Setup.cs`
  - `git -C D:\AIWF_master_preview restore apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- Then implement the dialog cleanly from `master` baseline (`c5984c9`).

## Stashes to Keep in Mind
- `stash@{0}` — `On codex/winui-review-debug-20260309: wip winui async-read and uia-harness 2026-03-09`
- `stash@{1}` — safe-switch backup from rust branch
- `stash@{2}` — older winui backup before switching to rust branch

### Notes on stash usage
- `stash@{0}` contains exploration related to:
  - async snapshot read attempts
  - UIA harness experiments
- Only reuse it if you specifically need that historical exploration.
- It is **not** required for the current popup-layout fix.

## Validation Baseline To Preserve
- The merged WinUI baseline at `c5984c9` is known-good for:
  - `dotnet build`
  - `dotnet test`
  - `check_native_winui_smoke.ps1`
  - `check_native_winui_uia_smoke.ps1`
- Preserve those validations while working on the Add Node popup fix.

## Suggested Pickup Workflow
1. Open `D:\AIWF_master_preview`
2. Inspect current uncommitted WinUI diff
3. Decide whether to continue current dialog conversion or reset to `c5984c9`
4. Implement a stable Add Node dialog/panel
5. Run:
   - `dotnet build D:\AIWF_master_preview\apps\dify-native-winui\AIWF.Native.WinUI.sln -c Debug -p:Platform=x64`
   - `dotnet test D:\AIWF_master_preview\apps\dify-native-winui\AIWF.Native.WinUI.sln -c Debug --no-build`
6. Manually verify the Add Node UI visually
7. If stable, create a small dedicated PR from `codex/winui-node-flyout-layout-20260309`

## Final Summary
- WinUI main stabilization work is merged and done.
- The only open WinUI item in flight is the Add Node popup layout bug.
- Current `Flyout` approach is not trustworthy.
- Next session should continue in `D:\AIWF_master_preview` and replace that popup with a stable dialog/panel implementation instead of further flyout tuning.
