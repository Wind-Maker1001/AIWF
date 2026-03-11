# WinUI 3 Handoff — Drawer / Canvas / Inspector Refinement

## Branch / Base

- Worktree: `D:\AIWF_master_preview`
- Branch: `codex/winui-node-flyout-layout-20260309`
- Base merged commit on `master`: `c5984c9`

## Goal of This Branch

This branch continued from the already-merged WinUI stabilization work and focused on:

- replacing the old “添加节点” centered popup / modal feel with a left drawer node library
- making the canvas page feel more like a real editor
- tightening Fluent / Win11 desktop interaction quality
- reducing prototype / web-backend feeling without rewriting the app skeleton

## What Changed in This Branch

### 1. Add Node Interaction

Old behavior:

- “添加节点” used unstable popup / flyout / modal-like patterns during experimentation
- visually felt disconnected from the editor workflow

Current behavior in that branch:

- “添加节点” toggles a left drawer inside the canvas editor area
- drawer contains:
  - title
  - short description
  - search box
  - grouped node list
  - close button
- clicking a node item still reuses the original node insertion logic

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.NodePalette.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Setup.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`

### 2. Canvas Page Structure

Canvas page in that branch followed:

- top page header
- tabs / top-level navigation
- grouped command bar
- main editor area with:
  - left node library drawer
  - center canvas
  - right inspector

This was done without replacing the overall page skeleton.

Key file:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`

### 3. Canvas Fit / Viewport Behavior

Canvas fit logic was refined so node groups better occupy the real usable center area:

- forces layout refresh before fit
- dynamically adjusts fit padding
- reuses existing `FitCanvasToNodes()` logic
- re-runs fit after drawer open / close animation completes

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Zoom.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.NodePalette.cs`

### 4. Splitter / Resize Handle

The divider between canvas and inspector remained fully draggable.
It was refined to behave more like a proper editor splitter:

- light visual by default
- stronger hover / dragging feedback
- larger hit area than visible line
- correct resize cursor
- z-order fix so it is not visually hidden by the inspector while dragging

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.SplitLayout.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`

### 5. Canvas Selection / Inspector Refinement

Inspector was adjusted to feel less like an empty form:

- clearer sectioning
- explicit empty-state messaging for the actions region
- delete buttons only appear when relevant

Selection visuals were softened so selected nodes do not look like an error state.

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Selection.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/CanvasSelectionPresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`

### 6. Connection / Canvas Polish

Small but high-value editor polish:

- softer line color
- rounded caps
- reduced harshness of active visuals
- lighter, lower-noise canvas empty state

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Connections.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Zoom.cs`

### 7. Visual Language Alignment

That branch continued the visual cleanup started earlier:

- less “hero banner / backend dashboard” feeling
- more restrained Win11-ish Fluent tool feel
- grouped command bar instead of flat button row
- reduced accidental emphasis and repeated section headers

Key files:

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/NavigationStylePresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/RunBadgePresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/StatusPresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/InputFieldPresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Artifacts.cs`

## Files Changed on That Branch

- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Setup.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.NodePalette.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Zoom.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Selection.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.SplitLayout.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Connections.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.Artifacts.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Shortcuts.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/CanvasSelectionPresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/InputFieldPresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/NavigationStylePresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/RunBadgePresenter.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/StatusPresenter.cs`

## Validation Performed

Verified successfully on that branch:

### Build

- `dotnet build D:\AIWF_master_preview\apps\dify-native-winui\AIWF.Native.WinUI.sln -c Debug -p:Platform=x64`

### Smoke

- `powershell -ExecutionPolicy Bypass -File D:\AIWF_master_preview\ops\scripts\check_native_winui_smoke.ps1 -Root D:\AIWF_master_preview -Configuration Debug`

### UIA Smoke

- `powershell -ExecutionPolicy Bypass -File D:\AIWF_master_preview\ops\scripts\check_native_winui_uia_smoke.ps1 -Root D:\AIWF_master_preview -Configuration Debug`

At handoff time, all three were passing.

## Current User-Visible State on That Branch

Stable:

- left drawer opens / closes
- drawer items still add nodes
- canvas nodes scale with viewport
- inspector still updates on selection
- fit / center behavior is improved
- splitter is draggable and visually lighter

## Known Remaining Imperfections

These were not blockers, but were still candidates for future polishing:

- canvas node cards could still be refined to feel more like a mature flow editor
- the workspace page still had some “form card” character
- results page could be visually unified one more pass
- canvas could still benefit from subtle editor texture / background nuance

## Recommended Next Steps

If continuing that branch:

1. visually inspect:
   - drawer width and rhythm
   - canvas centering after drawer toggle
   - splitter discoverability
   - node card density
2. if approved, create a focused PR from:
   - `codex/winui-node-flyout-layout-20260309`
3. keep scope narrow; do not reopen large structural refactors

## Notes for the Next Agent

- do not reintroduce modal add-node UI
- keep the left drawer pattern
- keep the split-pane / inspector architecture
- prioritize small product-quality improvements over visual experimentation
