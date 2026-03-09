# WinUI 交接说明

更新日期：2026-03-07  
对应分支：`feat/native-winui-bootstrap`

## 当前状态
- WinUI 主功能可用：工作区、结果区、画布区可正常切换。
- 画布核心交互可用：节点新增、拖拽、连线、框选、右键菜单、缩放、定位节点流、分栏拖拽。
- 后端调用链已接入新路径：`/jobs/{job_id}/run/{flow}`（不再依赖旧 `/run-cleaning`）。
- 运行结果面板可显示状态、指标、产物列表与原始 JSON。

## 本次已完成
目标：继续将 `MainWindow.xaml.cs` 解耦，并补上结果展示映射、基础自动化测试与启动性能基线。

### 新增模块
- 视图模型与目录
  - `ViewModels/MainViewModel.cs`
  - `Nodes/NodeCatalogService.cs`
- 运行时与展示映射
  - `Runtime/WorkflowRunnerAdapter.cs`
  - `Runtime/RunFlowCoordinator.cs`
  - `Runtime/RunPayloadBuilder.cs`
  - `Runtime/RunInputValidator.cs`
  - `Runtime/RunResultParser.cs`
  - `Runtime/RunResultPresentationMapper.cs`
  - `Runtime/RunVisualStateMapper.cs`
  - `Runtime/ResultPanelState.cs`
  - `Runtime/ResultPanelController.cs`
  - `Runtime/StatusPresenter.cs`
  - `Runtime/NavigationStylePresenter.cs`
  - `Runtime/RunBadgePresenter.cs`
  - `Runtime/InputFieldPresenter.cs`
  - `Runtime/CanvasSelectionPresenter.cs`
- 画布数学与控制器
  - `Canvas/CanvasViewportEngine.cs`
  - `Canvas/CanvasFitCalculator.cs`
  - `Canvas/CanvasInteractionMath.cs`
  - `Canvas/NodeDragMath.cs`
  - `Canvas/SplitLayoutController.cs`
- MainWindow 分拆（partial）
  - `MainWindow.Windowing.cs`
  - `MainWindow.Shortcuts.cs`
  - `MainWindow.Canvas.Viewport.cs`
  - `MainWindow.Canvas.Selection.cs`
  - `MainWindow.Canvas.ContextMenus.cs`
  - `MainWindow.Canvas.Persistence.cs`
  - `MainWindow.Canvas.NodeInteraction.cs`
  - `MainWindow.Canvas.Connections.cs`
  - `MainWindow.Canvas.PointerState.cs`
  - `MainWindow.Canvas.NodePalette.cs`
  - `MainWindow.Canvas.Artifacts.cs`
  - `MainWindow.Canvas.Nodes.cs`
  - `MainWindow.Results.Binding.cs`
- 诊断与性能基线
  - `Diagnostics/NativePerfRecorder.cs`
  - `Runtime/ArtifactPresentationMapper.cs`
  - `ops/scripts/capture_native_winui_perf.ps1`
- 自动化测试
  - `tests/WinUI3Bootstrap.Tests/CanvasInteractionMathTests.cs`
  - `tests/WinUI3Bootstrap.Tests/RunResultPresentationMapperTests.cs`
  - `tests/WinUI3Bootstrap.Perf/Program.cs`

### 变更结果
- `MainWindow.xaml.cs` 已显著瘦身，职责从“巨石文件”转为编排层。
- 交互逻辑按职责拆分，便于后续继续演进（触屏手势、节点类型扩展、后端协议变化）。
- 运行结果到画布/结果区的映射已独立，便于后续继续调整文案、状态色与产物布局。
- 启动路径已经打点，可产出本机性能基线报告。
- WinUI 数学/展示映射已有最小单测兜底。

## 构建与验证
- 命令：`dotnet build apps/dify-native-winui/src/WinUI3Bootstrap/WinUI3Bootstrap.csproj -c Debug`
- 结果：通过（0 errors, 0 warnings）。
- 命令：`dotnet test apps/dify-native-winui/tests/WinUI3Bootstrap.Tests/WinUI3Bootstrap.Tests.csproj -c Release`
- 结果：通过（22 passed）。
- 命令：`powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_smoke.ps1 -Configuration Release`
- 结果：通过；已校验窗口激活、构造时长、画布初始化与预热打点。
- 命令：`powershell -ExecutionPolicy Bypass -File .\ops\scripts\check_native_winui_uia_smoke.ps1 -Configuration Release`
- 结果：通过；已校验原生窗口附着、工作台输入编辑、画布命令执行（新建画布 + 快照生成）以及工作台/画布/结果往返导航。
- 命令：`powershell -ExecutionPolicy Bypass -File .\ops\scripts\capture_native_winui_perf.ps1 -Configuration Release`
- 结果：通过；已生成启动/纯逻辑基线报告，当前本机样本约为：
  - `First window activated`: `233.122 ms`
  - `MainWindow ctor`: `109.549 ms`
  - `Canvas workspace init`: `24.71 ms`
  - `Canvas prewarm`: `0.592 ms`

## 已知问题/风险
- 当前机器使用 .NET 预览 SDK，会出现 `NETSDK1057` 提示（非阻断）。
- 画布区域仍有性能优化空间（尤其真实 XAML 拖拽/框选/连线场景，当前 perf 仍以启动与纯逻辑基准为主）。
- Add Node 弹层样式与 Fluent 统一度还可继续打磨。
- `tests/` 目录下新项目需要确保 `bin/obj` 不入库。
- 当前性能基线只覆盖启动与纯逻辑微基准，不代表完整 XAML/合成器帧率。

## 下一步建议（按优先级）
1. 继续拆 `MainWindow.xaml.cs` 剩余编排逻辑，减少 UI 事件与状态操作耦合。
2. 为运行链路再补 2-3 个关键测试（运行结果绑定、产物布局、保存/恢复）。
3. 做一轮性能专项（首帧、拖拽帧率、分割条拖动卡顿点定位），必要时把基线纳入 CI/日报。
4. 再做一次 UI 视觉统一（圆角、阴影、材质、菜单风格一致性）。

## 关键文件入口
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.*.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/*.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Canvas/*.cs`

## 下周续做计划（建议顺序）
1. 先跑当前分支全流程回归
   - 后端：`/health`、`/jobs/{job_id}/run/{flow}`
   - WinUI：启动、运行、画布交互（拖拽/连线/框选/缩放/保存加载）
2. 做发布前稳定化
   - 定位并修复 500 错误根因（结合后端日志）
   - 补 2-3 个关键自动化测试（运行链路 + 画布核心交互）
3. 继续 WinUI 解耦收口
   - 拆分 `MainWindow.xaml.cs` 剩余大块（节点创建/结果绑定）
   - 将可复用状态继续迁移到 ViewModel/Runtime 层
4. 交付收尾
   - 同步 README 与交接文档“已完成能力/已知问题”
   - 视情况打版本 tag（例如 `v1.1.x`）

备注：当前仍在 `feat/native-winui-bootstrap` 分支推进，是否合并到 `master` 以远端实际状态为准。
