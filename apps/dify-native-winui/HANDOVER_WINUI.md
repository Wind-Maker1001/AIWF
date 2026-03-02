# WinUI 交接说明

更新日期：2026-03-02  
对应分支：`feat/native-winui-bootstrap`

## 当前状态
- WinUI 主功能可用：工作区、结果区、画布区可正常切换。
- 画布核心交互可用：节点新增、拖拽、连线、框选、右键菜单、缩放、定位节点流、分栏拖拽。
- 后端调用链已接入新路径：`/jobs/{job_id}/run/{flow}`（不再依赖旧 `/run-cleaning`）。
- 运行结果面板可显示状态、指标、产物列表与原始 JSON。

## 本次已完成（提交 `f567748`）
目标：将 `MainWindow.xaml.cs` 进行芯片化/解耦，降低单文件复杂度。

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

### 变更结果
- `MainWindow.xaml.cs` 已显著瘦身，职责从“巨石文件”转为编排层。
- 交互逻辑按职责拆分，便于后续继续演进（触屏手势、节点类型扩展、后端协议变化）。

## 构建与验证
- 命令：`dotnet build apps/dify-native-winui/src/WinUI3Bootstrap/WinUI3Bootstrap.csproj -c Debug`
- 结果：通过（0 errors, 0 warnings）。

## 已知问题/风险
- 当前机器使用 .NET 预览 SDK，会出现 `NETSDK1057` 提示（非阻断）。
- 画布区域仍有性能优化空间（尤其首次进入和高频拖拽时）。
- Add Node 弹层样式与 Fluent 统一度还可继续打磨。

## 下一步建议（按优先级）
1. 继续拆 `MainWindow.xaml.cs` 剩余“节点创建/产物节点更新/结果绑定 UI”逻辑。
2. 为画布交互与运行流程补最小自动化测试（至少数学函数与状态映射单测）。
3. 做一轮性能专项（首帧、拖拽帧率、分割条拖动卡顿点定位）。
4. 再做一次 UI 视觉统一（圆角、阴影、材质、菜单风格一致性）。

## 关键文件入口
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.Canvas.*.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Runtime/*.cs`
- `apps/dify-native-winui/src/WinUI3Bootstrap/Canvas/*.cs`
