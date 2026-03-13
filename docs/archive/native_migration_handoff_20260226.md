# AIWF 原生化迁移交接文档（2026-02-26）

## 1. 分支与目标
- 当前原生化分支：`feat/native-winui-bootstrap`
- 目标：从 `Electron + HTML` 迁移到 `WinUI 3` 原生 GUI，同时尽量复用现有运行时能力。

## 2. 已完成内容

### 2.1 代码与文档
- 新增原生化目录：`apps/dify-native-winui`
- 新增 WinUI 启动骨架：
  - `src/WinUI3Bootstrap/App.xaml`
  - `src/WinUI3Bootstrap/App.xaml.cs`
  - `src/WinUI3Bootstrap/MainWindow.xaml`
  - `src/WinUI3Bootstrap/MainWindow.xaml.cs`
- 新增桥接契约草案：
  - `apps/dify-native-winui/IPC_BRIDGE_CONTRACT.md`
- 新增原生化说明：
  - `apps/dify-native-winui/README.md`
- 新增运行脚本占位：
  - `ops/scripts/run_dify_native_winui.ps1`

### 2.2 Git 提交
- GUI 阶段成果（master）：
  - commit: `c43c7ed`
- native bootstrap（原生分支）：
  - commit: `5369bf8`

## 3. 当前阻塞
- 本机未安装 .NET SDK，`dotnet --version` 不可用，导致：
  - 无法创建 WinUI `.sln / .csproj`
  - 无法本地编译和启动原生壳

## 4. 架构状态说明
已完成第一阶段架构拆解：
- 原生 UI 与 Electron Web UI 已做边界隔离
- 通过桥接契约定义了 UI 与运行时的接口边界

尚未完成的下一阶段内容：
- 落地可运行的 `runtime bridge` 服务（HTTP / pipe）
- 将 WinUI 页面正式接到桥接层
- 将配置、任务、产物访问收敛为统一服务接口

## 5. 安装 .NET 后的执行顺序
1. 安装工具链（Visual Studio Installer）
   - `.NET desktop development`
   - WinUI 3 / Windows App SDK 相关组件
   - `.NET SDK 8.x`
2. 验证环境
   - `dotnet --info`
3. 在 `apps/dify-native-winui/src/WinUI3Bootstrap` 下创建项目
   - 生成 `.sln`、`.csproj`、`Package.appxmanifest`
4. 接入第一批页面
   - 首页（运行设置 / 队列 / 结果）优先迁移
   - Debug / 诊断页面后移
5. 接入桥接服务
   - 先跑 `GET /health` + `POST /run-cleaning` 两个核心接口

## 6. 阶段一验收标准
- WinUI 主窗口可启动
- 可提交一次清洗任务并看到成功 / 失败状态
- 可展示产物列表，至少包含路径与类型
- 不影响当前 `master` 上 Electron 桌面版可用性

## 7. 备注
- 当前仓库仍有用户本地未跟踪目录：`tmp_docx_check/`，未纳入提交
- 原生化推进建议继续在 `feat/native-winui-bootstrap` 上进行，稳定后再开 PR 合并
