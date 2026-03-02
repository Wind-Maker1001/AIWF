# WinUI 交接说明

## 当前版本能力
- 视觉体系：红/灰/白/黑撞色，局部与整窗材质（Mica 优先，Acrylic 降级）。
- 结构：顶部导航与命令区、工作台/结果分区、结果信息三层（状态条+指标+明细）。
- 交互：触屏友好尺寸、按钮 hover/pressed/focus、页面分层淡入动画。
- 自适应：按窗口宽度自动切换布局（窄屏单列、中屏双列、宽屏三列）。
- 校验：`桥接地址`、`所有者`、`执行者`、`报告标题` 必填，逐项中文提示+红框错误态。

## 已知接口现状（本机环境）
- `GET /health` 可用（200，`{"ok":true}`）。
- 历史 `POST /run-cleaning` 已不可用（404）。
- OpenAPI 当前暴露：
  - `/health`
  - `/jobs/{job_id}/run/{flow}`
- 试跑 `POST /jobs/smoke/run/cleaning` 返回 500（服务端运行侧问题，非 WinUI 崩溃）。

## 冒烟验证结果
- WinUI Release 构建通过。
- 启动稳定性通过（10 秒存活检查通过）。
- UI 功能侧可正常打开、切换、输入校验、状态展示。

## 关键文件
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml`
- `apps/dify-native-winui/src/WinUI3Bootstrap/MainWindow.xaml.cs`

## 下一步建议
1. 与桥接服务确认可用 flow 名称及请求契约（当前 `cleaning` 返回 500）。
2. 对接后把 WinUI 请求路径从旧 `/run-cleaning` 迁移到 `/jobs/{job_id}/run/{flow}`。
3. 增加一条“真实后端联调冒烟脚本”（健康检查 + 指定 flow 运行 + 结果回填校验）。
