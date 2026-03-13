# AIWF GUI 交接文档（2026-02-26）

## 1. 当前状态（已完成）
本次已在 `Electron + HTML` 现有架构内完成 GUI 外壳重构与分层整理，核心成果包括：
- 统一 Fluent 2 风格壳层与接近 Win11 的视觉语义，包含玻璃层、圆角、分层和动效
- 首页与 `Workflow Studio` 共享统一主题
- 部分表单控件已替换为 Fluent Web Components，如 `fluent-text-field` 与 `fluent-text-area`
- 默认隐藏非日常使用的 debug / 诊断区域（`dev-only`）
- 在自动化测试环境中自动开启 `dev-mode`，保证 UI 用例稳定

## 2. 关键改动文件
- `apps/dify-desktop/package.json`
- `apps/dify-desktop/renderer/index.html`
- `apps/dify-desktop/renderer/workflow.html`
- `apps/dify-desktop/renderer/fluent-shell.css`
- `apps/dify-desktop/renderer/fluent-init.js`
- `apps/dify-desktop/renderer/vendor/fluent-web-components.min.js`

## 3. 已验证结果
在 `apps/dify-desktop` 下验证通过：
- `npm run test:unit`
- `npm run test:workflow-ui`

## 4. 为什么“看起来仍非 Win11 原生”
当前依然是 Electron Web 壳层，而不是 WinUI / WPF 原生控件栈，因此：
- 视觉效果可以接近，但不等同于系统原生材质与合成链路
- 仍有部分原生 HTML 控件未完全替换为 Fluent 组件
- 页面信息密度仍高于典型 Win11 原生产品形态

## 5. Debug 区域策略
默认普通用户不可见（`dev-only`）。以下条件会显示：
- URL 带 `?devtools=1`
- `localStorage.aiwf_dev_mode = "1"`
- 自动化测试环境（Playwright / WebDriver）

## 6. 原生化迁移建议（下一阶段）
建议按“最小可运行原生壳”分阶段迁移：
1. 建立原生桌面宿主（WinUI 3）
2. 保留现有业务后端，Electron 主进程逻辑先不动，通过本地桥接协议通信
3. 优先迁移主工作流：首页三步走与 Workflow 画布入口
4. 对齐测试：补原生端 smoke 与合同测试，Web UI 测试保留做回归参考

## 7. 交接结论
当前分支可以作为“Web 壳完成版”继续维护；原生化应在新分支开展，避免影响现网可用桌面版。
