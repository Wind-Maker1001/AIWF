# AIWF GUI 交接文档（2026-02-26）

## 1. 当前状态（已完成）

本次已在 `Electron + HTML` 现有架构内完成 GUI 外壳重构与分层整理，核心结果：

- 统一 Fluent 2 风格壳层与 Win11 视觉语义（玻璃层、圆角、分层、动效）
- 首页与 Workflow Studio 共享统一主题
- 部分表单控件已改为 Fluent Web Components（`fluent-text-field` / `fluent-text-area`）
- 默认隐藏非日常使用的 debug/诊断区域（`dev-only`）
- 在自动化测试环境中自动开启 `dev-mode`，保证 UI 用例稳定

## 2. 关键改动文件

- `apps/dify-desktop/package.json`
- `apps/dify-desktop/renderer/index.html`
- `apps/dify-desktop/renderer/workflow.html`
- `apps/dify-desktop/renderer/fluent-shell.css`（新增）
- `apps/dify-desktop/renderer/fluent-init.js`（新增）
- `apps/dify-desktop/renderer/vendor/fluent-web-components.min.js`（新增）

## 3. 已验证结果

在 `apps/dify-desktop` 下验证通过：

- `npm run test:unit`：55/55 pass
- `npm run test:workflow-ui`：26 pass, 2 skipped, 0 failed

## 4. 为什么“看起来仍非 Win11 原生”

当前仍是 Electron Web 渲染层，不是 WinUI/WPF 原生控件栈：

- 视觉可接近，但无法等同系统原生材质合成链路
- 仍有部分原生 HTML 控件未完全替换为 Fluent 组件
- 页面信息密度高于 Win11 典型产品形态

## 5. Debug 区域策略

默认普通用户不可见（`dev-only`），以下条件会显示：

- URL 带 `?devtools=1`
- `localStorage.aiwf_dev_mode = "1"`
- 自动化测试环境（Playwright/WebDriver）

## 6. 原生化迁移建议（下一阶段）

建议新分支按“最小可运行原生壳”分阶段迁移：

1. 建立原生桌面宿主（WinUI 3）
2. 保留现有业务后端（Electron 主进程逻辑先不动），通过本地桥接协议通信
3. 先迁移主工作流（首页三步走 + Workflow 画布入口），后迁移高级面板
4. 对齐测试：补原生端 smoke + 合同测试，Web UI 测试保留做回归参考

## 7. 交接结论

当前分支可作为“Web 壳完成版”基线继续维护；原生化应在新分支开展，避免影响现网可用桌面版本。
