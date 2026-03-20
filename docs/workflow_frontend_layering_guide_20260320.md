# Workflow 前端分层指南（2026-03-20）

## 目标

本指南面向 `D:\AIWF\apps\dify-desktop\renderer\workflow`，用于约束后续改动继续沿用当前已经成型的结构，而不是回退到“大文件堆逻辑”的模式。

目标只有三个：

- 让入口文件保持薄
- 让纯逻辑可独立测试
- 让 UI / 绑定 / 渲染 / 装配边界稳定

## 当前推荐分层

### 1. `entry / assembly`

职责：

- 只负责组装模块
- 不承载大段业务逻辑
- 不写复杂数据转换

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\app.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-boot.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-startup.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-services.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-core-services.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-late-services.js`

规则：

- 入口层优先只做“传参”和“组合”
- 如果出现大量字段映射，就抽到 `*-support.js`
- 如果出现纯逻辑分支，就抽到 `support/helper`

### 2. `support / helper`

职责：

- 纯逻辑
- 参数归一化
- payload 构造
- builder / mapper
- 文本/状态/规则辅助

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-support.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-services-support.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\store-support.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\run-payload-support.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\quality-gate-support.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\template-utils-rules.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\template-utils-migration.js`

规则：

- support 层尽量不直接碰 DOM
- support 层尽量不依赖 `window`
- support 层优先补纯单测

### 3. `ui`

职责：

- 面向某一块业务能力的可调用 UI 行为
- 允许访问 `window.aiwfDesktop`
- 允许调用 render / setStatus
- 不要承载太多公用纯逻辑

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\flow-io-ui.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\sandbox-ui.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\quality-gate-ui.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\review-queue-ui.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\version-cache-ui.js`

规则：

- 如果 UI 文件里出现多个“可复用纯函数”，先下沉到 `*-support.js`
- UI 文件保留“流程编排感”，不要退化成工具箱

### 4. `bindings`

职责：

- 挂事件
- 调已有服务
- 不做复杂业务计算

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-toolbar-bindings.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-editor-bindings.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-canvas-bindings.js`

规则：

- 事件回调里的纯逻辑尽量拆到 `*-bindings-*.js`
- bindings 不负责“定义数据结构”，只负责“调度现有能力”

### 5. `renderers`

职责：

- 把数据渲染成 DOM
- 不做远程调用
- 不做复杂业务状态持久化

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\panels-ui-run-history-renderers.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\panels-ui-governance-sandbox-renderers.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\support-ui-run-compare-renderer.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-form-schema-renderer.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\app-form-run-renderer.js`

规则：

- renderer 尽量只吃入参和 `els`
- renderer 的输出行为要稳定，避免隐含副作用

### 6. `canvas / routing / store`

职责：

- 画布交互
- 路由与几何
- 图状态与图归一化

典型文件：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\canvas.js`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\canvas_class_methods*.mjs`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\canvas_interactions*.mjs`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\routing_core*.mjs`
- `D:\AIWF\apps\dify-desktop\renderer\workflow\store.js`

规则：

- 这层优先保持纯逻辑和稳定 API
- 与 DOM 的耦合尽量放到 wrapper / interaction 层，而不是核心算法层

## 文件命名约定

推荐延续这些后缀：

- `*-support.js`：纯辅助、构造、归一化
- `*-renderer.js`：渲染输出
- `*-bindings.js`：事件入口
- `*-bindings-*.js`：分组事件逻辑
- `*-services.js`：装配服务
- `*-services-support.js`：装配 builder
- `*-ui.js`：业务 UI 行为
- `*-state.js`：状态容器
- `*_*.mjs`：画布/路由相关较底层逻辑

## 什么时候该继续拆

出现以下任一情况就该拆：

- 文件超过约 `120~150` 行且包含多个独立职责
- 同时出现“DOM + 状态 + 远程调用 + 纯逻辑”
- 测试需要大量 mock 才能覆盖一个小函数
- 改一处逻辑必须看完整个文件才能确定影响

## 什么时候不要再拆

不要为了拆而拆。以下情况不建议继续拆：

- 文件已经很薄，只是字段多
- 继续拆只会增加跳转成本，没有明显职责收益
- 文本回归测试或外部引用要求文件字面保持稳定

典型例子：

- `D:\AIWF\apps\dify-desktop\renderer\workflow\static-config.js`

## 测试策略建议

当前推荐三层测试：

### 1. support 单测

针对：

- payload builder
- schema normalize
- graph migration
- route helper

目标：

- 快速
- 稳定
- 覆盖纯逻辑

### 2. module smoke

针对：

- `ui`
- `services`
- `bindings`

目标：

- 验证装配链没断
- 验证字段映射和回调 wiring 正常

### 3. GUI / Electron 验证

目前放在最后：

- 只有当 support + smoke 已稳定后才值得补
- 否则成本高、噪音大、打扰强

## 当前仍值得继续处理的热点

按优先级建议：

1. `D:\AIWF\apps\dify-desktop\renderer\workflow\canvas_edges.mjs`
2. `D:\AIWF\apps\dify-desktop\renderer\workflow\canvas_selection.mjs`
3. `D:\AIWF\apps\dify-desktop\renderer\workflow\support-ui-sandbox.js`
4. `D:\AIWF\apps\dify-desktop\renderer\workflow\static-config.js`（谨慎）

## 结论

从现在开始，workflow 前端应优先遵守：

- 薄入口
- support 下沉
- smoke 补强
- 谨慎继续细拆

这会比单纯继续“把文件拆小”更有长期收益。
