# Frontend Convergence Decision

日期：2026-03-20

决策：

- `apps/dify-native-winui` 作为 AIWF 主前端。
- `apps/dify-desktop` 进入有边界的 Electron compatibility shell 角色，不再与 WinUI 作为双主前端并行演进。

## 为什么现在做这个决定

此前仓库处于“Electron 与 WinUI 默认双主线”状态，这会持续制造：

- 双倍前端维护成本
- 功能边界不清
- adapter / workflow / diagnostics 逻辑重复实现
- 收敛计划无法判定主次

这次决策的目标不是立即删除 Electron，而是终止“默认双主线”。

## 主次边界

### WinUI 主前端

WinUI 负责：

- 主用户启动入口
- 主工作区 / 运行配置 / 结果展示
- 主画布与长期桌面交互体验
- 后续主路径新功能承接

### Electron 次级兼容前端

Electron 负责：

- Workflow Studio 兼容入口
- 过渡期高级调试 / 诊断 / 历史能力
- 现有 Electron 打包与兼容交付

Electron 不再默认承担：

- 主桌面前端定位
- 净新增主路径用户功能
- 与 WinUI 等价的长期 roadmap

## 从现在开始的执行规则

1. 默认启动入口使用 WinUI。
2. 新的“主路径用户能力”优先落 WinUI。
3. 若要在 Electron 增加功能，必须满足以下至少一条：
   - 为迁移 WinUI 提供过渡能力
   - 属于调试/治理/兼容工具面
   - 明确标注 owner、remove_by 或 migration target
4. 不允许再把 Electron 与 WinUI 写成默认并列主前端。

## 本次落仓内容

- README 与 quickstart 改为 WinUI 主入口
- 新增 `ops/scripts/run_aiwf_frontend.ps1`，默认启动 WinUI
- Electron 启动脚本增加“次级兼容前端”警示
- 新增 frontend convergence gate，防止仓库回到默认双主线表述

## 后续仍需推进

- WinUI 打包 / 安装管线收口
- Electron 退役范围与时间表
- Workflow Studio 高级能力向 WinUI 迁移的阶段划分
