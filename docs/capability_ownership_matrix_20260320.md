# AIWF Capability Ownership Matrix

日期：2026-03-20

## 原则

- 权威语义只能有一个 owner。
- adapter 可以转换，不可以重新定义语义。
- 同一份 operator / capability / workflow identity 不允许长期多端手工双写。

## Ownership Matrix

| 领域 | Owner | 拥有的真相 | 明确不拥有 |
| --- | --- | --- | --- |
| Rust operator 语义 | `apps/accel-rust` | operator id、domain、catalog、capabilities、operator-level contract、published metadata | desktop authoring UX、job lifecycle |
| Workflow authoring UX | `apps/dify-desktop` | canvas、node editor、import/save/run UX、本地 shell 适配 | Rust operator truth、全平台 capability truth |
| Job lifecycle / control plane | `apps/base-java` | job / step / artifact lifecycle、`job_context` transport、orchestration API | Rust operator registry truth、desktop local authoring state |
| Flow composition / preprocess runtime | `apps/glue-python` | flow composition、runtime catalog、preprocess pipeline、runtime composition | 全平台 operator registry truth、control-plane lifecycle truth |
| Adapter topology | `offline_local` / `base_api` | transport / execution adapter 细节 | workflow contract 语义真相 |

## Component-Level Rules

### Rust

- Rust 拥有 published operator metadata。
- Rust 拥有 workflow-step operator truth。
- desktop catalog 与 routing 只能消费或校验 Rust truth，不能长期重定义。

### Desktop

- desktop 拥有节点编辑、参数编辑、import/save/run 交互。
- desktop 可以有 UX alias，例如 `ds_refine`，但 alias 必须有明确 runtime backing，且不能伪造 Rust operator truth。
- desktop 的 `offline_local` 与 `base_api` 只是执行 adapter，不是 contract source。
- desktop 可以保留质量规则、sandbox mute / autofix、人工审核等人工调整界面，但这些界面只消费后端治理 contract，不拥有治理状态真相。

### base-java

- base-java 拥有控制平面编排与 `job_context` 传输边界。
- base-java 不拥有 workflow studio graph schema。
- base-java 不应重新定义 Rust operator catalog。

### glue-python

- glue-python 拥有 flow composition 与 preprocess/runtime composition。
- glue-python 从 2026-03-21 起开始拥有 quality rule set 的后端存储、查询与版本边界。
- glue-python 从 2026-03-21 起开始拥有 workflow sandbox rule 的后端存储与版本边界。
- glue-python 从 2026-03-22 起开始拥有 workflow sandbox autofix state / action history 的后端状态与查询边界。
- glue-python 从 2026-03-21 起开始拥有 workflow app registry 的后端存储与查询边界。
- glue-python 从 2026-03-21 起开始拥有 workflow version snapshot 与 compare diff 的后端存储与查询边界。
- glue-python 从 2026-03-21 起开始拥有 manual review queue/history 的后端状态与历史边界。
- glue-python 从 2026-03-21 起开始拥有 workflow run history / timeline / failure summary / audit log 的后端查询边界。
- glue-python 从 2026-03-21 起开始拥有 run baseline registry 的后端存储与查询边界。
- glue-python 可以维护 runtime catalog，但它不是跨平台 operator registry 的权威源。

## Adapter Rule

以下两条路径都不是语义源：

- `offline_local`
- `base_api`

统一规则：

- 二者都必须消费同一个 validated workflow input
- contract 分层必须清楚：
- top-level workflow contract
- chiplet envelope contract
- backend / operator payload contract

禁止事项：

- adapter 自己扩展 top-level workflow 语义
- adapter 长期持有与 contract 不一致的 fallback-only payload

## Current Enforcement Status

本次 Phase 1 已建立的约束：

- Rust published operator metadata 与 desktop catalog / routing 已进入 gate
- desktop workflow `version` 已进入 import / save / run 主路径
- `ds_refine` 不再只是 UI alias，已具备 runtime backing

Phase 2 目标：

- Rust operator manifest 成为 desktop 的单源输入或强校验源
- `offline_local` 与 `base_api` 共享 validated workflow envelope
