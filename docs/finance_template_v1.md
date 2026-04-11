# 财报模板 v1 使用说明

## 目的
`finance_report_v1` 用于把财务报表类原始数据清洗成可计算、可复核的结构化数据，重点约束资产负债表/利润表/现金流相关金额字段。

边界说明：

- `finance_report_v1` 面向财报/报表类金额字段，不覆盖银行流水/对账单语义。
- 银行流水请改用 `bank_statement_v1`，见 [docs/bank_statement_template_v1.md](bank_statement_template_v1.md)。

## 启用方式
- GUI: 在“数据模板”选择 `财报模板 v1（资产/利润/现金流）`。
- API payload: `params.cleaning_template = "finance_report_v1"`。
- 模板主入口：`rules/templates/finance_report_v1.cleaning_spec_v2.json`。
- 兼容入口：`rules/templates/generic_finance_strict.json` 仍保留，但会先编译为 `cleaning_spec.v2` 再执行。
- 模板元数据：
  - `template_expected_profile = finance_statement`
  - `blank_output_expected = false`
  - 模板驱动运行默认 `profile_mismatch_action = block`

## 运行前预检（GUI）
- 点击 `模板预检`，会先做必填字段检查、金额可转换率检查、质量门槛预判。
- 预检通过后再点击 `开始生成`，可减少运行期失败。
- 若预检失败，可点击问题按钮直接定位到“缺失字段 / 异常金额样本 / 质量门槛错误”详情。
- 可配置门槛：
  - `预检金额可转换率下限`
  - `最大无效行比例`
  - `最小输出行数`
- 点击 `打开样本` 可直接打开对应源文件，并提示建议定位行号。

## 内置规则（当前版本）
来源：`rules/templates/finance_report_v1.cleaning_spec_v2.json`

兼容来源：`rules/templates/generic_finance_strict.json`

- 字段重命名：`Amt -> amount`，`ID -> id`
- 类型转换：`id:int`，`amount:float`，`currency:string`
- 必填：`id`、`amount`
- 默认值：`currency = CNY`
- 字符串清理：开启 trim
- 标准化：`currency` 转大写
- 过滤：`0 <= amount <= 100000000`
- 去重：按 `id` 去重，保留最后一条
- 排序：按 `id` 升序
- 质量门槛：
  - `max_invalid_rows = 0`
  - `max_invalid_ratio = 0.01`
  - `min_output_rows = 1`
  - `allow_empty_output = false`

## 输入建议
- 优先提供有表头的数据（xlsx/csv/txt 表格化文本）。
- 金额列尽量统一单位；若有“万元/亿元”，建议在入库前先换算。
- 保持主键字段稳定（如 `id` 或你自己的唯一键）。

## 常见失败与处理
- 报错“必填字段缺失”：确认是否存在 `id`/`amount` 或对应重命名来源字段。
- 报错“类型转换失败”：检查金额列是否包含非数值字符（如逗号、中文单位、空格）。
- 输出为空被拒绝：说明过滤条件过严或输入数据异常，先检查 `amount` 范围。
- 报错 `profile_mismatch_blocked`：当前输入更像证据/文本数据而不是财报数据；优先改用 `debate_evidence_v1` 或对应表格模板，而不是继续强跑 finance 模板。

## 扩展方式
- 如需按你自己的财报字段命名，优先新增 `cleaning_spec.v2` 模板 JSON。
- 若复制 legacy `generic_finance_strict.json`，运行时也会先编译成 `cleaning_spec.v2`。
- 如需更多规则（分组汇总、跨表一致性校验），可在 `offline_ingest.js` 的规则执行链继续扩展。

## 模板注册表（可插拔）
- 桌面端模板注册文件：`rules/templates/cleaning_templates_desktop.json`
- 新增模板时可直接在该文件追加条目（`id/file/label/description`），GUI 下拉会自动加载。
- 同时支持自动发现 `rules/templates/generic_*.json`（未在注册表声明时会按文件名生成模板项）。
- GUI `模板管理` 支持当前用户级模板启停，以及导入/导出模板 JSON。
