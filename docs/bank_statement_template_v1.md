# 银行流水模板 v1 使用说明

## 目的
`bank_statement_v1` 用于把银行流水 / 对账单类原始表格清洗成可核对、可去重、可计算的结构化数据。

重点字段：

- `account_no`
- `txn_date`
- `debit_amount`
- `credit_amount`
- `amount`
- `balance`
- `counterparty_name`
- `remark`
- `ref_no`
- `txn_type`

## 启用方式
- API payload: `params.cleaning_template = "bank_statement_v1"`
- 模板主入口：`rules/templates/bank_statement_v1.cleaning_spec_v2.json`
- 兼容入口：`rules/templates/generic_bank_statement_standardize.json`

## 运行语义
- canonical profile: `bank_statement`
- `amount` 表示规范化后的签名金额
- `debit_amount` / `credit_amount` 会同时保留
- 默认币种：`CNY`
- 去重键：`account_no + txn_date + ref_no + amount`

## 典型输入
推荐使用：

- `xlsx`
- `csv`
- `jsonl`

优先支持中英混合表头，例如：

- `账号` / `账户` / `账户号`
- `交易日期` / `记账日期`
- `借方金额` / `贷方金额`
- `余额`
- `对方户名`
- `摘要`
- `流水号`

## 内置质量门槛
- `max_required_missing_ratio = 0.0`
- `duplicate_key_ratio_max = 0.1`
- `numeric_parse_rate_min = 0.95`
- `date_parse_rate_min = 0.95`
- `allow_empty_output = false`

## 常见失败
- `account_no` 缺失：通常是表头未映射或输入列名异常
- `txn_date` 解析失败：常见于非标准日期文本或混合日期格式
- `numeric_parse_rate` 过低：通常是金额列含中文说明、单位或噪声字符过多
- `duplicate_key_ratio` 过高：通常是同一流水号重复导出或明细页重复拼接

## 说明
- 银行流水不是财报模板覆盖范围，和 [docs/finance_template_v1.md](finance_template_v1.md) 分开维护。
- 当前默认以表格型输入为主；图片/PDF 仅复用已有 sidecar 抽取能力，不额外提供银行流水专用 OCR 规则。
