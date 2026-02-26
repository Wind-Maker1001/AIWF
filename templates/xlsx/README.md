# XLSX Templates

- `report_chart_template_zh.xlsx`: 中文高质量图表模板（示例模板）。

## 说明

- 运行时主流程会先输出 `chart_data` 数据表。
- 随后会尝试用本机 `python + openpyxl` 自动注入/更新 `dashboard` 图表。
- 你也可以在 GUI 里手动指定 `XLSX模板路径`，用于套用你自己的版式（不建议使用包含复杂图表对象的模板给 `ExcelJS` 直接读写）。
