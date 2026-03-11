# XLSX Templates

## Files

- `report_chart_template_zh.xlsx`: sample Chinese report template with chart placeholders

## How It Is Used

- the desktop and backend Office pipelines can first write cleaned tabular data into workbook sheets
- when the local runtime has `python + openpyxl`, the workflow may further populate or refresh chart content
- the desktop UI also lets you provide a custom `XLSX` template path

## Guidance

- prefer templates with stable worksheet names and simple chart bindings
- avoid highly complex embedded objects if you expect direct writeback from `ExcelJS`
- test custom templates with a small sample run before using them in release packaging

## Related Docs

- [../../docs/dify_desktop_app.md](../../docs/dify_desktop_app.md)
- [../../docs/offline_delivery_minimal.md](../../docs/offline_delivery_minimal.md)
