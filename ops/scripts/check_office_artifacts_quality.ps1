param(
  [Parameter(Mandatory=$true)]
  [string]$XlsxPath,
  [Parameter(Mandatory=$true)]
  [string]$DocxPath,
  [Parameter(Mandatory=$true)]
  [string]$PptxPath,
  [int]$MinScore = 80
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Ok($m){ Write-Host "[ OK ] $m" -ForegroundColor Green }
function Warn($m){ Write-Host "[WARN] $m" -ForegroundColor Yellow }

foreach($p in @($XlsxPath, $DocxPath, $PptxPath)){
  if (-not (Test-Path $p)) { throw "artifact not found: $p" }
}

$pyCheck = @'
import json
import re
import sys
from openpyxl import load_workbook
from docx import Document
from pptx import Presentation

xlsx_path, docx_path, pptx_path = sys.argv[1], sys.argv[2], sys.argv[3]
zh_font_allow = {
    "Microsoft YaHei",
    "SimHei",
    "SimSun",
    "DengXian",
    "Noto Sans CJK SC",
    "Source Han Sans SC"
}

def has_mojibake(s: str) -> bool:
    if not s:
        return False
    return "\ufffd" in s

def iter_docx_text(doc):
    for p in doc.paragraphs:
        if p.text:
            yield p.text
    for t in doc.tables:
        for row in t.rows:
            for cell in row.cells:
                if cell.text:
                    yield cell.text

def iter_ppt_text(prs):
    for slide in prs.slides:
        for shape in slide.shapes:
            if getattr(shape, "has_text_frame", False) and shape.text_frame:
                for p in shape.text_frame.paragraphs:
                    if p.text:
                        yield p.text
            if getattr(shape, "has_table", False):
                for row in shape.table.rows:
                    for cell in row.cells:
                        if cell.text:
                            yield cell.text

issues = []
metrics = {}

wb = load_workbook(xlsx_path, read_only=True, data_only=True)
try:
    sheets = wb.sheetnames
    metrics["xlsx_sheets"] = len(sheets)
    if "summary" not in [s.lower() for s in sheets]:
        issues.append("xlsx missing summary sheet")
    ws = wb[sheets[0]]
    x_rows = max(0, int(ws.max_row or 0) - 1)
    metrics["xlsx_rows"] = x_rows
finally:
    wb.close()

doc = Document(docx_path)
doc_texts = list(iter_docx_text(doc))
metrics["docx_tables"] = len(doc.tables)
metrics["docx_images"] = len(doc.inline_shapes)
if len(doc.tables) < 2:
    issues.append("docx tables less than 2")
if len(doc.inline_shapes) < 1:
    issues.append("docx no image inserted")
doc_mojibake = sum(1 for t in doc_texts if has_mojibake(t))
metrics["docx_mojibake_lines"] = doc_mojibake
if doc_mojibake > 0:
    issues.append("docx contains mojibake text")

zh_runs = 0
zh_font_bad = 0
for p in doc.paragraphs:
    for r in p.runs:
        txt = r.text or ""
        if re.search(r"[\u4e00-\u9fff]", txt):
            zh_runs += 1
            fn = (r.font.name or "").strip()
            if fn and fn not in zh_font_allow:
                zh_font_bad += 1
for t in doc.tables:
    for row in t.rows:
        for cell in row.cells:
            for p in cell.paragraphs:
                for r in p.runs:
                    txt = r.text or ""
                    if re.search(r"[\u4e00-\u9fff]", txt):
                        zh_runs += 1
                        fn = (r.font.name or "").strip()
                        if fn and fn not in zh_font_allow:
                            zh_font_bad += 1
metrics["docx_zh_runs"] = zh_runs
metrics["docx_zh_font_bad"] = zh_font_bad
if zh_runs > 0 and zh_font_bad > 0:
    issues.append("docx has non-standard zh fonts")

prs = Presentation(pptx_path)
metrics["pptx_slides"] = len(prs.slides)
if len(prs.slides) < 4:
    issues.append("pptx slides less than 4")

slide_w = int(prs.slide_width)
slide_h = int(prs.slide_height)
ppt_images = 0
ppt_out_of_bounds = 0
ppt_mojibake = 0
for slide in prs.slides:
    for shape in slide.shapes:
        if getattr(shape, "shape_type", None) == 13:
            ppt_images += 1
            l = int(shape.left)
            t = int(shape.top)
            w = int(shape.width)
            h = int(shape.height)
            if l < 0 or t < 0 or (l + w) > slide_w or (t + h) > slide_h:
                ppt_out_of_bounds += 1
for txt in iter_ppt_text(prs):
    if has_mojibake(txt):
        ppt_mojibake += 1
metrics["pptx_images"] = ppt_images
metrics["pptx_image_out_of_bounds"] = ppt_out_of_bounds
metrics["pptx_mojibake_lines"] = ppt_mojibake
if ppt_images < 1:
    issues.append("pptx no image found")
if ppt_out_of_bounds > 0:
    issues.append("pptx image out of slide bounds")
if ppt_mojibake > 0:
    issues.append("pptx contains mojibake text")

score = 100
if "xlsx missing summary sheet" in issues:
    score -= 10
if metrics.get("xlsx_rows", 0) < 1:
    score -= 10
if "docx tables less than 2" in issues:
    score -= 15
if "docx no image inserted" in issues:
    score -= 15
if "docx contains mojibake text" in issues:
    score -= 40
if "docx has non-standard zh fonts" in issues:
    score -= 15
if "pptx slides less than 4" in issues:
    score -= 20
if "pptx no image found" in issues:
    score -= 10
if "pptx image out of slide bounds" in issues:
    score -= 30
if "pptx contains mojibake text" in issues:
    score -= 40
mixed_content_ok = (
    metrics.get("docx_tables", 0) >= 2
    and metrics.get("docx_images", 0) >= 1
    and metrics.get("pptx_images", 0) >= 1
)
if not mixed_content_ok:
    score -= 10
score = max(0, score)

out = {"ok": len(issues) == 0, "issues": issues, "metrics": metrics, "score": score}
print(json.dumps(out, ensure_ascii=False))
sys.exit(0 if out["ok"] else 2)
'@

$json = $pyCheck | python - $XlsxPath $DocxPath $PptxPath
$pyCode = $LASTEXITCODE
try {
  $report = $json | ConvertFrom-Json
} catch {
  Write-Host $json
  throw "office quality python check failed: invalid json output"
}

Write-Host ""
Write-Host "=== Office Quality Report ==="
Write-Host "xlsx_sheets            : $($report.metrics.xlsx_sheets)"
Write-Host "xlsx_rows              : $($report.metrics.xlsx_rows)"
Write-Host "docx_tables            : $($report.metrics.docx_tables)"
Write-Host "docx_images            : $($report.metrics.docx_images)"
Write-Host "docx_mojibake_lines    : $($report.metrics.docx_mojibake_lines)"
Write-Host "pptx_slides            : $($report.metrics.pptx_slides)"
Write-Host "pptx_images            : $($report.metrics.pptx_images)"
Write-Host "pptx_out_of_bounds     : $($report.metrics.pptx_image_out_of_bounds)"
Write-Host "pptx_mojibake_lines    : $($report.metrics.pptx_mojibake_lines)"
Write-Host "quality_score          : $($report.score)"

if (-not $report.ok) {
  foreach($i in $report.issues){ Warn $i }
  throw "office artifact quality gate failed"
}
if ([int]$report.score -lt $MinScore) {
  throw "office artifact quality score too low: $($report.score) < $MinScore"
}

Ok "office artifact quality gate passed"
