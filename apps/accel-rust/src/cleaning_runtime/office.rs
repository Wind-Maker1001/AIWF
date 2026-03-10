use super::outputs::{find_python_command, office_mode, write_placeholder_office_documents};
use super::*;

pub(crate) fn write_office_documents_with_mode(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
    job_id: &str,
) -> Result<OfficeGenInfo, String> {
    let force_placeholder = env::var("AIWF_ACCEL_OFFICE_FORCE_PLACEHOLDER")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true");

    if force_placeholder {
        write_placeholder_office_documents(xlsx, docx, pptx)?;
        return Ok(OfficeGenInfo {
            mode: "placeholder".to_string(),
            warning: Some(
                "forced placeholder mode by AIWF_ACCEL_OFFICE_FORCE_PLACEHOLDER=true".to_string(),
            ),
        });
    }

    match write_office_documents_python(xlsx, docx, pptx, job_id) {
        Ok(()) => Ok(OfficeGenInfo {
            mode: "python".to_string(),
            warning: None,
        }),
        Err(e) => {
            if office_mode() == "strict" {
                Err(e)
            } else {
                write_placeholder_office_documents(xlsx, docx, pptx)?;
                Ok(OfficeGenInfo {
                    mode: "placeholder".to_string(),
                    warning: Some(format!(
                        "python office generation failed, used placeholders: {e}"
                    )),
                })
            }
        }
    }
}

pub(crate) fn write_office_documents_python(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
    job_id: &str,
) -> Result<(), String> {
    let py = find_python_command()
        .ok_or_else(|| "python runtime not found for office generation".to_string())?;

    let script = r####"
import sys
from datetime import datetime
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter
from docx import Document
from pptx import Presentation
from pptx.util import Inches, Pt

xlsx_path, docx_path, pptx_path, job_id = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]

# XLSX
wb = Workbook()
ws = wb.active
ws.title = "detail"
header_fill = PatternFill(fill_type="solid", fgColor="1F4E78")
header_font = Font(color="FFFFFF", bold=True)
rows = [
    {"id": 1, "amount": 100.0},
    {"id": 2, "amount": 200.0},
]
cols = ["id", "amount"]
for i, c in enumerate(cols, start=1):
    cell = ws.cell(row=1, column=i, value=c)
    cell.fill = header_fill
    cell.font = header_font
for r_idx, r in enumerate(rows, start=2):
    ws.cell(row=r_idx, column=1, value=r["id"])
    ws.cell(row=r_idx, column=2, value=r["amount"])
    ws.cell(row=r_idx, column=2).number_format = "#,##0.00"
ws.freeze_panes = "A2"
ws.auto_filter.ref = "A1:B3"
ws.column_dimensions[get_column_letter(1)].width = 10
ws.column_dimensions[get_column_letter(2)].width = 14

sum_sheet = wb.create_sheet("summary")
sum_sheet["A1"] = "Metric"
sum_sheet["B1"] = "Value"
sum_sheet["A1"].fill = header_fill
sum_sheet["B1"].fill = header_fill
sum_sheet["A1"].font = header_font
sum_sheet["B1"].font = header_font
metrics = [
    ("rows", 2),
    ("cols", 2),
    ("sum_amount", 300.0),
    ("avg_amount", 150.0),
    ("generated_at", datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")),
]
for i, (k, v) in enumerate(metrics, start=2):
    sum_sheet.cell(row=i, column=1, value=k)
    sum_sheet.cell(row=i, column=2, value=v)
sum_sheet.column_dimensions["A"].width = 20
sum_sheet.column_dimensions["B"].width = 28
wb.save(xlsx_path)

# DOCX
doc = Document()
doc.add_heading("AIWF Data Cleaning Audit Report", level=1)
meta = doc.add_table(rows=4, cols=2)
meta.style = "Light List Accent 1"
meta.cell(0, 0).text = "Job ID"
meta.cell(0, 1).text = job_id
meta.cell(1, 0).text = "Step"
meta.cell(1, 1).text = "cleaning"
meta.cell(2, 0).text = "Generated At"
meta.cell(2, 1).text = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
meta.cell(3, 0).text = "Status"
meta.cell(3, 1).text = "DONE"
doc.add_paragraph("")
doc.add_heading("Core Metrics", level=2)
t = doc.add_table(rows=1, cols=2)
t.style = "Light Grid Accent 1"
t.rows[0].cells[0].text = "Metric"
t.rows[0].cells[1].text = "Value"
for k, v in metrics[:4]:
    row = t.add_row().cells
    row[0].text = str(k)
    row[1].text = str(v)
doc.save(docx_path)

# PPTX
prs = Presentation()
s1 = prs.slides.add_slide(prs.slide_layouts[0])
s1.shapes.title.text = "AIWF Cleaning Output Summary"
if len(s1.placeholders) > 1:
    s1.placeholders[1].text = f"Job {job_id}\nGenerated at {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}"

s2 = prs.slides.add_slide(prs.slide_layouts[5])
s2.shapes.title.text = "Key Metrics"
tb = s2.shapes.add_textbox(Inches(0.8), Inches(1.4), Inches(8.5), Inches(3.2))
tf = tb.text_frame
tf.clear()
for idx, text in enumerate(["Rows: 2", "Columns: 2", "Sum Amount: 300.0", "Avg Amount: 150.0"]):
    p = tf.paragraphs[0] if idx == 0 else tf.add_paragraph()
    p.text = text
    p.font.size = Pt(24 if idx < 2 else 20)

s3 = prs.slides.add_slide(prs.slide_layouts[1])
s3.shapes.title.text = "Data Quality"
s3.placeholders[1].text = "Input rows: 2\nOutput rows: 2\nInvalid rows: 0\nFiltered rows: 0"
prs.save(pptx_path)
"####;

    let py_file = std::env::temp_dir().join("aiwf_accel_office_gen.py");
    fs::write(&py_file, script).map_err(|e| format!("write temp python script: {e}"))?;

    let mut cmd = Command::new(&py);
    if py == "py" {
        cmd.arg("-3");
    }

    let out = cmd
        .arg(py_file.to_string_lossy().to_string())
        .arg(xlsx.to_string_lossy().to_string())
        .arg(docx.to_string_lossy().to_string())
        .arg(pptx.to_string_lossy().to_string())
        .arg(job_id)
        .output()
        .map_err(|e| format!("run python office generator: {e}"))?;

    let _ = fs::remove_file(&py_file);

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr).to_string();
        let stdout = String::from_utf8_lossy(&out.stdout).to_string();
        return Err(format!(
            "python office generation failed; stdout={stdout}; stderr={stderr}"
        ));
    }

    Ok(())
}
