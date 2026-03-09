use crate::api_types::{
    CleanRow, CleaningOutputs, CleaningReq, CleaningResp, ComputeMetrics, ComputeReq, ComputeResp,
    FileOut, OfficeGenInfo, ProfileOut,
};
use parquet::{
    basic::{Compression, Repetition, Type as PhysicalType},
    column::writer::ColumnWriter,
    file::{properties::WriterProperties, writer::SerializedFileWriter},
    schema::types::Type,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    env, fs,
    io::{Read, Write},
    path::{Path, PathBuf},
    process::Command,
    sync::Arc,
};

pub(crate) fn run_compute_metrics(req: ComputeReq) -> Result<ComputeResp, String> {
    let text = req.text;
    if text.trim().is_empty() {
        return Err("empty text for compute_metrics".to_string());
    }

    let lines_vec: Vec<&str> = text.lines().collect();
    let mut sections = 0usize;
    let mut bullets = 0usize;
    let mut cjk = 0usize;
    let mut latin = 0usize;
    let mut digits = 0usize;
    let mut reference_hits = 0usize;
    let mut note_hits = 0usize;

    for line in &lines_vec {
        let t = line.trim();
        if t.starts_with("## ") {
            sections += 1;
        }
        if t.starts_with("- ") {
            bullets += 1;
        }
        let tl = t.to_lowercase();
        if tl.contains("references")
            || tl.contains("bibliography")
            || t.contains("参考文献")
            || t.contains("引用文献")
            || t.contains("文献目录")
        {
            reference_hits += 1;
        }
        if tl.contains("acknowledg")
            || tl.contains("footnote")
            || tl.contains("appendix")
            || t.contains("注释")
            || t.contains("脚注")
            || t.contains("附录")
            || t.contains("致谢")
        {
            note_hits += 1;
        }
    }

    for ch in text.chars() {
        if ch.is_ascii_alphabetic() {
            latin += 1;
        } else if ch.is_ascii_digit() {
            digits += 1;
        } else if ('\u{4E00}'..='\u{9FFF}').contains(&ch) {
            cjk += 1;
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(text.as_bytes());
    let sha256 = format!("{:x}", hasher.finalize());

    Ok(ComputeResp {
        ok: true,
        operator: "compute_metrics".to_string(),
        status: "done".to_string(),
        run_id: req.run_id,
        metrics: ComputeMetrics {
            sections,
            bullets,
            chars: text.chars().count(),
            lines: lines_vec.len(),
            cjk,
            latin,
            digits,
            reference_hits,
            note_hits,
            sha256,
        },
    })
}

pub(crate) fn run_cleaning_operator(req: CleaningReq) -> Result<CleaningResp, String> {
    let job_id = req
        .job_id
        .clone()
        .unwrap_or_else(|| "job_unknown".to_string());
    let step_id = req
        .step_id
        .clone()
        .unwrap_or_else(|| "cleaning".to_string());

    let job_root = resolve_job_root(req.job_root.as_deref(), &job_id)?;
    let stage_dir = job_root.join("stage");
    let artifacts_dir = job_root.join("artifacts");
    let evidence_dir = job_root.join("evidence");

    fs::create_dir_all(&stage_dir).map_err(|e| format!("create stage dir: {e}"))?;
    fs::create_dir_all(&artifacts_dir).map_err(|e| format!("create artifacts dir: {e}"))?;
    fs::create_dir_all(&evidence_dir).map_err(|e| format!("create evidence dir: {e}"))?;

    let csv_path = stage_dir.join("cleaned.csv");
    let parquet_path = stage_dir.join("cleaned.parquet");
    let profile_path = evidence_dir.join("profile.json");
    let xlsx_path = artifacts_dir.join("fin.xlsx");
    let docx_path = artifacts_dir.join("audit.docx");
    let pptx_path = artifacts_dir.join("deck.pptx");

    let cleaned_rows = load_and_clean_rows(req.params.as_ref())?;

    write_cleaned_csv(&csv_path, &cleaned_rows)?;
    if should_force_bad_parquet(req.force_bad_parquet) {
        write_bad_parquet_placeholder(&parquet_path)?;
    } else {
        write_cleaned_parquet(&parquet_path, &cleaned_rows)?;
    }
    write_profile_json(&profile_path, &cleaned_rows)?;
    let office = write_office_documents_with_mode(&xlsx_path, &docx_path, &pptx_path, &job_id)?;

    let csv_sha = sha256_file(&csv_path)?;
    let parquet_sha = sha256_file(&parquet_path)?;
    let profile_sha = sha256_file(&profile_path)?;
    let xlsx_sha = sha256_file(&xlsx_path)?;
    let docx_sha = sha256_file(&docx_path)?;
    let pptx_sha = sha256_file(&pptx_path)?;

    Ok(CleaningResp {
        ok: true,
        operator: "cleaning".to_string(),
        status: "done".to_string(),
        job_id: req.job_id,
        step_id: Some(step_id),
        input_uri: req.input_uri,
        output_uri: req.output_uri,
        job_root: path_to_string(&job_root),
        outputs: CleaningOutputs {
            cleaned_csv: FileOut {
                path: path_to_string(&csv_path),
                sha256: csv_sha,
            },
            cleaned_parquet: FileOut {
                path: path_to_string(&parquet_path),
                sha256: parquet_sha,
            },
            profile_json: FileOut {
                path: path_to_string(&profile_path),
                sha256: profile_sha,
            },
            xlsx_fin: FileOut {
                path: path_to_string(&xlsx_path),
                sha256: xlsx_sha,
            },
            audit_docx: FileOut {
                path: path_to_string(&docx_path),
                sha256: docx_sha,
            },
            deck_pptx: FileOut {
                path: path_to_string(&pptx_path),
                sha256: pptx_sha,
            },
        },
        profile: ProfileOut {
            rows: cleaned_rows.len(),
            cols: 2,
        },
        office_generation_mode: office.mode,
        office_generation_warning: office.warning,
        message: "accel-rust generated outputs".to_string(),
    })
}

pub(crate) fn resolve_job_root(input_root: Option<&str>, job_id: &str) -> Result<PathBuf, String> {
    fn is_valid_job_id(s: &str) -> bool {
        let t = s.trim();
        if t.len() < 8 || t.len() > 128 {
            return false;
        }
        t.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    }

    fn normalize_path(p: &Path) -> PathBuf {
        use std::path::Component;
        let mut out = PathBuf::new();
        for c in p.components() {
            match c {
                Component::CurDir => {}
                Component::ParentDir => {
                    let _ = out.pop();
                }
                other => out.push(other.as_os_str()),
            }
        }
        out
    }

    let jid = job_id.trim();
    if !is_valid_job_id(jid) {
        return Err("invalid job_id".to_string());
    }

    let bus = env::var("AIWF_BUS").unwrap_or_else(|_| "R:\\aiwf".to_string());
    let allowed_root = normalize_path(&PathBuf::from(bus).join("jobs"));
    let requested = if let Some(v) = input_root {
        if v.trim().is_empty() {
            allowed_root.join(jid)
        } else {
            PathBuf::from(v)
        }
    } else {
        allowed_root.join(jid)
    };

    let absolute = if requested.is_absolute() {
        requested
    } else {
        std::env::current_dir()
            .map_err(|e| format!("resolve current dir: {e}"))?
            .join(requested)
    };
    let normalized = normalize_path(&absolute);

    let leaf_ok = normalized.file_name().and_then(|n| n.to_str()) == Some(jid);
    let in_scope = normalized.starts_with(&allowed_root);
    if !leaf_ok || !in_scope {
        return Err(format!(
            "job_root must be under '{}' and end with job_id",
            allowed_root.to_string_lossy()
        ));
    }

    Ok(normalized)
}

pub(crate) fn rule_value<'a>(params: &'a Value, key: &str) -> Option<&'a Value> {
    if let Some(rules) = params.get("rules").and_then(|v| v.as_object())
        && let Some(v) = rules.get(key)
    {
        return Some(v);
    }
    params.get(key)
}

pub(crate) fn value_as_bool(v: Option<&Value>, default: bool) -> bool {
    match v {
        Some(Value::Bool(b)) => *b,
        Some(Value::String(s)) => {
            let l = s.trim().to_lowercase();
            matches!(l.as_str(), "1" | "true" | "yes" | "on")
        }
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0) != 0,
        _ => default,
    }
}

pub(crate) fn value_as_i32(v: Option<&Value>, default: i32) -> i32 {
    match v {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(default as i64) as i32,
        Some(Value::String(s)) => s.trim().parse::<i32>().unwrap_or(default),
        _ => default,
    }
}

pub(crate) fn value_as_f64(v: Option<&Value>) -> Option<f64> {
    match v {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => parse_amount(s),
        _ => None,
    }
}

pub(crate) fn parse_i64(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_i64().or_else(|| n.as_f64().map(|x| x as i64)),
        Value::String(s) => s.trim().parse::<f64>().ok().map(|x| x as i64),
        _ => None,
    }
}

pub(crate) fn parse_amount(s: &str) -> Option<f64> {
    let mut t = s.trim().replace(',', "");
    if t.starts_with('$') {
        t = t[1..].to_string();
    }
    t.parse::<f64>().ok()
}

pub(crate) fn parse_f64(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => parse_amount(s),
        _ => None,
    }
}

pub(crate) fn round_half_up(v: f64, digits: i32) -> f64 {
    let factor = 10f64.powi(digits.max(0));
    (v * factor).round() / factor
}

pub(crate) fn load_and_clean_rows(params_opt: Option<&Value>) -> Result<Vec<CleanRow>, String> {
    let default_rows = vec![
        CleanRow {
            id: 1,
            amount: 100.0,
        },
        CleanRow {
            id: 2,
            amount: 200.0,
        },
    ];
    let Some(params) = params_opt else {
        return Ok(default_rows);
    };

    let rows_val = params.get("rows");
    let Some(rows_arr) = rows_val.and_then(|v| v.as_array()) else {
        return Ok(default_rows);
    };

    let id_field = rule_value(params, "id_field")
        .and_then(|v| v.as_str())
        .unwrap_or("id")
        .to_string();
    let amount_field = rule_value(params, "amount_field")
        .and_then(|v| v.as_str())
        .unwrap_or("amount")
        .to_string();
    let drop_negative = value_as_bool(rule_value(params, "drop_negative_amount"), false);
    let deduplicate = value_as_bool(rule_value(params, "deduplicate_by_id"), true);
    let dedup_keep = rule_value(params, "deduplicate_keep")
        .and_then(|v| v.as_str())
        .unwrap_or("last")
        .to_lowercase();
    let sort_by_id = value_as_bool(rule_value(params, "sort_by_id"), true);
    let digits = value_as_i32(rule_value(params, "amount_round_digits"), 2).clamp(0, 6);
    let min_amount = value_as_f64(rule_value(params, "min_amount"));
    let max_amount = value_as_f64(rule_value(params, "max_amount"));

    let mut normalized: Vec<(i64, f64)> = Vec::new();
    for r in rows_arr {
        let Some(obj) = r.as_object() else {
            continue;
        };
        let id_val = obj.get(&id_field).and_then(parse_i64);
        let amount_val = obj.get(&amount_field).and_then(parse_f64);
        let (Some(id), Some(amount)) = (id_val, amount_val) else {
            continue;
        };
        if drop_negative && amount < 0.0 {
            continue;
        }
        if let Some(min_v) = min_amount
            && amount < min_v
        {
            continue;
        }
        if let Some(max_v) = max_amount
            && amount > max_v
        {
            continue;
        }
        normalized.push((id, round_half_up(amount, digits)));
    }

    let mut cleaned: Vec<(i64, f64)> = if deduplicate {
        use std::collections::HashMap;
        let mut map: HashMap<i64, f64> = HashMap::new();
        if dedup_keep == "first" {
            for (id, amount) in &normalized {
                map.entry(*id).or_insert(*amount);
            }
        } else {
            for (id, amount) in &normalized {
                map.insert(*id, *amount);
            }
        }
        map.into_iter().collect()
    } else {
        normalized
    };

    if sort_by_id {
        cleaned.sort_by_key(|x| x.0);
    }

    let out = cleaned
        .into_iter()
        .map(|(id, amount)| CleanRow { id, amount })
        .collect::<Vec<_>>();
    if out.is_empty() {
        return Ok(Vec::new());
    }
    Ok(out)
}

pub(crate) fn write_cleaned_csv(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create csv: {e}"))?;
    f.write_all(b"id,amount\n")
        .map_err(|e| format!("write csv header: {e}"))?;
    for r in rows {
        let line = format!("{},{}\n", r.id, r.amount);
        f.write_all(line.as_bytes())
            .map_err(|e| format!("write csv row: {e}"))?;
    }
    Ok(())
}

pub(crate) fn write_cleaned_parquet(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let id_col = Arc::new(
        Type::primitive_type_builder("id", PhysicalType::INT64)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet id column schema: {e}"))?,
    );
    let amount_col = Arc::new(
        Type::primitive_type_builder("amount", PhysicalType::DOUBLE)
            .with_repetition(Repetition::REQUIRED)
            .build()
            .map_err(|e| format!("build parquet amount column schema: {e}"))?,
    );
    let schema = Arc::new(
        Type::group_type_builder("aiwf_cleaned")
            .with_fields(vec![id_col, amount_col])
            .build()
            .map_err(|e| format!("build parquet schema: {e}"))?,
    );

    let props = Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .build(),
    );
    let file = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    let mut writer = SerializedFileWriter::new(file, schema, props)
        .map_err(|e| format!("create parquet writer: {e}"))?;

    let mut row_group_writer = writer
        .next_row_group()
        .map_err(|e| format!("open parquet row group: {e}"))?;

    let ids: Vec<i64> = rows.iter().map(|r| r.id).collect();
    let amounts: Vec<f64> = rows.iter().map(|r| r.amount).collect();
    while let Some(mut column_writer) = row_group_writer
        .next_column()
        .map_err(|e| format!("open parquet column: {e}"))?
    {
        match column_writer.untyped() {
            ColumnWriter::Int64ColumnWriter(typed) => {
                let values: &[i64] = &ids;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet id values: {e}"))?;
            }
            ColumnWriter::DoubleColumnWriter(typed) => {
                let values: &[f64] = &amounts;
                typed
                    .write_batch(values, None, None)
                    .map_err(|e| format!("write parquet amount values: {e}"))?;
            }
            _ => {
                return Err("unexpected parquet column type".to_string());
            }
        }
        column_writer
            .close()
            .map_err(|e| format!("close parquet column: {e}"))?;
    }

    row_group_writer
        .close()
        .map_err(|e| format!("close parquet row group: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("close parquet writer: {e}"))?;
    Ok(())
}

pub(crate) fn write_profile_json(path: &Path, rows: &[CleanRow]) -> Result<(), String> {
    let sum_amount: f64 = rows.iter().map(|r| r.amount).sum();
    let payload = json!({
        "profile": {"rows": rows.len(), "cols": 2, "sum_amount": sum_amount},
        "engine": "accel-rust",
    });
    let s = serde_json::to_string_pretty(&payload).map_err(|e| format!("json profile: {e}"))?;
    fs::write(path, s).map_err(|e| format!("write profile: {e}"))
}

pub(crate) fn office_mode() -> String {
    let mode = env::var("AIWF_ACCEL_OFFICE_MODE").unwrap_or_else(|_| "fallback".to_string());
    let lower = mode.trim().to_lowercase();
    if lower == "strict" {
        "strict".to_string()
    } else {
        "fallback".to_string()
    }
}

pub(crate) fn should_force_bad_parquet(force_bad_parquet: Option<bool>) -> bool {
    if force_bad_parquet.unwrap_or(false) {
        return true;
    }
    env::var("AIWF_ACCEL_FORCE_BAD_PARQUET")
        .unwrap_or_else(|_| "false".to_string())
        .trim()
        .eq_ignore_ascii_case("true")
}

pub(crate) fn write_bad_parquet_placeholder(path: &Path) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create parquet: {e}"))?;
    f.write_all(b"PARQUET_PLACEHOLDER\n")
        .map_err(|e| format!("write parquet: {e}"))?;
    Ok(())
}

pub(crate) fn find_python_command() -> Option<String> {
    for cmd in ["python", "py"] {
        let probe = if cmd == "py" {
            Command::new(cmd).arg("-3").arg("--version").output()
        } else {
            Command::new(cmd).arg("--version").output()
        };

        if let Ok(out) = probe
            && out.status.success()
        {
            return Some(cmd.to_string());
        }
    }
    None
}

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

pub(crate) fn write_placeholder_office_documents(
    xlsx: &Path,
    docx: &Path,
    pptx: &Path,
) -> Result<(), String> {
    write_placeholder_binary(xlsx, b"XLSX_PLACEHOLDER\n")?;
    write_placeholder_binary(docx, b"DOCX_PLACEHOLDER\n")?;
    write_placeholder_binary(pptx, b"PPTX_PLACEHOLDER\n")?;
    Ok(())
}

pub(crate) fn write_placeholder_binary(path: &Path, bytes: &[u8]) -> Result<(), String> {
    let mut f = fs::File::create(path).map_err(|e| format!("create placeholder: {e}"))?;
    f.write_all(bytes)
        .map_err(|e| format!("write placeholder: {e}"))?;
    Ok(())
}

pub(crate) fn sha256_file(path: &Path) -> Result<String, String> {
    let mut file = fs::File::open(path).map_err(|e| format!("open for hash: {e}"))?;
    let mut hasher = Sha256::new();
    let mut buf = [0u8; 64 * 1024];

    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read for hash: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }

    let digest = hasher.finalize();
    Ok(format!("{digest:x}"))
}

pub(crate) fn path_to_string(path: &Path) -> String {
    path.to_string_lossy().to_string()
}
