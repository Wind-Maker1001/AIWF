use super::clean_rows::resolve_job_root;
use super::office::write_office_documents_with_mode;
use super::outputs::{
    path_to_string, sha256_file, should_force_bad_parquet, write_bad_parquet_placeholder,
    write_cleaned_csv, write_profile_json,
};
use super::*;

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
