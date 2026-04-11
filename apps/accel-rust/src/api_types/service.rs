use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Serialize)]
pub(crate) struct HealthResp {
    pub(crate) ok: bool,
    pub(crate) service: String,
}

#[derive(Deserialize)]
pub(crate) struct CleaningReq {
    pub(crate) job_id: Option<String>,
    pub(crate) step_id: Option<String>,
    pub(crate) input_uri: Option<String>,
    pub(crate) output_uri: Option<String>,
    pub(crate) job_root: Option<String>,
    pub(crate) force_bad_parquet: Option<bool>,
    pub(crate) params: Option<Value>,
}

#[derive(Deserialize)]
pub(crate) struct ComputeReq {
    pub(crate) run_id: Option<String>,
    pub(crate) text: String,
}

#[derive(Deserialize)]
pub(crate) struct TextPreprocessReq {
    pub(crate) run_id: Option<String>,
    pub(crate) text: String,
    pub(crate) title: Option<String>,
    pub(crate) remove_references: Option<bool>,
    pub(crate) remove_notes: Option<bool>,
    pub(crate) normalize_whitespace: Option<bool>,
}

#[derive(Deserialize)]
pub(crate) struct PostprocessRowsV1Req {
    pub(crate) run_id: Option<String>,
    pub(crate) trace_id: Option<String>,
    pub(crate) traceparent: Option<String>,
    pub(crate) rows: Vec<Value>,
    pub(crate) standardize_evidence: Option<bool>,
    pub(crate) evidence_schema: Option<Value>,
    pub(crate) chunk_mode: Option<String>,
    pub(crate) chunk_field: Option<String>,
    pub(crate) chunk_max_chars: Option<usize>,
    pub(crate) detect_conflicts: Option<bool>,
    pub(crate) conflict_topic_field: Option<String>,
    pub(crate) conflict_stance_field: Option<String>,
    pub(crate) conflict_text_field: Option<String>,
    pub(crate) conflict_positive_words: Option<Vec<String>>,
    pub(crate) conflict_negative_words: Option<Vec<String>>,
    pub(crate) schema_hint: Option<Value>,
}

#[derive(Serialize)]
pub(crate) struct ComputeMetrics {
    pub(crate) sections: usize,
    pub(crate) bullets: usize,
    pub(crate) chars: usize,
    pub(crate) lines: usize,
    pub(crate) cjk: usize,
    pub(crate) latin: usize,
    pub(crate) digits: usize,
    pub(crate) reference_hits: usize,
    pub(crate) note_hits: usize,
    pub(crate) sha256: String,
}

#[derive(Serialize)]
pub(crate) struct ComputeResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) metrics: ComputeMetrics,
}

#[derive(Serialize)]
pub(crate) struct TextPreprocessResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) run_id: Option<String>,
    pub(crate) markdown: String,
    pub(crate) removed_references_lines: usize,
    pub(crate) removed_notes_lines: usize,
    pub(crate) sha256: String,
}

#[derive(Serialize)]
pub(crate) struct FileOut {
    pub(crate) path: String,
    pub(crate) sha256: String,
}

#[derive(Serialize)]
pub(crate) struct ProfileOut {
    pub(crate) rows: usize,
    pub(crate) cols: usize,
}

#[derive(Clone, Debug)]
pub(crate) struct CleanRow {
    pub(crate) id: i64,
    pub(crate) amount: f64,
}

#[derive(Serialize)]
pub(crate) struct CleaningOutputs {
    pub(crate) cleaned_csv: FileOut,
    pub(crate) cleaned_parquet: FileOut,
    pub(crate) profile_json: FileOut,
    pub(crate) xlsx_fin: FileOut,
    pub(crate) audit_docx: FileOut,
    pub(crate) deck_pptx: FileOut,
}

#[derive(Serialize)]
pub(crate) struct CleaningResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) job_id: Option<String>,
    pub(crate) step_id: Option<String>,
    pub(crate) input_uri: Option<String>,
    pub(crate) output_uri: Option<String>,
    pub(crate) job_root: String,
    pub(crate) outputs: CleaningOutputs,
    pub(crate) profile: ProfileOut,
    pub(crate) office_generation_mode: String,
    pub(crate) office_generation_warning: Option<String>,
    pub(crate) message: String,
}

#[derive(Serialize)]
pub(crate) struct ErrResp {
    pub(crate) ok: bool,
    pub(crate) operator: String,
    pub(crate) status: String,
    pub(crate) error: String,
}

pub(crate) struct OfficeGenInfo {
    pub(crate) mode: String,
    pub(crate) warning: Option<String>,
}
