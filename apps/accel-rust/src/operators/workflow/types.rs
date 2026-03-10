use super::*;

#[derive(Deserialize)]
pub(crate) struct LineageV2Req {
    pub run_id: Option<String>,
    pub rules: Option<Value>,
    pub computed_fields_v3: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub(crate) struct LineageV3Req {
    pub run_id: Option<String>,
    pub rules: Option<Value>,
    pub computed_fields_v3: Option<Vec<Value>>,
    pub workflow_steps: Option<Vec<Value>>,
    pub rows: Option<Vec<Value>>,
}

#[derive(Deserialize)]
pub(crate) struct WorkflowRunReq {
    pub run_id: Option<String>,
    pub trace_id: Option<String>,
    pub traceparent: Option<String>,
    pub tenant_id: Option<String>,
    pub context: Option<Value>,
    pub steps: Vec<Value>,
}

#[derive(Serialize)]
pub(crate) struct WorkflowRunResp {
    pub ok: bool,
    pub operator: String,
    pub status: String,
    pub trace_id: String,
    pub run_id: Option<String>,
    pub context: Value,
    pub steps: Vec<WorkflowStepReplay>,
    pub failed_step: Option<String>,
    pub error: Option<String>,
}

#[derive(Serialize)]
pub(crate) struct WorkflowStepReplay {
    pub id: String,
    pub operator: String,
    pub status: String,
    pub started_at: String,
    pub finished_at: String,
    pub duration_ms: u128,
    pub input_summary: Value,
    pub output_summary: Option<Value>,
    pub error: Option<String>,
}
