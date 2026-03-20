export const NODE_CONFIG_TEMPLATES_CORE_ANALYTICS = {
  lineage_v2: {
    rules: {},
    computed_fields_v3: [],
  },
  lineage_v3: {
    rules: {},
    computed_fields_v3: [],
    workflow_steps: [],
    rows: [],
  },
  rule_simulator_v1: {
    rows: [],
    rules: {},
    candidate_rules: {},
  },
  constraint_solver_v1: {
    rows: [],
    constraints: [],
  },
  chart_data_prep_v1: {
    rows: [],
    category_field: "category",
    value_field: "value",
    series_field: "series",
    top_n: 100,
  },
  diff_audit_v1: {
    left_rows: [],
    right_rows: [],
    keys: ["id"],
  },
  vector_index_v1_build: {
    rows: [],
    id_field: "id",
    text_field: "text",
  },
  vector_index_v1_search: {
    query: "",
    top_k: 5,
  },
  evidence_rank_v1: {
    rows: [],
    time_field: "time",
    source_field: "source_score",
    relevance_field: "relevance",
    consistency_field: "consistency",
  },
  fact_crosscheck_v1: {
    rows: [],
    claim_field: "claim",
    source_field: "source",
  },
  timeseries_forecast_v1: {
    rows: [],
    time_field: "time",
    value_field: "value",
    horizon: 3,
    method: "naive_drift",
  },
  finance_ratio_v1: {
    rows: [],
  },
  anomaly_explain_v1: {
    rows: [],
    score_field: "score",
    threshold: 0.8,
  },
  evidence_conflict_v1: {
    rows: [],
    claim_field: "claim",
    stance_field: "stance",
    source_field: "source",
  },
  template_bind_v1: {
    template_text: "",
    data: {},
  },
  provenance_sign_v1: {
    payload: {},
    prev_hash: "",
  },
  stream_state_v1_save: {
    stream_key: "default",
    state: {},
    offset: 0,
  },
  stream_state_v1_load: {
    stream_key: "default",
  },
};
