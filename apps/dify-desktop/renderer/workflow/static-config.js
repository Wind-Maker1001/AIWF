// Extracted from workflow/app.js to keep static workflow metadata separate from boot/runtime wiring.
// Copy anchors kept here for regression tests:
// 辩论证据流水线v1
// 财报清洗流水线v2
// group: "基础"
// group: "AI 参数"
// label: "导出熟肉包"
// label: "审核键"

export const TEMPLATE_STORAGE_KEY = "aiwf.workflow.templates.v1";

export const BUILTIN_TEMPLATES = [
  {
    id: "debate_evidence_v1",
    name: "辩论证据流水线v1",
    params_schema: {
      title: {
        type: "string",
        required: true,
        min_length: 1,
        group: "基础",
        description: "模板标题",
      },
    },
    graph: {
      workflow_id: "debate_evidence_v1",
      template_key: "debate_evidence",
      template_version: 1,
      name: "辩论证据流水线 {{title}}",
      nodes: [
        { id: "n1", type: "ingest_files", x: 40, y: 120, config: {} },
        { id: "n2", type: "clean_md", x: 320, y: 120, config: { export_canonical_bundle: true, canonical_title: "{{title}}" } },
        { id: "n3", type: "manual_review", x: 600, y: 120, config: { review_key: "debate_review", default_approve: false } },
        { id: "n4", type: "ai_refine", x: 880, y: 60, config: {} },
        { id: "n5", type: "md_output", x: 1160, y: 60, config: {} },
        { id: "n6", type: "md_output", x: 880, y: 200, config: {} },
      ],
      edges: [
        { from: "n1", to: "n2" },
        { from: "n2", to: "n3" },
        { from: "n3", to: "n4", when: { field: "approved", op: "eq", value: true } },
        { from: "n4", to: "n5" },
        { from: "n3", to: "n6", when: { field: "approved", op: "eq", value: false } },
      ],
    },
  },
  {
    id: "finance_report_v2",
    name: "财报清洗流水线v2",
    params_schema: {
      title: {
        type: "string",
        required: true,
        min_length: 1,
        group: "基础",
        description: "财报任务名称",
      },
      language: {
        type: "string",
        required: true,
        enum: ["zh-CN", "en-US"],
        default: "zh-CN",
        group: "AI 参数",
        description: "输出语言偏好",
      },
    },
    graph: {
      workflow_id: "finance_report_v2",
      template_key: "finance_report",
      template_version: 2,
      name: "财报清洗流水线 {{title}}",
      nodes: [
        { id: "n1", type: "ingest_files", x: 40, y: 100, config: {} },
        { id: "n2", type: "clean_md", x: 300, y: 100, config: { export_canonical_bundle: true, canonical_title: "{{title}}" } },
        { id: "n3", type: "finance_ratio_v1", x: 560, y: 100, config: { rows: [], input_map: { rows: "$prev.detail.rows" } } },
        { id: "n4", type: "anomaly_explain_v1", x: 820, y: 100, config: { rows: [], score_field: "anomaly_score", threshold: 0.8 } },
        { id: "n5", type: "md_output", x: 1080, y: 100, config: {} },
      ],
      edges: [
        { from: "n1", to: "n2" },
        { from: "n2", to: "n3" },
        { from: "n3", to: "n4" },
        { from: "n4", to: "n5" },
      ],
    },
  },
];

export const EDGE_HINTS_BY_NODE_TYPE = {
  manual_review: ["approved", "status", "review_key", "comment"],
  ai_audit: ["passed", "status", "reasons", "metrics_hash", "ai_hash"],
  ai_refine: ["ai_mode", "ai_text_chars", "detail"],
  office_slot_fill_v1: ["template_kind", "template_version", "slots", "binding_path", "validation_path", "warnings"],
  compute_rust: ["engine", "rust_started", "metrics.sections", "metrics.bullets", "metrics.chars", "metrics.sha256"],
  clean_md: ["job_id", "ai_corpus_path", "rust_v2_used", "warnings"],
  md_output: ["artifact_id", "kind", "path", "sha256"],
};

export const NODE_FORM_SCHEMAS = {
  clean_md: [
    { key: "export_canonical_bundle", label: "导出熟肉包", type: "bool" },
    { key: "canonical_title", label: "熟肉标题", type: "text" },
  ],
  manual_review: [
    { key: "review_key", label: "审核键", type: "text" },
    { key: "default_approve", label: "默认批准", type: "bool" },
    { key: "default_comment", label: "默认备注", type: "text" },
  ],
  ai_refine: [
    { key: "language", label: "语言", type: "text" },
  ],
  ai_audit: [
    { key: "numeric_lock", label: "数值锁定", type: "bool" },
    { key: "citation_required", label: "必须引用", type: "bool" },
  ],
  office_slot_fill_v1: [
    { key: "template_kind", label: "模板类型（docx/pptx/xlsx）", type: "text" },
    { key: "template_version", label: "模板版本", type: "text" },
    { key: "required_slots", label: "必填槽位(JSON数组)", type: "json" },
    { key: "slots", label: "槽位映射(JSON对象)", type: "json" },
  ],
  sql_chart_v1: [
    { key: "rows", label: "行数据(JSON数组)", type: "json" },
    { key: "chart_type", label: "图表类型", type: "text" },
    { key: "category_field", label: "分类字段", type: "text" },
    { key: "value_field", label: "数值字段", type: "text" },
    { key: "series_field", label: "系列字段", type: "text" },
    { key: "top_n", label: "TopN", type: "number" },
  ],
};
