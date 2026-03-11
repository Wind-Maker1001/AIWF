import { defaultNodeConfig, NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { validateGraph } from "./graph.js";
import { WorkflowCanvas } from "./canvas.js";
import { getWorkflowElements } from "./elements.js";
import { createWorkflowDiagnosticsUi } from "./diagnostics-ui.js";
import { createWorkflowPreflightUi } from "./preflight-ui.js";
import { migrateLoadedWorkflowGraph } from "./template-utils.js";
import { createWorkflowPanelsUi } from "./panels-ui.js";
import { createWorkflowTemplateUi } from "./template-ui.js";
import { createWorkflowConfigUi } from "./config-ui.js";
import { createWorkflowAppFormUi } from "./app-form-ui.js";
import { createWorkflowSupportUi } from "./support-ui.js";
import { createWorkflowQualityGateUi } from "./quality-gate-ui.js";

if (!window.aiwfDesktop) {
  try {
    if (window.parent && window.parent !== window && window.parent.aiwfDesktop) {
      window.aiwfDesktop = window.parent.aiwfDesktop;
    }
  } catch {}
}

const $ = (id) => document.getElementById(id);

const store = createWorkflowStore();
const QUALITY_GATE_PREFS_KEY = "aiwf.workflow.qualityGatePrefs.v1";

const els = getWorkflowElements($);
const { renderNodeRuns, renderDiagRuns, fetchRustRuntimeStats } = createWorkflowDiagnosticsUi(els);

let cfgViewMode = "form";
let selectedEdge = null;
let lastCompareResult = null;
const OFFLINE_LOCAL_NODE_TYPES = new Set([
  "ingest_files",
  "clean_md",
  "compute_rust",
  "transform_rows_v3",
  "join_rows_v2",
  "join_rows_v3",
  "join_rows_v4",
  "aggregate_rows_v2",
  "aggregate_rows_v3",
  "aggregate_rows_v4",
  "quality_check_v2",
  "quality_check_v3",
  "quality_check_v4",
  "window_rows_v1",
  "optimizer_v1",
  "load_rows_v2",
  "load_rows_v3",
  "schema_registry_v1_infer",
  "schema_registry_v1_get",
  "schema_registry_v1_register",
  "schema_registry_v2_check_compat",
  "schema_registry_v2_suggest_migration",
  "query_lang_v1",
  "columnar_eval_v1",
  "stream_window_v1",
  "stream_window_v2",
  "sketch_v1",
  "runtime_stats_v1",
  "capabilities_v1",
  "io_contract_v1",
  "failure_policy_v1",
  "incremental_plan_v1",
  "tenant_isolation_v1",
  "operator_policy_v1",
  "optimizer_adaptive_v2",
  "vector_index_v2_build",
  "vector_index_v2_search",
  "vector_index_v2_eval",
  "stream_reliability_v1",
  "lineage_provenance_v1",
  "contract_regression_v1",
  "perf_baseline_v1",
  "md_output",
  "manual_review",
  "sql_chart_v1",
  "office_slot_fill_v1",
]);

const ONLINE_REQUIRED_NODE_TYPES = new Set([
  "ai_refine",
  "ai_audit",
  "ai_strategy_v1",
  "ai_call",
]);

const IO_CONTRACT_COMPATIBLE_OPERATORS = new Set([
  "transform_rows_v2",
  "transform_rows_v3",
  "load_rows_v3",
  "finance_ratio_v1",
  "anomaly_explain_v1",
  "stream_window_v2",
  "plugin_operator_v1",
]);

let lastPreflightReport = null;
let lastAutoFixSummary = null;
let lastTemplateAcceptanceReport = null;


let singleSelectedNode = () => null;
let prettyJson = (value) => JSON.stringify(value || {}, null, 2);
let parseNodeConfigText = () => ({});
let parseEdgeWhenText = () => null;
let parseLooseJsonValue = (raw) => raw;
let setEdgeWhenBuilderVisibility = () => {};
let edgeWhenFromBuilder = () => null;
let applyEdgeWhenToBuilder = () => {};
let syncEdgeTextFromBuilder = () => {};
let rebuildEdgeHints = () => {};
let renderEdgeConfigEditor = () => {};
let formSchemaForNodeType = () => [];
let setCfgMode = () => {};
let toFieldDisplayValue = (_type, v) => String(v ?? "");
let parseFieldValue = (_type, text) => text;
let renderNodeConfigForm = () => {};
let parseNodeConfigForm = (_node, baseCfg = {}) => ({ ...(baseCfg || {}) });
let readMapTableRows = () => ({});
let renderMapTableRows = () => {};
let syncIoMapFromTables = () => {};
let renderIoMapEditor = () => {};
let renderNodeConfigEditor = () => {};

function applyRestoredWorkflowGraph(graph) {
  store.importGraph(graph || {});
  selectedEdge = null;
  renderAll();
}

const TEMPLATE_STORAGE_KEY = "aiwf.workflow.templates.v1";
const BUILTIN_TEMPLATES = [
  {
    id: "debate_evidence_v1",
    name: "辩论证据流水线 v1",
    params_schema: {
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "本次任务标题，用于流程名与输出标题。" },
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
    id: "debate_evidence_v2",
    name: "辩论证据流水线 v2",
    params_schema: {
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "本次任务标题，用于流程名与输出标题。" },
      language: {
        type: "string",
        required: true,
        enum: ["zh-CN", "en-US"],
        default: "zh-CN",
        group: "AI 参数",
        description: "AI 整理输出的语言偏好。",
      },
    },
    graph: {
      workflow_id: "debate_evidence_v2",
      template_key: "debate_evidence",
      template_version: 2,
      name: "辩论证据流水线 {{title}}",
      nodes: [
        { id: "n1", type: "ingest_files", x: 40, y: 120, config: {} },
        { id: "n2", type: "clean_md", x: 320, y: 120, config: { export_canonical_bundle: true, canonical_title: "{{title}}" } },
        { id: "n3", type: "manual_review", x: 600, y: 120, config: { review_key: "debate_review", default_approve: false } },
        { id: "n4", type: "ai_refine", x: 880, y: 60, config: { language: "{{language}}" } },
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
    id: "finance_report_v1",
    name: "财报清洗流水线 v1",
    params_schema: {
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "财报任务名称。" },
      threshold: { type: "number", required: true, min: 0, max: 1, default: 0.8, group: "风控阈值", description: "异常解释阈值(0~1)。" },
    },
    graph: {
      workflow_id: "finance_report_v1",
      template_key: "finance_report",
      template_version: 1,
      name: "财报清洗流水线 {{title}}",
      nodes: [
        { id: "n1", type: "ingest_files", x: 40, y: 100, config: {} },
        { id: "n2", type: "clean_md", x: 300, y: 100, config: { export_canonical_bundle: true, canonical_title: "{{title}}" } },
        { id: "n3", type: "finance_ratio_v1", x: 560, y: 100, config: { rows: [], input_map: { rows: "$prev.detail.rows" } } },
        { id: "n4", type: "anomaly_explain_v1", x: 820, y: 100, config: { rows: [], score_field: "anomaly_score", threshold: "{{threshold}}" } },
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
  {
    id: "finance_report_v2",
    name: "财报清洗流水线 v2",
    params_schema: {
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "财报任务名称。" },
      threshold: {
        type: "number",
        required: true,
        default: 0.8,
        group: "风控阈值",
        description: "阈值会随评分字段联动校验。",
        conditional: [
          { when: { field: "score_field", in: ["anomaly_score", "risk_score"] }, min: 0, max: 1, description: "anomaly/risk 模式下阈值范围是 0~1" },
          { when: { field: "score_field", equals: "quality_score" }, min: 0, max: 100, description: "quality 模式下阈值范围是 0~100" },
        ],
      },
      score_field: {
        type: "string",
        required: true,
        enum: ["anomaly_score", "risk_score", "quality_score"],
        default: "anomaly_score",
        group: "风控阈值",
        description: "异常评分字段名。",
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
        { id: "n4", type: "anomaly_explain_v1", x: 820, y: 100, config: { rows: [], score_field: "{{score_field}}", threshold: "{{threshold}}" } },
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

const EDGE_HINTS_BY_NODE_TYPE = {
  manual_review: ["approved", "status", "review_key", "comment"],
  ai_audit: ["passed", "status", "reasons", "metrics_hash", "ai_hash"],
  ai_refine: ["ai_mode", "ai_text_chars", "detail"],
  office_slot_fill_v1: ["template_kind", "template_version", "slots", "binding_path", "validation_path", "warnings"],
  compute_rust: ["engine", "rust_started", "metrics.sections", "metrics.bullets", "metrics.chars", "metrics.sha256"],
  clean_md: ["job_id", "ai_corpus_path", "rust_v2_used", "warnings"],
  md_output: ["artifact_id", "kind", "path", "sha256"],
};

const NODE_FORM_SCHEMAS = {
  clean_md: [
    { key: "export_canonical_bundle", label: "导出熟肉包", type: "bool" },
    { key: "canonical_title", label: "熟肉标题", type: "text" },
  ],
  manual_review: [
    { key: "review_key", label: "审核键", type: "text" },
    { key: "default_approve", label: "默认批准", type: "bool" },
    { key: "default_comment", label: "默认备注", type: "text" },
  ],
  sql_chart_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "chart_type", label: "图表类型", type: "text" },
    { key: "category_field", label: "类目字段", type: "text" },
    { key: "value_field", label: "值字段", type: "text" },
    { key: "series_field", label: "系列字段", type: "text" },
    { key: "top_n", label: "TopN", type: "number" },
  ],
  office_slot_fill_v1: [
    { key: "template_kind", label: "模板类型(docx/pptx/xlsx)", type: "text" },
    { key: "template_version", label: "模板版本", type: "text" },
    { key: "chart_source_node", label: "图表来源节点类型", type: "text" },
    { key: "required_slots", label: "必填插槽(JSON数组)", type: "json" },
    { key: "slots", label: "插槽(JSON对象)", type: "json" },
  ],
  ai_strategy_v1: [
    { key: "providers", label: "供应商列表(JSON数组)", type: "json" },
  ],
  load_rows_v3: [
    { key: "source_type", label: "数据类型", type: "text" },
    { key: "source", label: "数据路径", type: "text" },
    { key: "query", label: "查询语句", type: "text" },
    { key: "limit", label: "读取上限", type: "number" },
    { key: "max_retries", label: "最大重试次数", type: "number" },
    { key: "retry_backoff_ms", label: "重试间隔(ms)", type: "number" },
    { key: "resume_token", label: "续跑标记", type: "text" },
    { key: "connector_options", label: "连接器选项(JSON)", type: "json" },
  ],
  transform_rows_v3: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "rules", label: "规则(JSON对象)", type: "json" },
    { key: "computed_fields_v3", label: "表达式字段(JSON数组)", type: "json" },
    { key: "filter_expr_v3", label: "过滤表达式(JSON对象)", type: "json" },
  ],
  join_rows_v3: [
    { key: "left_rows", label: "左表行(JSON数组)", type: "json" },
    { key: "right_rows", label: "右表行(JSON数组)", type: "json" },
    { key: "left_on", label: "左键(逗号分隔)", type: "csv" },
    { key: "right_on", label: "右键(逗号分隔)", type: "csv" },
    { key: "join_type", label: "连接类型", type: "text" },
    { key: "strategy", label: "策略(auto/hash/sort_merge)", type: "text" },
    { key: "chunk_size", label: "分块大小", type: "number" },
    { key: "spill_path", label: "spill文件路径", type: "text" },
  ],
  aggregate_rows_v3: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "group_by", label: "分组字段(逗号分隔)", type: "csv" },
    { key: "aggregates", label: "聚合定义(JSON数组)", type: "json" },
    { key: "approx_sample_size", label: "近似采样数", type: "number" },
  ],
  quality_check_v3: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "rules", label: "检查规则(JSON对象)", type: "json" },
  ],
  schema_registry_v2_check_compat: [
    { key: "name", label: "Schema 名称", type: "text" },
    { key: "from_version", label: "源版本", type: "text" },
    { key: "to_version", label: "目标版本", type: "text" },
    { key: "mode", label: "兼容模式", type: "text" },
  ],
  schema_registry_v2_suggest_migration: [
    { key: "name", label: "Schema 名称", type: "text" },
    { key: "from_version", label: "源版本", type: "text" },
    { key: "to_version", label: "目标版本", type: "text" },
  ],
  udf_wasm_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "field", label: "输入字段", type: "text" },
    { key: "output_field", label: "输出字段", type: "text" },
    { key: "op", label: "运算(identity/double/negate/trim/upper)", type: "text" },
    { key: "wasm_base64", label: "WASM(Base64,可选)", type: "text" },
  ],
  time_series_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "time_field", label: "时间字段", type: "text" },
    { key: "value_field", label: "数值字段", type: "text" },
    { key: "group_by", label: "分组字段(逗号分隔)", type: "csv" },
    { key: "window", label: "窗口大小", type: "number" },
  ],
  stats_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "x_field", label: "X字段", type: "text" },
    { key: "y_field", label: "Y字段", type: "text" },
  ],
  entity_linking_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "field", label: "实体字段", type: "text" },
    { key: "id_field", label: "ID字段", type: "text" },
  ],
  table_reconstruct_v1: [
    { key: "lines", label: "行文本(JSON数组)", type: "json" },
    { key: "text", label: "原始文本", type: "text" },
    { key: "delimiter", label: "分隔符正则", type: "text" },
  ],
  feature_store_v1_upsert: [
    { key: "key_field", label: "主键字段", type: "text" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
  ],
  feature_store_v1_get: [
    { key: "key", label: "查询Key", type: "text" },
  ],
  lineage_v2: [
    { key: "rules", label: "规则(JSON对象)", type: "json" },
    { key: "computed_fields_v3", label: "表达式字段(JSON数组)", type: "json" },
  ],
  rule_simulator_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "rules", label: "基线规则(JSON对象)", type: "json" },
    { key: "candidate_rules", label: "候选规则(JSON对象)", type: "json" },
  ],
  constraint_solver_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "constraints", label: "约束列表(JSON数组)", type: "json" },
  ],
  chart_data_prep_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "category_field", label: "分类字段", type: "text" },
    { key: "value_field", label: "值字段", type: "text" },
    { key: "series_field", label: "序列字段", type: "text" },
    { key: "top_n", label: "TopN", type: "number" },
  ],
  diff_audit_v1: [
    { key: "left_rows", label: "左侧数据(JSON数组)", type: "json" },
    { key: "right_rows", label: "右侧数据(JSON数组)", type: "json" },
    { key: "keys", label: "主键字段(逗号分隔)", type: "csv" },
  ],
  vector_index_v1_build: [
    { key: "rows", label: "文档行(JSON数组)", type: "json" },
    { key: "id_field", label: "ID字段", type: "text" },
    { key: "text_field", label: "文本字段", type: "text" },
  ],
  vector_index_v1_search: [
    { key: "query", label: "查询文本", type: "text" },
    { key: "top_k", label: "返回条数", type: "number" },
  ],
  evidence_rank_v1: [
    { key: "rows", label: "证据行(JSON数组)", type: "json" },
    { key: "time_field", label: "时间字段", type: "text" },
    { key: "source_field", label: "来源分数字段", type: "text" },
    { key: "relevance_field", label: "相关度字段", type: "text" },
    { key: "consistency_field", label: "一致性字段", type: "text" },
  ],
  fact_crosscheck_v1: [
    { key: "rows", label: "事实行(JSON数组)", type: "json" },
    { key: "claim_field", label: "事实字段", type: "text" },
    { key: "source_field", label: "来源字段", type: "text" },
  ],
  columnar_eval_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "select_fields", label: "选择字段(逗号分隔)", type: "csv" },
    { key: "filter_eq", label: "等值过滤(JSON对象)", type: "json" },
    { key: "limit", label: "输出上限", type: "number" }
  ],
  stream_window_v1: [
    { key: "stream_key", label: "流Key", type: "text" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "event_time_field", label: "事件时间字段", type: "text" },
    { key: "window_ms", label: "窗口(ms)", type: "number" },
    { key: "watermark_ms", label: "水位线(ms)", type: "number" },
    { key: "group_by", label: "分组字段(逗号分隔)", type: "csv" },
    { key: "value_field", label: "数值字段", type: "text" },
    { key: "trigger", label: "触发模式", type: "text" }
  ],
  stream_window_v2: [
    { key: "stream_key", label: "流Key", type: "text" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "event_time_field", label: "事件时间字段", type: "text" },
    { key: "window_type", label: "窗口类型(tumbling/sliding/session)", type: "text" },
    { key: "window_ms", label: "窗口(ms)", type: "number" },
    { key: "slide_ms", label: "滑动步长(ms)", type: "number" },
    { key: "session_gap_ms", label: "会话间隔(ms)", type: "number" },
    { key: "watermark_ms", label: "水位线(ms)", type: "number" },
    { key: "allowed_lateness_ms", label: "允许迟到(ms)", type: "number" },
    { key: "group_by", label: "分组字段(逗号分隔)", type: "csv" },
    { key: "value_field", label: "数值字段", type: "text" },
    { key: "trigger", label: "触发模式", type: "text" },
    { key: "emit_late_side", label: "输出迟到侧流(true/false)", type: "text" }
  ],
  sketch_v1: [
    { key: "op", label: "操作(create/update/merge)", type: "text" },
    { key: "kind", label: "类型(hll/tdigest/topk)", type: "text" },
    { key: "state", label: "状态(JSON对象)", type: "json" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "field", label: "字段名", type: "text" },
    { key: "topk_n", label: "TopK N", type: "number" },
    { key: "merge_state", label: "合并状态(JSON对象)", type: "json" }
  ],
  runtime_stats_v1: [
    { key: "op", label: "操作(record/summary/reset)", type: "text" },
    { key: "operator", label: "目标算子", type: "text" },
    { key: "ok", label: "是否成功(true/false)", type: "text" },
    { key: "error_code", label: "错误码", type: "text" },
    { key: "duration_ms", label: "耗时(ms)", type: "number" },
    { key: "rows_in", label: "输入行数", type: "number" },
    { key: "rows_out", label: "输出行数", type: "number" }
  ],
  capabilities_v1: [
    { key: "include_ops", label: "指定算子(逗号分隔，可空)", type: "csv" },
  ],
  io_contract_v1: [
    { key: "operator", label: "目标算子", type: "text" },
    { key: "input", label: "输入负载(JSON对象)", type: "json" },
    { key: "strict", label: "严格模式(true/false)", type: "text" },
  ],
  failure_policy_v1: [
    { key: "operator", label: "目标算子", type: "text" },
    { key: "error", label: "错误文本", type: "text" },
    { key: "status_code", label: "状态码(可空)", type: "number" },
    { key: "attempts", label: "已重试次数", type: "number" },
    { key: "max_retries", label: "最大重试次数", type: "number" },
  ],
  incremental_plan_v1: [
    { key: "operator", label: "目标算子", type: "text" },
    { key: "input", label: "输入负载(JSON对象)", type: "json" },
    { key: "checkpoint_key", label: "断点键(可空)", type: "text" },
  ],
  tenant_isolation_v1: [
    { key: "op", label: "操作(get/set/reset)", type: "text" },
    { key: "tenant_id", label: "租户ID", type: "text" },
    { key: "max_concurrency", label: "并发上限", type: "number" },
    { key: "max_rows", label: "行数上限", type: "number" },
    { key: "max_payload_bytes", label: "负载字节上限", type: "number" },
    { key: "max_workflow_steps", label: "步骤上限", type: "number" },
  ],
  operator_policy_v1: [
    { key: "op", label: "操作(get/set/reset)", type: "text" },
    { key: "tenant_id", label: "租户ID", type: "text" },
    { key: "allow", label: "允许算子(逗号分隔)", type: "csv" },
    { key: "deny", label: "拒绝算子(逗号分隔)", type: "csv" },
  ],
  optimizer_adaptive_v2: [
    { key: "operator", label: "目标算子", type: "text" },
    { key: "row_count_hint", label: "行数提示", type: "number" },
    { key: "prefer_arrow", label: "偏好Arrow(true/false)", type: "text" },
  ],
  vector_index_v2_build: [
    { key: "shard", label: "分片", type: "text" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "id_field", label: "ID字段", type: "text" },
    { key: "text_field", label: "文本字段", type: "text" },
    { key: "metadata_fields", label: "元数据字段(逗号分隔)", type: "csv" },
    { key: "replace", label: "全量替换(true/false)", type: "text" },
  ],
  vector_index_v2_search: [
    { key: "query", label: "查询", type: "text" },
    { key: "top_k", label: "TopK", type: "number" },
    { key: "shard", label: "分片(可空)", type: "text" },
    { key: "filter_eq", label: "过滤条件(JSON对象)", type: "json" },
    { key: "rerank_meta_field", label: "重排元数据字段(可空)", type: "text" },
    { key: "rerank_meta_weight", label: "重排权重(可空)", type: "number" },
  ],
  vector_index_v2_eval: [
    { key: "run_id", label: "运行ID(可空)", type: "text" },
    { key: "shard", label: "分片(可空)", type: "text" },
    { key: "top_k", label: "TopK", type: "number" },
    { key: "cases", label: "评估样本(JSON数组)", type: "json" },
  ],
  stream_reliability_v1: [
    { key: "op", label: "操作(record/checkpoint/flush_dlq/replay/consistency_check/stats)", type: "text" },
    { key: "stream_key", label: "流键", type: "text" },
    { key: "msg_id", label: "消息ID", type: "text" },
    { key: "row", label: "消息行(JSON对象)", type: "json" },
    { key: "error", label: "错误(可空)", type: "text" },
    { key: "checkpoint", label: "断点", type: "number" },
  ],
  lineage_provenance_v1: [
    { key: "rules", label: "规则(JSON对象)", type: "json" },
    { key: "computed_fields_v3", label: "计算字段(JSON数组)", type: "json" },
    { key: "workflow_steps", label: "流程步骤(JSON数组)", type: "json" },
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "payload", label: "签名负载(JSON对象)", type: "json" },
    { key: "prev_hash", label: "前序哈希", type: "text" },
  ],
  contract_regression_v1: [
    { key: "operators", label: "目标算子(逗号分隔，可空)", type: "csv" },
  ],
  perf_baseline_v1: [
    { key: "op", label: "操作(get/set/check)", type: "text" },
    { key: "operator", label: "目标算子", type: "text" },
    { key: "p95_ms", label: "基线p95(ms)", type: "number" },
    { key: "max_p95_ms", label: "当前p95(ms)", type: "number" },
  ],
  plugin_operator_v1: [
    { key: "plugin", label: "插件名", type: "text" },
    { key: "op", label: "插件操作", type: "text" },
    { key: "payload", label: "插件参数(JSON对象)", type: "json" }
  ],
  explain_plan_v2: [
    { key: "steps", label: "步骤(JSON数组)", type: "json" },
    { key: "rows", label: "样本行(JSON数组)", type: "json" },
    { key: "actual_stats", label: "实际统计(JSON数组)", type: "json" },
    { key: "persist_feedback", label: "持久化反馈(true/false)", type: "text" },
    { key: "include_runtime_stats", label: "包含运行统计(true/false)", type: "text" },
  ],
  timeseries_forecast_v1: [
    { key: "rows", label: "时序行(JSON数组)", type: "json" },
    { key: "time_field", label: "时间字段", type: "text" },
    { key: "value_field", label: "数值字段", type: "text" },
    { key: "horizon", label: "预测步数", type: "number" },
    { key: "method", label: "方法(naive_drift/naive_last)", type: "text" },
  ],
  finance_ratio_v1: [
    { key: "rows", label: "财务行(JSON数组)", type: "json" },
  ],
  anomaly_explain_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "score_field", label: "异常分数字段", type: "text" },
    { key: "threshold", label: "阈值", type: "number" },
  ],
  evidence_conflict_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "claim_field", label: "论点字段", type: "text" },
    { key: "stance_field", label: "立场字段", type: "text" },
    { key: "source_field", label: "来源字段", type: "text" },
  ],
  template_bind_v1: [
    { key: "template_text", label: "模板文本", type: "text" },
    { key: "data", label: "绑定数据(JSON对象)", type: "json" },
  ],
  provenance_sign_v1: [
    { key: "payload", label: "签名负载(JSON对象)", type: "json" },
    { key: "prev_hash", label: "前序哈希", type: "text" },
  ],
  stream_state_v1_save: [
    { key: "stream_key", label: "流键", type: "text" },
    { key: "state", label: "状态(JSON对象)", type: "json" },
    { key: "offset", label: "偏移量", type: "number" },
  ],
  stream_state_v1_load: [
    { key: "stream_key", label: "流键", type: "text" },
  ],
  query_lang_v1: [
    { key: "rows", label: "输入行(JSON数组)", type: "json" },
    { key: "query", label: "查询语句(where/select/limit)", type: "text" },
  ],
};

const canvas = new WorkflowCanvas({
  store,
  nodeCatalog: NODE_CATALOG,
  canvasWrap: els.canvasWrap,
  canvasSurface: els.canvasSurface,
  nodesLayer: els.nodesLayer,
  guideLayer: els.guideLayer,
  minimapCanvas: els.minimap,
  edgesSvg: els.edges,
  onChange: renderAll,
  onWarn: (msg) => setStatus(msg, false),
  onSelectionChange: () => renderNodeConfigEditor(),
  onEdgeSelect: (edge) => {
    selectedEdge = edge && edge.from && edge.to ? { ...edge } : null;
    renderEdgeConfigEditor();
  },
});

const debugApiEnabled = (() => {
  try {
    const q = new URLSearchParams(window.location.search || "");
    return q.get("debug") === "1";
  } catch {
    return false;
  }
})();

if (debugApiEnabled) {
  window.__aiwfDebug = Object.freeze({
    // Simulate canvas link gesture semantics: relink same direction toggles off.
    tryLink: (from, to) => {
      const a = String(from || "");
      const b = String(to || "");
      if (!a || !b) return { ok: false, reason: "empty" };
      if (store.hasEdge(a, b)) {
        store.unlink(a, b);
        renderAll();
        return { ok: true, toggled: true };
      }
      const out = store.linkToFrom(a, b);
      if (out?.ok) renderAll();
      return out;
    },
    graph: () => store.exportGraph(),
    routeStats: () => canvas.getRouteMetrics(),
    selectNodes: (ids) => {
      canvas.setSelectedIds(Array.isArray(ids) ? ids : []);
      renderAll();
      return canvas.getSelectedIds();
    },
    setGraph: (graph) => {
      store.importGraph(graph || {});
      renderAll();
      return store.exportGraph();
    },
  });
} else {
  try { delete window.__aiwfDebug; } catch {}
}

function setStatus(text, ok = true) {
  els.status.className = `status ${ok ? "ok" : "bad"}`;
  els.status.textContent = text;
}

const {
  sandboxThresholdsPayload,
  sandboxDedupWindowSec,
  parseCsvList,
  sandboxRulesPayloadFromUi,
  applySandboxRulesToUi,
  applySandboxPresetToUi,
  renderSandboxHealth,
  reviewHistoryFilterPayload,
  renderReviewHistoryRows,
  refreshReviewHistory,
  exportReviewHistory,
  compareRuns,
  exportCompareReport,
  renderCompareResult,
  currentSandboxPresetPayload,
  applySandboxPresetPayload,
} = createWorkflowSupportUi(els, {
  setStatus,
  getLastCompareResult: () => lastCompareResult,
  setLastCompareResult: (out) => { lastCompareResult = out; },
});

const {
  normalizeAppSchemaObject,
  appSchemaRowsFromObject,
  collectAppSchemaFromForm,
  renderAppSchemaForm,
  syncAppSchemaJsonFromForm,
  syncAppSchemaFormFromJson,
  defaultRunParamValue,
  collectRunParamsForm,
  syncRunParamsJsonFromForm,
  renderRunParamsFormBySchema,
  syncRunParamsFormFromJson,
} = createWorkflowAppFormUi(els, { setStatus });

const {
  renderMigrationReport,
  renderRunHistoryRows,
  renderQueueRows,
  renderQueueControl,
  renderVersionRows,
  renderVersionCompare,
  renderCacheStats,
  renderAppRows,
  renderTimelineRows,
  renderFailureRows,
  renderSandboxRows,
  renderSandboxRuleVersionRows,
  renderSandboxAutoFixRows,
  renderQualityGateRows,
  qualityGateFilterPayload,
  qualityGatePrefsPayload,
  renderAuditRows,
  renderReviewRows,
} = createWorkflowPanelsUi(els, {
  setStatus,
  refreshRunHistory,
  refreshReviewQueue,
  refreshReviewHistory,
  refreshQueue,
  refreshDiagnostics,
  refreshSandboxRuleVersions,
  refreshSandboxAlerts,
  applySandboxRulesToUi,
  applyRestoredGraph: applyRestoredWorkflowGraph,
  renderSandboxHealth,
  normalizeAppSchemaObject,
  renderAppSchemaForm,
  appSchemaRowsFromObject,
  renderRunParamsFormBySchema,
  collectRunParamsForm,
  runPayload,
});

const {
  refreshQualityGateReports,
  exportQualityGateReports,
} = createWorkflowQualityGateUi(els, {
  setStatus,
  saveQualityGatePrefs,
  qualityGateFilterPayload,
  renderQualityGateRows,
});

({
  singleSelectedNode,
  prettyJson,
  parseNodeConfigText,
  parseEdgeWhenText,
  parseLooseJsonValue,
  setEdgeWhenBuilderVisibility,
  edgeWhenFromBuilder,
  applyEdgeWhenToBuilder,
  syncEdgeTextFromBuilder,
  rebuildEdgeHints,
  renderEdgeConfigEditor,
  formSchemaForNodeType,
  setCfgMode,
  toFieldDisplayValue,
  parseFieldValue,
  renderNodeConfigForm,
  parseNodeConfigForm,
  readMapTableRows,
  renderMapTableRows,
  syncIoMapFromTables,
  renderIoMapEditor,
  renderNodeConfigEditor,
} = createWorkflowConfigUi(els, {
  store,
  canvas,
  nodeFormSchemas: NODE_FORM_SCHEMAS,
  edgeHintsByNodeType: EDGE_HINTS_BY_NODE_TYPE,
  setStatus,
  renderAll,
  refreshOfflineBoundaryHint,
  getSelectedEdge: () => selectedEdge,
  setSelectedEdge: (edge) => { selectedEdge = edge; },
  getCfgViewMode: () => cfgViewMode,
  setCfgViewMode: (mode) => { cfgViewMode = mode; },
}));

const {
  parseRunParamsLoose,
  currentTemplateGovernance,
  allTemplates,
  renderTemplateSelect,
  refreshTemplateMarketplace,
  renderTemplateParamsForm,
  collectTemplateParamsFromForm,
  syncTemplateParamsJsonFromForm,
  applyTemplateDependencyState,
  applySelectedTemplate,
  saveCurrentAsTemplate,
  parseTemplateParams,
} = createWorkflowTemplateUi(els, {
  templateStorageKey: TEMPLATE_STORAGE_KEY,
  builtinTemplates: BUILTIN_TEMPLATES,
  store,
  setStatus,
  renderAll,
  renderMigrationReport,
  graphPayload,
  syncRunParamsFormFromJson,
});

const SIMPLE_NODE_TYPES = new Set([
  "ingest_files",
  "clean_md",
  "ds_refine",
  "ai_refine",
  "ai_audit",
  "manual_review",
  "compute_rust",
  "sql_chart_v1",
  "office_slot_fill_v1",
  "md_output",
]);

function resolveNodeCreateSpec(rawType) {
  const t = String(rawType || "").trim().toLowerCase();
  if (t === "ds_refine") {
    return {
      nodeType: "ai_refine",
      label: "DS提炼",
      config: {
        ...defaultNodeConfig("ai_refine"),
        reuse_existing: false,
        provider_name: "DeepSeek",
        ai_endpoint: String(els.aiEndpoint?.value || "").trim() || "https://api.deepseek.com/v1/chat/completions",
        ai_api_key: String(els.aiKey?.value || "").trim(),
        ai_model: String(els.aiModel?.value || "").trim() || "deepseek-chat",
      },
    };
  }
  return { nodeType: rawType, label: String(rawType || ""), config: null };
}

function renderPalette() {
  els.palette.innerHTML = "";
  const mode = String(els.paletteMode?.value || "simple").trim().toLowerCase();
  const kw = String(els.paletteSearch?.value || "").trim().toLowerCase();
  const list = NODE_CATALOG.filter((n) => {
    const t = String(n?.type || "").trim().toLowerCase();
    if (mode === "simple" && !SIMPLE_NODE_TYPES.has(t)) return false;
    if (!kw) return true;
    const hay = `${String(n?.name || "")} ${String(n?.type || "")} ${String(n?.desc || "")}`.toLowerCase();
    return hay.includes(kw);
  });
  list.forEach((n) => {
    const item = document.createElement("div");
    const titleWrap = document.createElement("div");
    const title = document.createElement("strong");
    const type = document.createElement("div");
    const desc = document.createElement("div");
    item.className = "palette-item";
    item.draggable = true;
    item.dataset.nodeType = String(n.type || "");
    title.textContent = String(n.name || "");
    titleWrap.appendChild(title);
    type.style.fontSize = "12px";
    type.style.color = "#4f6378";
    type.textContent = String(n.type || "");
    desc.style.fontSize = "12px";
    desc.style.color = "#6b7f94";
    desc.style.marginTop = "3px";
    desc.textContent = String(n.desc || "");
    item.append(titleWrap, type, desc);
    els.palette.appendChild(item);
  });
  Array.from(els.palette.querySelectorAll(".palette-item")).forEach((item) => {
    item.addEventListener("click", () => {
      els.nodeType.value = String(item.dataset.nodeType || "ingest_files");
    });
    item.addEventListener("dragstart", (evt) => {
      const t = String(item.dataset.nodeType || "");
      evt.dataTransfer.setData("text/plain", t);
      evt.dataTransfer.effectAllowed = "copy";
    });
  });
}



function saveQualityGatePrefs() {
  try {
    localStorage.setItem(QUALITY_GATE_PREFS_KEY, JSON.stringify(qualityGatePrefsPayload()));
  } catch {}
}

function loadQualityGatePrefs() {
  try {
    const raw = localStorage.getItem(QUALITY_GATE_PREFS_KEY);
    if (!raw) return;
    const obj = JSON.parse(raw);
    const filter = obj?.filter && typeof obj.filter === "object" ? obj.filter : {};
    if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.value = String(filter.run_id || "");
    const status = String(filter.status || "all").trim().toLowerCase();
    if (els.qualityGateStatusFilter) els.qualityGateStatusFilter.value = (status === "blocked" || status === "pass") ? status : "all";
    const fmt = String(obj?.format || "md").trim().toLowerCase();
    if (els.qualityGateExportFormat) els.qualityGateExportFormat.value = fmt === "json" ? "json" : "md";
  } catch {}
}



function focusNodeInCanvas(nodeId) {
  const id = String(nodeId || "").trim();
  if (!id) return;
  const node = store.getNode(id);
  if (!node) return;
  canvas.setSelectedIds([id]);
  renderAll();
  const cx = Math.max(0, Number(node.x || 0) - Math.floor(els.canvasWrap.clientWidth * 0.35));
  const cy = Math.max(0, Number(node.y || 0) - Math.floor(els.canvasWrap.clientHeight * 0.35));
  els.canvasWrap.scrollTo({ left: cx, top: cy, behavior: "smooth" });
}

const { computePreflightRisk, renderPreflightReport, renderAutoFixDiff } = createWorkflowPreflightUi(els, { focusNodeInCanvas });


function buildIoContractInput(operator, nodeConfig) {
  const op = String(operator || "").trim();
  const cfg = nodeConfig && typeof nodeConfig === "object" ? nodeConfig : {};
  const firstInputFile = String((els.inputFiles?.value || "").split(/\r?\n/).map((s) => s.trim()).find(Boolean) || "");
  if (op === "transform_rows_v2" || op === "transform_rows_v3" || op === "load_rows_v3") {
    const input = {};
    if (Array.isArray(cfg.rows) && cfg.rows.length) input.rows = cfg.rows;
    else if (cfg.input_uri) input.input_uri = cfg.input_uri;
    else if (firstInputFile) input.input_uri = firstInputFile;
    else input.rows = [];
    return input;
  }
  if (op === "finance_ratio_v1") return { rows: Array.isArray(cfg.rows) ? cfg.rows : [] };
  if (op === "anomaly_explain_v1") {
    return {
      rows: Array.isArray(cfg.rows) ? cfg.rows : [],
      score_field: String(cfg.score_field || "").trim(),
    };
  }
  if (op === "stream_window_v2") {
    return {
      stream_key: String(cfg.stream_key || "").trim(),
      event_time_field: String(cfg.event_time_field || "").trim(),
    };
  }
  if (op === "plugin_operator_v1") return { plugin: String(cfg.plugin || "").trim() };
  return cfg;
}

async function postRustOperator(endpoint, operatorPath, payload) {
  const resp = await fetch(`${endpoint}${operatorPath}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload || {}),
  });
  if (!resp.ok) {
    return { ok: false, status: resp.status, error: `HTTP ${resp.status}` };
  }
  const body = await resp.json();
  return { ok: true, body };
}

function autoFixGraphStructure() {
  const graph = store.exportGraph();
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes.slice() : [];
  const edges = Array.isArray(graph?.edges) ? graph.edges.slice() : [];
  const nodeIds = new Set(nodes.map((n) => String(n?.id || "")).filter(Boolean));
  const cleanedEdges = [];
  const edgeSeen = new Set();
  let removedDup = 0;
  let removedSelf = 0;
  let removedBroken = 0;
  const dupEdges = [];
  const selfLoops = [];
  const brokenEdges = [];
  edges.forEach((e) => {
    const from = String(e?.from || "").trim();
    const to = String(e?.to || "").trim();
    if (!from || !to || !nodeIds.has(from) || !nodeIds.has(to)) {
      removedBroken += 1;
      brokenEdges.push({ from, to });
      return;
    }
    if (from === to) {
      removedSelf += 1;
      selfLoops.push({ from, to });
      return;
    }
    const k = `${from}=>${to}`;
    if (edgeSeen.has(k)) {
      removedDup += 1;
      dupEdges.push({ from, to });
      return;
    }
    edgeSeen.add(k);
    cleanedEdges.push(e);
  });
  const inDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
  const outDegree = new Map(nodes.map((n) => [String(n.id || ""), 0]));
  cleanedEdges.forEach((e) => {
    const from = String(e?.from || "");
    const to = String(e?.to || "");
    outDegree.set(from, (outDegree.get(from) || 0) + 1);
    inDegree.set(to, (inDegree.get(to) || 0) + 1);
  });
  let removedIsolated = 0;
  const isolatedNodes = [];
  const cleanedNodes = nodes.filter((n) => {
    const id = String(n?.id || "");
    if (nodes.length <= 1) return true;
    const isolated = (inDegree.get(id) || 0) === 0 && (outDegree.get(id) || 0) === 0;
    if (isolated) {
      removedIsolated += 1;
      isolatedNodes.push({ id, type: String(n?.type || "") });
    }
    return !isolated;
  });
  store.importGraph({ ...graph, nodes: cleanedNodes, edges: cleanedEdges });
  selectedEdge = null;
  renderAll();
  const moved = removedDup + removedSelf + removedBroken + removedIsolated;
  return {
    changed: moved > 0,
    removed_dup_edges: removedDup,
    removed_self_loops: removedSelf,
    removed_broken_edges: removedBroken,
    removed_isolated_nodes: removedIsolated,
    dup_edges: dupEdges,
    self_loops: selfLoops,
    broken_edges: brokenEdges,
    isolated_nodes: isolatedNodes,
  };
}

async function runWorkflowPreflight() {
  const graph = graphPayload();
  const valid = validateGraph(graph);
  const issues = [];
  (valid.errors || []).forEach((msg) => issues.push({ level: "error", kind: "graph", message: String(msg) }));
  (valid.warnings || []).forEach((msg) => issues.push({ level: "warning", kind: "graph", message: String(msg) }));

  const endpoint = String(els.rustEndpoint?.value || "").trim().replace(/\/$/, "");
  const rustNodes = (Array.isArray(graph?.nodes) ? graph.nodes : []).filter((n) => IO_CONTRACT_COMPATIBLE_OPERATORS.has(String(n?.type || "")));
  if (!endpoint) {
    if (rustNodes.length > 0 && els.rustRequired?.checked) {
      issues.push({ level: "error", kind: "rust", message: "Rust Endpoint 为空，无法执行契约预检" });
    }
  } else if (rustNodes.length > 0) {
    try {
      const ops = Array.from(new Set(rustNodes.map((n) => String(n.type || ""))));
      const capRes = await postRustOperator(endpoint, "/operators/capabilities_v1", { include_ops: ops });
      if (!capRes.ok) {
        issues.push({
          level: els.rustRequired?.checked ? "error" : "warning",
          kind: "rust",
          message: `能力发现失败: ${capRes.error || "unknown"}`,
        });
      }
    } catch (e) {
      issues.push({
        level: els.rustRequired?.checked ? "error" : "warning",
        kind: "rust",
        message: `能力发现异常: ${String(e)}`,
      });
    }

    for (const n of rustNodes) {
      const operator = String(n?.type || "");
      const payload = {
        run_id: `preflight_${Date.now()}`,
        operator,
        input: buildIoContractInput(operator, n?.config || {}),
        strict: false,
      };
      try {
        const out = await postRustOperator(endpoint, "/operators/io_contract_v1/validate", payload);
        if (!out.ok) {
          issues.push({
            level: els.rustRequired?.checked ? "error" : "warning",
            kind: "io_contract",
            node_id: String(n?.id || ""),
            message: `${operator} 契约校验请求失败: ${out.error || "unknown"}`,
          });
          continue;
        }
        const body = out.body || {};
        const validContract = !!body.valid;
        const errs = Array.isArray(body.errors) ? body.errors : [];
        if (!validContract || errs.length) {
          issues.push({
            level: "error",
            kind: "io_contract",
            node_id: String(n?.id || ""),
            message: `${operator} 契约不通过: ${errs.join("; ") || "unknown"}`,
          });
        }
      } catch (e) {
        issues.push({
          level: els.rustRequired?.checked ? "error" : "warning",
          kind: "io_contract",
          node_id: String(n?.id || ""),
          message: `${operator} 契约校验异常: ${String(e)}`,
        });
      }
    }
  }
  const ok = issues.every((x) => String(x.level || "") !== "error");
  const risk = computePreflightRisk(issues);
  const report = { ok, issues, risk, ts: new Date().toISOString() };
  lastPreflightReport = report;
  renderPreflightReport(report);
  return report;
}

async function exportPreflightReport() {
  const report = lastPreflightReport || await runWorkflowPreflight();
  const format = String(els.preflightExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
  const out = await window.aiwfDesktop.exportWorkflowPreflightReport({
    report,
    format,
  });
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导出预检报告失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`预检报告已导出: ${out.path}`, true);
}

async function runTemplateAcceptance() {
  const id = String(els.templateSelect?.value || "").trim();
  const tpl = allTemplates().find((x) => String(x.id || "") === id);
  const before = await runWorkflowPreflight();
  const fix = autoFixGraphStructure();
  lastAutoFixSummary = fix;
  renderAutoFixDiff(fix);
  const after = await runWorkflowPreflight();
  const accepted = !!after?.ok;
  const report = {
    ts: new Date().toISOString(),
    template_id: id || "",
    template_name: String(tpl?.name || ""),
    accepted,
    governance: currentTemplateGovernance(),
    before,
    auto_fix: fix,
    after,
  };
  lastTemplateAcceptanceReport = report;
  if (accepted) setStatus("模板验收通过", true);
  else setStatus("模板验收未通过：仍有错误，请修复后重试", false);
  if (els.log) els.log.textContent = JSON.stringify(report, null, 2);
  return report;
}

async function exportTemplateAcceptanceReport() {
  const report = lastTemplateAcceptanceReport || await runTemplateAcceptance();
  const format = String(els.templateAcceptanceExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
  const out = await window.aiwfDesktop.exportWorkflowTemplateAcceptanceReport({
    report,
    format,
  });
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导出模板验收报告失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`模板验收报告已导出: ${out.path}`, true);
}

function graphPayload() {
  store.setWorkflowName(els.workflowName.value);
  return store.exportGraph();
}



function runPayload(extra = {}) {
  const graph = graphPayload();
  const isolatedTypes = String(els.chipletIsolatedTypes?.value || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const toNum = (v, d) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : d;
  };
  const base = {
    workflow_id: graph.workflow_id || "custom_v1",
    workflow: graph,
    quality_rule_set_id: String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim(),
    params: {
      report_title: String(els.reportTitle.value || "").trim(),
      input_files: String(els.inputFiles.value || "").trim(),
      md_only: true,
      paper_markdown_enabled: true,
      export_canonical_bundle: !!els.exportCanonicalBundle.checked,
      canonical_title: String(els.canonicalTitle.value || "").trim() || "AIWF 熟肉语料",
      ocr_lang: "chi_sim+eng",
    },
    breakpoint_node_id: String(els.breakpointNodeId?.value || "").trim(),
    ai: {
      endpoint: String(els.aiEndpoint.value || "").trim(),
      api_key: String(els.aiKey.value || "").trim(),
      model: String(els.aiModel.value || "").trim(),
      temperature: 0.2,
    },
    rust: {
      endpoint: String(els.rustEndpoint.value || "").trim(),
      required: !!els.rustRequired.checked,
    },
    chiplet_isolation_enabled: els.chipletIsolationEnabled ? !!els.chipletIsolationEnabled.checked : true,
    chiplet_isolation_mode: String(els.chipletIsolationMode?.value || "high_risk").trim() || "high_risk",
    chiplet_isolated_types: isolatedTypes,
    sandbox_alert_dedup_window_sec: sandboxDedupWindowSec(),
    sandbox_autofix_enabled: els.sandboxAutoFixEnabled ? !!els.sandboxAutoFixEnabled.checked : true,
    sandbox_autofix_pause_queue: els.sandboxAutoFixPauseQueue ? !!els.sandboxAutoFixPauseQueue.checked : true,
    sandbox_autofix_require_review: els.sandboxAutoFixRequireReview ? !!els.sandboxAutoFixRequireReview.checked : true,
    sandbox_autofix_force_isolation: els.sandboxAutoFixForceIsolation ? !!els.sandboxAutoFixForceIsolation.checked : true,
    sandbox_autofix_red_threshold: Number(els.sandboxAutoFixRedThreshold?.value || 3),
    sandbox_autofix_window_sec: Number(els.sandboxAutoFixWindowSec?.value || 900),
    sandbox_autofix_force_minutes: Number(els.sandboxAutoFixForceMinutes?.value || 60),
    sandbox_autofix_force_mode: String(els.sandboxAutoFixForceMode?.value || "process").trim() || "process",
    sandbox_limits: {
      max_duration_ms: toNum(els.sandboxMaxDurationMs?.value, 180000),
      max_cpu_ms: toNum(els.sandboxMaxCpuMs?.value, 120000),
      max_rss_mb: toNum(els.sandboxMaxRssMb?.value, 512),
      max_output_bytes: toNum(els.sandboxMaxOutputBytes?.value, 2000000),
    },
  };
  const ext = extra && typeof extra === "object" ? extra : {};
  const out = { ...base, ...ext };
  out.params = { ...(base.params || {}), ...(ext.params || {}) };
  return out;
}

function renderAll() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.render();
  syncCanvasPanels();
}

function exportJson() {
  const json = JSON.stringify(graphPayload(), null, 2);
  els.log.textContent = json;
  setStatus("已导出流程 JSON 到右侧日志区", true);
}

function canvasViewportCenter() {
  const rect = els.canvasWrap.getBoundingClientRect();
  return {
    clientX: rect.left + els.canvasWrap.clientWidth / 2,
    clientY: rect.top + els.canvasWrap.clientHeight / 2,
  };
}

function syncCanvasPanels() {
  renderNodeConfigEditor();
  renderEdgeConfigEditor();
  refreshOfflineBoundaryHint();
  const pct = Math.round(canvas.getZoom() * 100);
  if (els.zoomText) els.zoomText.textContent = `${pct}%`;
}

function setZoom(z, focusClient = null) {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.setZoom(z, focusClient || canvasViewportCenter());
  syncCanvasPanels();
}

function fitCanvasToView() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  const ok = canvas.fitToView(104);
  syncCanvasPanels();
  if (ok) setStatus("已适配当前流程视图", true);
  else setStatus("当前没有可适配的节点", false);
}

async function saveFlow() {
  try {
    const graph = graphPayload();
    const name = String(els.workflowName.value || "").trim() || "workflow";
    const out = await window.aiwfDesktop.saveWorkflow(graph, name);
    if (out?.ok) {
      setStatus(`流程已保存: ${out.path}`, true);
      await refreshVersions();
    }
    else if (!out?.canceled) setStatus(`保存失败: ${out?.error || "unknown"}`, false);
  } catch (e) {
    setStatus(`保存失败: ${e}`, false);
  }
}

async function loadFlow() {
  try {
    const out = await window.aiwfDesktop.loadWorkflow();
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`加载失败: ${out?.error || "unknown"}`, false);
      return;
    }
    const migrated = migrateLoadedWorkflowGraph(out.graph || {});
    store.importGraph(migrated.graph || {});
    selectedEdge = null;
    els.workflowName.value = store.state.graph.name || "自定义流程";
    renderAll();
    renderMigrationReport(migrated);
    if (migrated.migrated) {
      setStatus(`流程已加载并迁移: ${out.path} (${migrated.notes.join(", ")})`, true);
    } else {
      setStatus(`流程已加载: ${out.path}`, true);
    }
  } catch (e) {
    setStatus(`加载失败: ${e}`, false);
  }
}

async function runWorkflow() {
  const pre = await runWorkflowPreflight();
  const strictOutputGate = true;
  if (!pre.ok && !strictOutputGate) {
    const errs = (pre.issues || []).filter((x) => String(x.level || "") === "error").map((x) => String(x.message || ""));
    setStatus(`预检失败: ${errs.join(" | ")}`, false);
    return;
  }
  const warns = (pre.issues || []).filter((x) => String(x.level || "") === "warning").map((x) => String(x.message || ""));
  if (warns.length) {
    setStatus(`预检警告: ${warns.join(" | ")}`, true);
  }
  if (!pre.ok && strictOutputGate) {
    setStatus("预检未通过，已启用严格产物门禁：本次仅输出 Markdown 熟肉。", true);
  }
  setStatus("工作流运行中...");
  try {
    const out = await window.aiwfDesktop.runWorkflow(runPayload({
      params: {
        strict_output_gate: strictOutputGate,
        preflight_passed: !!pre.ok,
        preflight_risk_score: Number(pre?.risk?.score || 0),
        preflight_risk_label: String(pre?.risk?.label || ""),
      },
    }), {});
    els.log.textContent = JSON.stringify(out, null, 2);
    renderNodeRuns(out.node_runs || []);
    await refreshDiagnostics();
    await refreshRunHistory();
    await refreshReviewQueue();
    if (out?.ok) {
      const slaPassed = out?.sla?.passed !== false;
      const lineageEdges = Number(out?.lineage?.edge_count || 0);
      const aiCalls = Number(out?.governance?.ai_budget?.calls || 0);
      setStatus(`运行完成: ${out.run_id} | SLA:${slaPassed ? "通过" : "未通过"} | 血缘边:${lineageEdges} | AI调用:${aiCalls}`, true);
    } else {
      setStatus(`运行结束: ${out.status || "failed"}`, false);
    }
  } catch (e) {
    setStatus(`运行失败: ${e}`, false);
  }
}

async function refreshQualityRuleSets() {
  if (!els.qualityRuleSetSelect) return;
  try {
    const out = await window.aiwfDesktop.listQualityRuleSets();
    const sets = Array.isArray(out?.sets) ? out.sets : [];
    const cur = String(els.qualityRuleSetId?.value || "").trim();
    els.qualityRuleSetSelect.innerHTML = '<option value="">选择规则集...</option>';
    sets.forEach((s) => {
      const id = String(s?.id || "");
      if (!id) return;
      const op = document.createElement("option");
      op.value = id;
      op.textContent = `${String(s?.name || id)} (${String(s?.version || "v1")})`;
      els.qualityRuleSetSelect.appendChild(op);
    });
    if (cur) els.qualityRuleSetSelect.value = cur;
  } catch {}
}

function collectRulesFromGraph() {
  const g = store.exportGraph();
  const nodes = Array.isArray(g?.nodes) ? g.nodes : [];
  const target = nodes.find((n) => ["quality_check_v2", "quality_check_v3", "quality_check_v4"].includes(String(n?.type || "")));
  if (!target) return {};
  const cfg = target?.config && typeof target.config === "object" ? target.config : {};
  return cfg.rules && typeof cfg.rules === "object" ? cfg.rules : {};
}

async function saveQualityRuleSetFromGraph() {
  const id = String(els.qualityRuleSetId?.value || "").trim();
  if (!id) {
    setStatus("请先填写质量规则集ID", false);
    return;
  }
  const rules = collectRulesFromGraph();
  const out = await window.aiwfDesktop.saveQualityRuleSet({
    set: {
      id,
      name: id,
      version: "v1",
      scope: "workflow",
      rules,
    },
  });
  if (out?.ok) {
    await refreshQualityRuleSets();
    setStatus(`质量规则集已保存: ${id}`, true);
  } else {
    setStatus(`保存规则集失败: ${out?.error || "unknown"}`, false);
  }
}

async function removeQualityRuleSetCurrent() {
  const id = String(els.qualityRuleSetId?.value || els.qualityRuleSetSelect?.value || "").trim();
  if (!id) {
    setStatus("请先选择质量规则集", false);
    return;
  }
  const out = await window.aiwfDesktop.removeQualityRuleSet({ id });
  if (out?.ok) {
    if (els.qualityRuleSetId) els.qualityRuleSetId.value = "";
    await refreshQualityRuleSets();
    setStatus(`质量规则集已删除: ${id}`, true);
  } else {
    setStatus(`删除规则集失败: ${out?.error || "unknown"}`, false);
  }
}

async function installTemplatePack() {
  const out = await window.aiwfDesktop.loadWorkflow();
  if (!out?.ok || !out?.path) {
    if (!out?.canceled) setStatus(`读取模板包失败: ${out?.error || "unknown"}`, false);
    return;
  }
  const ret = await window.aiwfDesktop.installTemplatePack({ path: out.path });
  if (!ret?.ok) {
    setStatus(`安装模板包失败: ${ret?.error || "unknown"}`, false);
    return;
  }
  await refreshTemplateMarketplace();
  renderTemplateSelect();
  setStatus(`模板包已安装: ${ret?.item?.name || ret?.item?.id || ""}`, true);
}

async function removeTemplatePackByCurrentTemplate() {
  const id = String(els.templateSelect?.value || "").trim();
  const tpl = allTemplates().find((x) => String(x?.id || "") === id);
  const packId = String(tpl?.__pack_id || "").trim();
  if (!packId) {
    setStatus("当前模板不是模板包来源，无法移除", false);
    return;
  }
  const out = await window.aiwfDesktop.removeTemplatePack({ id: packId });
  if (!out?.ok) {
    setStatus(`移除模板包失败: ${out?.error || "unknown"}`, false);
    return;
  }
  await refreshTemplateMarketplace();
  renderTemplateSelect();
  setStatus(`模板包已移除: ${packId}`, true);
}

async function exportTemplatePackByCurrentTemplate() {
  const id = String(els.templateSelect?.value || "").trim();
  const tpl = allTemplates().find((x) => String(x?.id || "") === id);
  const packId = String(tpl?.__pack_id || "").trim();
  if (!packId) {
    setStatus("当前模板不是模板包来源，无法导出", false);
    return;
  }
  const out = await window.aiwfDesktop.exportTemplatePack({ id: packId });
  if (out?.ok) setStatus(`模板包已导出: ${out.path}`, true);
  else if (!out?.canceled) setStatus(`导出模板包失败: ${out?.error || "unknown"}`, false);
}

async function saveCurrentRunAsBaseline() {
  const runA = String(els.compareRunA?.value || "").trim();
  if (!runA) {
    setStatus("请先在“运行对比”里选择 Run A 作为基线", false);
    return;
  }
  const out = await window.aiwfDesktop.saveRunBaseline({ run_id: runA, name: `baseline_${runA.slice(0, 8)}` });
  if (out?.ok) setStatus(`基线已保存: ${out?.item?.baseline_id}`, true);
  else setStatus(`保存基线失败: ${out?.error || "unknown"}`, false);
}

async function compareWithLatestBaseline() {
  const runB = String(els.compareRunB?.value || "").trim();
  if (!runB) {
    setStatus("请先在“运行对比”里选择 Run B", false);
    return;
  }
  const baselines = await window.aiwfDesktop.listRunBaselines();
  const first = Array.isArray(baselines?.items) ? baselines.items[0] : null;
  if (!first) {
    setStatus("未找到基线，请先保存基线", false);
    return;
  }
  const out = await window.aiwfDesktop.compareRunWithBaseline({ run_id: runB, baseline_id: first.baseline_id });
  if (!out?.ok) {
    setStatus(`基线对比失败: ${out?.error || "unknown"}`, false);
    return;
  }
  const reg = out.regression || {};
  setStatus(`基线对比完成: changed=${reg.changed_nodes || 0}, status_flip=${reg.status_flip_nodes || 0}, perf_hot=${reg.perf_hot_nodes || 0}`, true);
}

async function loadLineageForRunA() {
  const runId = String(els.compareRunA?.value || "").trim();
  if (!runId) {
    setStatus("请先在“运行对比”里选择 Run A", false);
    return;
  }
  const out = await window.aiwfDesktop.getWorkflowLineage({ run_id: runId });
  if (!out?.ok) {
    setStatus(`加载血缘失败: ${out?.error || "unknown"}`, false);
    return;
  }
  const lineage = out.lineage || {};
  const nodes = Array.isArray(lineage?.nodes) ? lineage.nodes.length : Number(lineage?.node_count || 0);
  const edges = Array.isArray(lineage?.edges) ? lineage.edges.length : Number(lineage?.edge_count || 0);
  els.log.textContent = JSON.stringify({ run_id: runId, lineage }, null, 2);
  setStatus(`血缘已加载: nodes=${nodes}, edges=${edges}`, true);
}

async function enqueueWorkflowRun() {
  const pre = await runWorkflowPreflight();
  const strictOutputGate = true;
  if (!pre.ok && !strictOutputGate) {
    const errs = (pre.issues || []).filter((x) => String(x.level || "") === "error").map((x) => String(x.message || ""));
    setStatus(`预检失败: ${errs.join(" | ")}`, false);
    return;
  }
  const warns = (pre.issues || []).filter((x) => String(x.level || "") === "warning").map((x) => String(x.message || ""));
  if (warns.length) {
    setStatus(`预检警告: ${warns.join(" | ")}`, true);
  }
  if (!pre.ok && strictOutputGate) {
    setStatus("预检未通过，入队任务将自动降级为 md_only。", true);
  }
  try {
    const out = await window.aiwfDesktop.enqueueWorkflowTask({
      label: String(els.workflowName?.value || "workflow_task"),
      payload: runPayload({
        params: {
          strict_output_gate: strictOutputGate,
          preflight_passed: !!pre.ok,
          preflight_risk_score: Number(pre?.risk?.score || 0),
          preflight_risk_label: String(pre?.risk?.label || ""),
        },
      }),
      cfg: {},
      priority: 100,
    });
    setStatus(out?.ok ? "任务已加入队列" : `入队失败: ${out?.error || "unknown"}`, !!out?.ok);
    await refreshQueue();
  } catch (e) {
    setStatus(`入队失败: ${e}`, false);
  }
}

async function refreshDiagnostics() {
  try {
    const [out, rust, perf] = await Promise.all([
      window.aiwfDesktop.getWorkflowDiagnostics({ limit: 80 }),
      fetchRustRuntimeStats(),
      window.aiwfDesktop.getWorkflowPerfDashboard({ limit: 200 }),
    ]);
    renderDiagRuns(out || {}, rust || {}, perf || {});
  } catch {}
}

function refreshOfflineBoundaryHint() {
  if (!els.offlineBoundaryHint) return;
  const graph = store.exportGraph();
  const nodes = Array.isArray(graph?.nodes) ? graph.nodes : [];
  const onlineNodes = nodes.filter((n) => ONLINE_REQUIRED_NODE_TYPES.has(String(n?.type || "")));
  const unknownNodes = nodes.filter((n) => {
    const t = String(n?.type || "");
    return !OFFLINE_LOCAL_NODE_TYPES.has(t) && !ONLINE_REQUIRED_NODE_TYPES.has(t);
  });
  if (onlineNodes.length === 0 && unknownNodes.length === 0) {
    els.offlineBoundaryHint.textContent = "离线能力边界：当前流程全部为本地可执行节点（离线可跑）。";
    return;
  }
  const aiEndpoint = String(els.aiEndpoint?.value || "").trim();
  const lines = [];
  if (onlineNodes.length > 0) {
    const types = Array.from(new Set(onlineNodes.map((n) => String(n.type || "")))).join(", ");
    lines.push(`检测到在线节点: ${types}。`);
    lines.push(aiEndpoint ? "已配置外部 AI Endpoint，可在线执行。" : "未配置外部 AI Endpoint，这些节点离线不可执行。");
  }
  if (unknownNodes.length > 0) {
    const types = Array.from(new Set(unknownNodes.map((n) => String(n.type || "")))).join(", ");
    lines.push(`检测到未知边界节点: ${types}（请确认是否需要外部服务）。`);
  }
  els.offlineBoundaryHint.textContent = `离线能力边界：${lines.join(" ")}`;
}

async function refreshRunHistory() {
  try {
    const out = await window.aiwfDesktop.listWorkflowRuns({ limit: 80 });
    renderRunHistoryRows(out?.items || []);
  } catch {
    renderRunHistoryRows([]);
  }
}

async function refreshQueue() {
  try {
    const out = await window.aiwfDesktop.listWorkflowQueue({ limit: 120 });
    renderQueueRows(out?.items || []);
    renderQueueControl(out?.control || {});
  } catch {
    renderQueueRows([]);
    renderQueueControl({});
  }
}

async function refreshVersions() {
  try {
    const out = await window.aiwfDesktop.listWorkflowVersions({ limit: 120 });
    renderVersionRows(out?.items || []);
  } catch {
    renderVersionRows([]);
  }
}

async function pauseQueue() {
  const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: true });
  setStatus(out?.ok ? "队列已暂停" : `暂停失败: ${out?.error || "unknown"}`, !!out?.ok);
  await refreshQueue();
}

async function resumeQueue() {
  const out = await window.aiwfDesktop.setWorkflowQueueControl({ paused: false });
  setStatus(out?.ok ? "队列已恢复" : `恢复失败: ${out?.error || "unknown"}`, !!out?.ok);
  await refreshQueue();
}

async function compareVersions() {
  const a = String(els.versionCompareA?.value || "").trim();
  const b = String(els.versionCompareB?.value || "").trim();
  if (!a || !b) {
    setStatus("请填写版本 A/B", false);
    return;
  }
  const out = await window.aiwfDesktop.compareWorkflowVersions({ version_a: a, version_b: b });
  renderVersionCompare(out);
  setStatus(out?.ok ? "版本对比完成" : `版本对比失败: ${out?.error || "unknown"}`, !!out?.ok);
}

async function refreshCacheStats() {
  try {
    const out = await window.aiwfDesktop.getWorkflowNodeCacheStats();
    renderCacheStats(out?.stats || {});
  } catch {
    renderCacheStats({});
  }
}

async function clearCache() {
  const out = await window.aiwfDesktop.clearWorkflowNodeCache();
  renderCacheStats(out?.stats || {});
  setStatus(out?.ok ? "缓存已清空" : `清空缓存失败: ${out?.error || "unknown"}`, !!out?.ok);
}



async function publishApp() {
  const graph = graphPayload();
  const name = String(els.appPublishName?.value || graph?.name || "").trim();
  if (!name) {
    setStatus("应用名称不能为空", false);
    return;
  }
  if (els.publishRequirePreflight?.checked) {
    const pre = await runWorkflowPreflight();
    if (!pre?.ok) {
      const errs = (pre.issues || []).filter((x) => String(x.level || "") === "error").map((x) => String(x.message || ""));
      setStatus(`发布阻断：预检未通过 (${errs.join(" | ")})`, false);
      return;
    }
  }
  let schema = {};
  try {
    const fromForm = collectAppSchemaFromForm();
    if (fromForm && Object.keys(fromForm).length) schema = fromForm;
    else if (String(els.appSchemaJson?.value || "").trim()) schema = normalizeAppSchemaObject(JSON.parse(String(els.appSchemaJson.value || "{}")));
  } catch (e) {
    setStatus(`参数 Schema 非法: ${e}`, false);
    return;
  }
  const out = await window.aiwfDesktop.publishWorkflowApp({
    name,
    graph,
    params_schema: schema,
    template_policy: {
      version: 1,
      governance: currentTemplateGovernance(),
      runtime_defaults: parseRunParamsLoose(),
      latest_preflight: lastPreflightReport || null,
      latest_template_acceptance: lastTemplateAcceptanceReport || null,
    },
  });
  setStatus(out?.ok ? "流程应用已发布" : `发布失败: ${out?.error || "unknown"}`, !!out?.ok);
  await refreshApps();
}

async function refreshApps() {
  try {
    const out = await window.aiwfDesktop.listWorkflowApps({ limit: 120 });
    renderAppRows(out?.items || []);
  } catch {
    renderAppRows([]);
  }
}

async function refreshTimeline() {
  const runId = String(els.timelineRunId?.value || "").trim();
  if (!runId) {
    setStatus("请先填写 Run ID", false);
    return;
  }
  const out = await window.aiwfDesktop.getWorkflowRunTimeline({ run_id: runId });
  renderTimelineRows(out);
  setStatus(out?.ok ? "时间线刷新完成" : `时间线刷新失败: ${out?.error || "unknown"}`, !!out?.ok);
}

async function refreshFailureSummary() {
  const out = await window.aiwfDesktop.getWorkflowFailureSummary({ limit: 500 });
  renderFailureRows(out || {});
}

async function refreshSandboxAlerts() {
  const out = await window.aiwfDesktop.getWorkflowSandboxAlerts({
    limit: 500,
    thresholds: sandboxThresholdsPayload(),
    dedup_window_sec: sandboxDedupWindowSec(),
  });
  if (out?.rules) applySandboxRulesToUi(out.rules);
  renderSandboxRows(out || {});
}

async function exportSandboxAudit() {
  const format = String(els.sandboxExportFormat?.value || "md").trim() || "md";
  const out = await window.aiwfDesktop.exportWorkflowSandboxAuditReport({
    limit: 500,
    thresholds: sandboxThresholdsPayload(),
    dedup_window_sec: sandboxDedupWindowSec(),
    format,
  });
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导出Sandbox报告失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`Sandbox报告已导出: ${out.path}`, true);
}

async function loadSandboxRules() {
  const out = await window.aiwfDesktop.getWorkflowSandboxAlertRules();
  if (!out?.ok) {
    setStatus(`加载Sandbox规则失败: ${out?.error || "unknown"}`, false);
    return;
  }
  applySandboxRulesToUi(out.rules || {});
  setStatus("Sandbox规则已加载", true);
}

async function refreshSandboxRuleVersions() {
  const out = await window.aiwfDesktop.listWorkflowSandboxRuleVersions({ limit: 80 });
  renderSandboxRuleVersionRows(out?.items || []);
}

async function saveSandboxRules() {
  const out = await window.aiwfDesktop.setWorkflowSandboxAlertRules({
    rules: sandboxRulesPayloadFromUi(),
  });
  if (!out?.ok) {
    setStatus(`保存Sandbox规则失败: ${out?.error || "unknown"}`, false);
    return;
  }
  applySandboxRulesToUi(out.rules || {});
  setStatus("Sandbox规则已保存", true);
  await refreshSandboxRuleVersions();
  await refreshSandboxAlerts();
}

async function applySandboxPreset() {
  const preset = String(els.sandboxPreset?.value || "balanced").trim().toLowerCase();
  applySandboxPresetToUi(preset);
  const out = await window.aiwfDesktop.setWorkflowSandboxAlertRules({
    rules: sandboxRulesPayloadFromUi(),
  });
  if (!out?.ok) {
    setStatus(`应用预设失败: ${out?.error || "unknown"}`, false);
    return;
  }
  applySandboxRulesToUi(out.rules || {});
  setStatus(`已应用Sandbox预设: ${preset}`, true);
  await refreshSandboxRuleVersions();
  await refreshSandboxAlerts();
}

async function applySandboxMute() {
  const out = await window.aiwfDesktop.muteWorkflowSandboxAlert({
    node_type: String(els.sandboxMuteNodeType?.value || "*").trim() || "*",
    node_id: String(els.sandboxMuteNodeId?.value || "*").trim() || "*",
    code: String(els.sandboxMuteCode?.value || "*").trim() || "*",
    minutes: Number(els.sandboxMuteMinutes?.value || 60),
  });
  if (!out?.ok) {
    setStatus(`应用静默失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`已静默: ${out.key} 到 ${out.mute_until}`, true);
  await refreshSandboxRuleVersions();
  await refreshSandboxAlerts();
}

async function exportSandboxPreset() {
  const out = await window.aiwfDesktop.exportWorkflowSandboxPreset({
    preset: currentSandboxPresetPayload(),
  });
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导出预设失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`已导出预设: ${out.path}`, true);
}

async function importSandboxPreset() {
  const out = await window.aiwfDesktop.importWorkflowSandboxPreset({});
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导入预设失败: ${out?.error || "unknown"}`, false);
    return;
  }
  applySandboxPresetPayload(out.preset || {});
  await saveSandboxRules();
  await refreshSandboxAlerts();
  setStatus(`已导入预设: ${out.path}`, true);
}

async function refreshSandboxAutoFixLog() {
  const out = await window.aiwfDesktop.listWorkflowSandboxAutoFixActions({ limit: 120 });
  renderSandboxAutoFixRows(out?.items || []);
}

async function refreshAudit() {
  const out = await window.aiwfDesktop.listWorkflowAuditLogs({ limit: 120 });
  renderAuditRows(out?.items || []);
}

async function refreshReviewQueue() {
  try {
    const out = await window.aiwfDesktop.listManualReviews();
    renderReviewRows(out?.items || []);
  } catch {
    renderReviewRows([]);
  }
}



els.btnAdd.addEventListener("click", () => {
  const raw = String(els.nodeType.value || "").trim();
  if (!raw) {
    setStatus("节点类型不能为空", false);
    return;
  }
  const spec = resolveNodeCreateSpec(raw);
  const id = store.addNode(spec.nodeType, 60, 60, spec.config || undefined);
  canvas.setSelectedIds([id]);
  renderAll();
  setStatus(`已添加节点: ${spec.label || spec.nodeType}`, true);
});

els.btnReset.addEventListener("click", () => {
  store.reset();
  selectedEdge = null;
  els.workflowName.value = store.state.graph.name || "自由编排流程";
  renderAll();
  renderMigrationReport({ migrated: false });
  setStatus("已重置默认流程", true);
});

els.btnClear.addEventListener("click", () => {
  store.clear();
  selectedEdge = null;
  renderAll();
  renderMigrationReport({ migrated: false });
  setStatus("画布已清空", true);
});

if (els.btnPreflight) {
  els.btnPreflight.addEventListener("click", async () => {
    const out = await runWorkflowPreflight();
    if (out.ok) {
      const warns = (out.issues || []).filter((x) => String(x.level || "") === "warning").length;
      setStatus(warns > 0 ? `预检通过（${warns} 条警告）` : "预检通过", true);
    } else {
      const errs = (out.issues || []).filter((x) => String(x.level || "") === "error").length;
      setStatus(`预检失败（${errs} 条错误）`, false);
    }
  });
}
if (els.btnPreflightExport) els.btnPreflightExport.addEventListener("click", exportPreflightReport);
if (els.btnAutoFixGraph) {
  els.btnAutoFixGraph.addEventListener("click", async () => {
    const out = autoFixGraphStructure();
    lastAutoFixSummary = out;
    renderAutoFixDiff(out);
    if (!out.changed) {
      setStatus("未发现可自动修复的问题", true);
      return;
    }
    setStatus(
      `自动修复完成: 重复连线-${out.removed_dup_edges}, 自环-${out.removed_self_loops}, 断裂连线-${out.removed_broken_edges}, 孤立节点-${out.removed_isolated_nodes}`,
      true
    );
    await runWorkflowPreflight();
  });
}
els.btnRun.addEventListener("click", runWorkflow);
if (els.btnEnqueueRun) els.btnEnqueueRun.addEventListener("click", enqueueWorkflowRun);
if (els.btnQueueRefresh) els.btnQueueRefresh.addEventListener("click", refreshQueue);
if (els.btnQueuePause) els.btnQueuePause.addEventListener("click", pauseQueue);
if (els.btnQueueResume) els.btnQueueResume.addEventListener("click", resumeQueue);
if (els.btnVersionsRefresh) els.btnVersionsRefresh.addEventListener("click", refreshVersions);
if (els.btnCompareVersions) els.btnCompareVersions.addEventListener("click", compareVersions);
if (els.btnCacheRefresh) els.btnCacheRefresh.addEventListener("click", refreshCacheStats);
if (els.btnCacheClear) els.btnCacheClear.addEventListener("click", clearCache);
if (els.btnAppSchemaAdd) {
  els.btnAppSchemaAdd.addEventListener("click", () => {
    const existing = appSchemaRowsFromObject(collectAppSchemaFromForm());
    existing.push({ key: "", type: "string", required: false, defaultText: "", description: "" });
    renderAppSchemaForm(existing);
    syncAppSchemaJsonFromForm();
    syncRunParamsFormFromJson();
  });
}
if (els.btnAppSchemaSyncJson) els.btnAppSchemaSyncJson.addEventListener("click", () => {
  syncAppSchemaJsonFromForm();
  syncRunParamsFormFromJson();
  setStatus("参数 Schema 已同步到 JSON", true);
});
if (els.btnAppSchemaFromJson) els.btnAppSchemaFromJson.addEventListener("click", () => {
  syncAppSchemaFormFromJson();
  syncRunParamsFormFromJson();
  setStatus("已从 JSON 回填参数 Schema", true);
});
if (els.btnAppRunSyncJson) els.btnAppRunSyncJson.addEventListener("click", () => {
  syncRunParamsJsonFromForm();
  setStatus("运行参数已同步到 JSON", true);
});
if (els.btnAppRunFromJson) els.btnAppRunFromJson.addEventListener("click", () => {
  syncRunParamsFormFromJson();
  setStatus("已从 JSON 回填运行参数", true);
});
if (els.btnPublishApp) els.btnPublishApp.addEventListener("click", publishApp);
if (els.btnAppsRefresh) els.btnAppsRefresh.addEventListener("click", refreshApps);
if (els.btnTimelineRefresh) els.btnTimelineRefresh.addEventListener("click", refreshTimeline);
if (els.btnFailureSummaryRefresh) els.btnFailureSummaryRefresh.addEventListener("click", refreshFailureSummary);
if (els.btnSandboxAlertsRefresh) els.btnSandboxAlertsRefresh.addEventListener("click", refreshSandboxAlerts);
if (els.btnSandboxExport) els.btnSandboxExport.addEventListener("click", exportSandboxAudit);
if (els.btnSandboxRulesLoad) els.btnSandboxRulesLoad.addEventListener("click", loadSandboxRules);
if (els.btnSandboxRulesSave) els.btnSandboxRulesSave.addEventListener("click", saveSandboxRules);
if (els.btnSandboxPresetApply) els.btnSandboxPresetApply.addEventListener("click", applySandboxPreset);
if (els.btnSandboxMuteApply) els.btnSandboxMuteApply.addEventListener("click", applySandboxMute);
if (els.btnSandboxRuleVersions) els.btnSandboxRuleVersions.addEventListener("click", refreshSandboxRuleVersions);
if (els.btnSandboxPresetExport) els.btnSandboxPresetExport.addEventListener("click", exportSandboxPreset);
if (els.btnSandboxPresetImport) els.btnSandboxPresetImport.addEventListener("click", importSandboxPreset);
if (els.btnSandboxAutoFixLog) els.btnSandboxAutoFixLog.addEventListener("click", refreshSandboxAutoFixLog);
if (els.btnQualityGateRefresh) els.btnQualityGateRefresh.addEventListener("click", refreshQualityGateReports);
if (els.btnQualityGateExport) els.btnQualityGateExport.addEventListener("click", exportQualityGateReports);
if (els.qualityGateRunIdFilter) els.qualityGateRunIdFilter.addEventListener("change", refreshQualityGateReports);
if (els.qualityGateStatusFilter) els.qualityGateStatusFilter.addEventListener("change", refreshQualityGateReports);
if (els.qualityGateExportFormat) els.qualityGateExportFormat.addEventListener("change", saveQualityGatePrefs);
if (els.sandboxThresholdYellow) els.sandboxThresholdYellow.addEventListener("change", refreshSandboxAlerts);
if (els.sandboxThresholdRed) els.sandboxThresholdRed.addEventListener("change", refreshSandboxAlerts);
if (els.sandboxDedupWindowSec) els.sandboxDedupWindowSec.addEventListener("change", refreshSandboxAlerts);
if (els.btnAuditRefresh) els.btnAuditRefresh.addEventListener("click", refreshAudit);
els.btnDiagRefresh.addEventListener("click", refreshDiagnostics);
if (els.btnRunsRefresh) els.btnRunsRefresh.addEventListener("click", refreshRunHistory);
if (els.btnReviewsRefresh) els.btnReviewsRefresh.addEventListener("click", refreshReviewQueue);
if (els.btnReviewHistoryRefresh) els.btnReviewHistoryRefresh.addEventListener("click", refreshReviewHistory);
if (els.btnReviewHistoryExport) els.btnReviewHistoryExport.addEventListener("click", exportReviewHistory);
if (els.btnCompareRuns) els.btnCompareRuns.addEventListener("click", compareRuns);
if (els.btnSaveBaseline) els.btnSaveBaseline.addEventListener("click", saveCurrentRunAsBaseline);
if (els.btnCompareBaseline) els.btnCompareBaseline.addEventListener("click", compareWithLatestBaseline);
if (els.btnLoadLineage) els.btnLoadLineage.addEventListener("click", loadLineageForRunA);
if (els.btnExportCompareReport) els.btnExportCompareReport.addEventListener("click", exportCompareReport);
if (els.compareOnlyChanged) els.compareOnlyChanged.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
if (els.compareOnlyStatusChanged) els.compareOnlyStatusChanged.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
if (els.compareMinDelta) els.compareMinDelta.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
els.btnExport.addEventListener("click", exportJson);
els.btnSaveFlow.addEventListener("click", saveFlow);
els.btnLoadFlow.addEventListener("click", loadFlow);
if (els.btnApplyTemplate) els.btnApplyTemplate.addEventListener("click", applySelectedTemplate);
if (els.btnSaveTemplate) els.btnSaveTemplate.addEventListener("click", saveCurrentAsTemplate);
if (els.btnTemplatePackInstall) els.btnTemplatePackInstall.addEventListener("click", installTemplatePack);
if (els.btnTemplatePackRemove) els.btnTemplatePackRemove.addEventListener("click", removeTemplatePackByCurrentTemplate);
if (els.btnTemplatePackExport) els.btnTemplatePackExport.addEventListener("click", exportTemplatePackByCurrentTemplate);
if (els.btnTemplateAcceptance) els.btnTemplateAcceptance.addEventListener("click", runTemplateAcceptance);
if (els.btnTemplateAcceptanceExport) els.btnTemplateAcceptanceExport.addEventListener("click", exportTemplateAcceptanceReport);
if (els.templateSelect) els.templateSelect.addEventListener("change", renderTemplateParamsForm);
if (els.qualityRuleSetSelect) {
  els.qualityRuleSetSelect.addEventListener("change", () => {
    if (els.qualityRuleSetId) els.qualityRuleSetId.value = String(els.qualityRuleSetSelect.value || "");
  });
}
if (els.btnQualityRuleSetsRefresh) els.btnQualityRuleSetsRefresh.addEventListener("click", refreshQualityRuleSets);
if (els.btnQualityRuleSetSave) els.btnQualityRuleSetSave.addEventListener("click", saveQualityRuleSetFromGraph);
if (els.btnQualityRuleSetRemove) els.btnQualityRuleSetRemove.addEventListener("click", removeQualityRuleSetCurrent);
if (els.paletteMode) els.paletteMode.addEventListener("change", renderPalette);
if (els.paletteSearch) els.paletteSearch.addEventListener("input", renderPalette);
if (els.btnUseDeepSeek) {
  els.btnUseDeepSeek.addEventListener("click", () => {
    if (els.aiEndpoint) els.aiEndpoint.value = "https://api.deepseek.com/v1/chat/completions";
    if (els.aiModel) els.aiModel.value = "deepseek-chat";
    refreshOfflineBoundaryHint();
    setStatus("已填充 DeepSeek 接口参数（请确认 API Key）", true);
  });
}
if (els.aiEndpoint) els.aiEndpoint.addEventListener("input", refreshOfflineBoundaryHint);
if (els.btnAddInputMap) {
  els.btnAddInputMap.addEventListener("click", () => {
    const node = singleSelectedNode();
    if (!node) return;
    const cfg = node.config && typeof node.config === "object" ? { ...node.config } : {};
    const map = cfg.input_map && typeof cfg.input_map === "object" && !Array.isArray(cfg.input_map) ? { ...cfg.input_map } : {};
    let i = 1;
    while (Object.prototype.hasOwnProperty.call(map, `target_${i}`)) i += 1;
    map[`target_${i}`] = "$prev.ok";
    cfg.input_map = map;
    store.updateNodeConfig(node.id, cfg);
    renderNodeConfigEditor();
  });
}
if (els.btnAddOutputMap) {
  els.btnAddOutputMap.addEventListener("click", () => {
    const node = singleSelectedNode();
    if (!node) return;
    const cfg = node.config && typeof node.config === "object" ? { ...node.config } : {};
    const map = cfg.output_map && typeof cfg.output_map === "object" && !Array.isArray(cfg.output_map) ? { ...cfg.output_map } : {};
    let i = 1;
    while (Object.prototype.hasOwnProperty.call(map, `alias_${i}`)) i += 1;
    map[`alias_${i}`] = "ok";
    cfg.output_map = map;
    store.updateNodeConfig(node.id, cfg);
    renderNodeConfigEditor();
  });
}
els.snapGrid.addEventListener("change", () => renderAll());
els.btnZoomIn.addEventListener("click", () => setZoom(canvas.getZoom() + 0.1));
els.btnZoomOut.addEventListener("click", () => setZoom(canvas.getZoom() - 0.1));
els.btnZoomReset.addEventListener("click", () => setZoom(1));
if (els.btnFitCanvas) els.btnFitCanvas.addEventListener("click", () => fitCanvasToView());
function applyArrange(mode, label) {
  const out = canvas.alignSelected(mode);
  if (!out || !out.ok) return;
  if (Number(out.moved || 0) <= 0) {
    setStatus(`${label}: 节点已处于目标布局`, true);
    return;
  }
  setStatus(`${label}: 已调整 ${out.moved}/${out.total} 个节点`, true);
}

els.btnAlignLeft.addEventListener("click", () => applyArrange("left", "左对齐"));
els.btnAlignTop.addEventListener("click", () => applyArrange("top", "上对齐"));
els.btnDistributeH.addEventListener("click", () => applyArrange("hspace", "水平分布"));
els.btnDistributeV.addEventListener("click", () => applyArrange("vspace", "垂直分布"));
els.btnUnlinkSelected.addEventListener("click", () => {
  const ids = canvas.getSelectedIds();
  if (ids.length < 2) {
    setStatus("请先框选至少两个节点再取消连线", false);
    return;
  }
  const selected = new Set(ids);
  const before = store.state.graph.edges.length;
  store.state.graph.edges = store.state.graph.edges.filter((e) => !(selected.has(e.from) && selected.has(e.to)));
  const removed = before - store.state.graph.edges.length;
  if (removed > 0) {
    if (selectedEdge && !store.getEdge(selectedEdge.from, selectedEdge.to)) selectedEdge = null;
    renderAll();
    setStatus(`已取消 ${removed} 条框选节点连线`, true);
  } else {
    setStatus("框选节点之间不存在可取消的连线", false);
  }
});
els.btnApplyNodeCfg.addEventListener("click", () => {
  const node = singleSelectedNode();
  if (!node) {
    setStatus("请先选中 1 个节点", false);
    return;
  }
  try {
    const parsed =
      cfgViewMode === "json"
        ? parseNodeConfigText()
        : parseNodeConfigForm(node, node.config && typeof node.config === "object" ? node.config : {});
    const ok = store.updateNodeConfig(node.id, parsed);
    if (!ok) {
      setStatus("配置应用失败：节点不存在", false);
      return;
    }
    setStatus(`配置已应用: ${node.id}`, true);
    renderNodeConfigEditor();
  } catch (e) {
    setStatus(String(e?.message || e || "配置应用失败"), false);
  }
});
els.btnResetNodeCfg.addEventListener("click", () => {
  const node = singleSelectedNode();
  if (!node) {
    setStatus("请先选中 1 个节点", false);
    return;
  }
  const cfg = defaultNodeConfig(node.type);
  store.updateNodeConfig(node.id, cfg);
  renderNodeConfigEditor();
  setStatus(`已重置为默认配置: ${node.type}`, true);
});
els.btnFormatNodeCfg.addEventListener("click", () => {
  if (els.nodeConfig.disabled) return;
  try {
    const parsed = parseNodeConfigText();
    els.nodeConfig.value = prettyJson(parsed);
    const node = singleSelectedNode();
    if (node) renderNodeConfigForm(node, parsed);
  } catch (e) {
    setStatus(String(e?.message || e || "配置格式化失败"), false);
  }
});
els.btnCfgForm.addEventListener("click", () => setCfgMode("form"));
els.btnCfgJson.addEventListener("click", () => setCfgMode("json"));
if (els.btnApplyEdgeCfg) {
  els.btnApplyEdgeCfg.addEventListener("click", () => {
    if (!selectedEdge) {
      setStatus("请先点击一条连线", false);
      return;
    }
    try {
      const when = parseEdgeWhenText();
      const ok = store.updateEdgeWhen(selectedEdge.from, selectedEdge.to, when);
      if (!ok) {
        setStatus("连线不存在，可能已被删除", false);
        selectedEdge = null;
        renderEdgeConfigEditor();
        return;
      }
      selectedEdge = store.getEdge(selectedEdge.from, selectedEdge.to);
      applyEdgeWhenToBuilder(selectedEdge?.when);
      renderAll();
      setStatus(`已更新连线条件: ${selectedEdge.from} -> ${selectedEdge.to}`, true);
    } catch (e) {
      setStatus(String(e?.message || e || "连线条件更新失败"), false);
    }
  });
}
if (els.btnClearEdgeCfg) {
  els.btnClearEdgeCfg.addEventListener("click", () => {
    if (!selectedEdge) {
      setStatus("请先点击一条连线", false);
      return;
    }
    const ok = store.updateEdgeWhen(selectedEdge.from, selectedEdge.to, null);
    if (!ok) {
      setStatus("连线不存在，可能已被删除", false);
      selectedEdge = null;
      renderEdgeConfigEditor();
      return;
    }
    selectedEdge = store.getEdge(selectedEdge.from, selectedEdge.to);
    applyEdgeWhenToBuilder(selectedEdge?.when);
    renderAll();
    setStatus(`已清空连线条件: ${selectedEdge.from} -> ${selectedEdge.to}`, true);
  });
}
if (els.edgeWhenKind) {
  els.edgeWhenKind.addEventListener("change", () => {
    setEdgeWhenBuilderVisibility(els.edgeWhenKind.value);
    syncEdgeTextFromBuilder();
  });
}
if (els.edgeWhenBool) els.edgeWhenBool.addEventListener("change", syncEdgeTextFromBuilder);
if (els.edgeWhenPath) els.edgeWhenPath.addEventListener("input", syncEdgeTextFromBuilder);
if (els.edgeWhenField) els.edgeWhenField.addEventListener("input", syncEdgeTextFromBuilder);
if (els.edgeWhenOp) els.edgeWhenOp.addEventListener("change", syncEdgeTextFromBuilder);
if (els.edgeWhenValue) els.edgeWhenValue.addEventListener("input", syncEdgeTextFromBuilder);
if (els.edgeFieldHintSelect) {
  els.edgeFieldHintSelect.addEventListener("change", () => {
    const v = String(els.edgeFieldHintSelect.value || "").trim();
    if (!v || !els.edgeWhenField) return;
    els.edgeWhenField.value = v;
    syncEdgeTextFromBuilder();
  });
}
if (els.edgePathHintSelect) {
  els.edgePathHintSelect.addEventListener("change", () => {
    const v = String(els.edgePathHintSelect.value || "").trim();
    if (!v || !els.edgeWhenPath) return;
    els.edgeWhenPath.value = v;
    syncEdgeTextFromBuilder();
  });
}
if (els.btnBuildEdgeWhen) {
  els.btnBuildEdgeWhen.addEventListener("click", () => {
    try {
      syncEdgeTextFromBuilder();
      setStatus("已从可视化构造器生成 JSON 条件", true);
    } catch (e) {
      setStatus(String(e?.message || e || "生成条件失败"), false);
    }
  });
}
if (els.btnParseEdgeWhen) {
  els.btnParseEdgeWhen.addEventListener("click", () => {
    try {
      const when = parseEdgeWhenText();
      applyEdgeWhenToBuilder(when);
      setStatus("已将 JSON 条件回填到可视化构造器", true);
    } catch (e) {
      setStatus(String(e?.message || e || "JSON 回填失败"), false);
    }
  });
}
els.nodeConfigForm.addEventListener("input", () => {
  const node = singleSelectedNode();
  if (!node) return;
  try {
    const parsed = parseNodeConfigForm(node, node.config && typeof node.config === "object" ? node.config : {});
    els.nodeConfig.value = prettyJson(parsed);
  } catch {}
});
els.canvasWrap.addEventListener(
  "wheel",
  (evt) => {
    if (!evt.ctrlKey) return;
    evt.preventDefault();
    setZoom(canvas.getZoom() + (evt.deltaY < 0 ? 0.08 : -0.08), {
      clientX: evt.clientX,
      clientY: evt.clientY,
    });
  },
  { passive: false }
);

window.addEventListener("resize", () => {
  syncCanvasPanels();
}, { passive: true });

els.canvasWrap.addEventListener("dragover", (evt) => {
  evt.preventDefault();
  evt.dataTransfer.dropEffect = "copy";
});

els.canvasWrap.addEventListener("drop", (evt) => {
  evt.preventDefault();
  const raw = String(evt.dataTransfer.getData("text/plain") || "").trim();
  if (!raw) return;
  const spec = resolveNodeCreateSpec(raw);
  const snapEnabled = !!els.snapGrid.checked;
  const grid = 24;
  const world = canvas.clientToWorld(evt.clientX, evt.clientY);
  const rawX = world.x - 105;
  const rawY = world.y - 43;
  const x = snapEnabled ? Math.round(rawX / grid) * grid : rawX;
  const y = snapEnabled ? Math.round(rawY / grid) * grid : rawY;
  const id = store.addNode(spec.nodeType, x, y, spec.config || undefined);
  canvas.setSelectedIds([id]);
  renderAll();
  setStatus(`已拖入节点: ${spec.label || spec.nodeType}`, true);
});

renderPalette();
renderTemplateSelect();
refreshTemplateMarketplace().then(() => renderTemplateSelect()).catch(() => {});
refreshQualityRuleSets().catch(() => {});
setCfgMode("form");
setEdgeWhenBuilderVisibility(els.edgeWhenKind?.value || "none");
rebuildEdgeHints(null);
renderAll();
renderNodeRuns([]);
renderDiagRuns({});
renderRunHistoryRows([]);
renderQueueRows([]);
renderQueueControl({});
renderVersionRows([]);
renderVersionCompare({ ok: false, error: "暂无" });
renderReviewRows([]);
renderCacheStats({});
renderAppSchemaForm([
  { key: "title", type: "string", required: true, defaultText: "", description: "任务标题" },
]);
syncAppSchemaJsonFromForm();
renderRunParamsFormBySchema((() => {
  try { return JSON.parse(String(els.appSchemaJson?.value || "{}")); } catch { return {}; }
})(), {});
renderAppRows([]);
renderTimelineRows({ ok: false });
renderFailureRows({});
renderSandboxHealth({ level: "green", total: 0, thresholds: sandboxThresholdsPayload(), dedup_window_sec: sandboxDedupWindowSec(), suppressed: 0 });
renderSandboxRows({});
renderSandboxRuleVersionRows([]);
renderSandboxAutoFixRows([]);
renderQualityGateRows([]);
loadQualityGatePrefs();
renderAuditRows([]);
renderReviewHistoryRows([]);
renderMigrationReport({ migrated: false });
renderCompareResult({ ok: false, error: "暂无" });
renderPreflightReport({ ok: true, issues: [] });
renderAutoFixDiff(null);
refreshDiagnostics();
refreshRunHistory();
refreshQueue();
refreshVersions();
refreshCacheStats();
refreshApps();
refreshFailureSummary();
refreshSandboxAlerts();
refreshSandboxRuleVersions();
refreshSandboxAutoFixLog();
refreshQualityGateReports();
refreshAudit();
refreshReviewQueue();
refreshReviewHistory();
setStatus("就绪。可拖拽节点并连线后运行。", true);
