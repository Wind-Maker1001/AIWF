import { defaultNodeConfig, NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { WorkflowCanvas } from "./canvas.js";
import { getWorkflowElements } from "./elements.js";
import { createWorkflowDiagnosticsUi } from "./diagnostics-ui.js";
import { createWorkflowDiagnosticsPanelUi } from "./diagnostics-panel-ui.js";
import { createWorkflowPreflightUi } from "./preflight-ui.js";
import { createWorkflowPreflightActionsUi } from "./preflight-actions-ui.js";
import { createWorkflowPreflightControllerUi } from "./preflight-controller-ui.js";
import { migrateLoadedWorkflowGraph } from "./template-utils.js";
import { createWorkflowPanelsUi } from "./panels-ui.js";
import { createWorkflowTemplateUi } from "./template-ui.js";
import { createWorkflowConfigUi } from "./config-ui.js";
import { createWorkflowAppFormUi } from "./app-form-ui.js";
import { createWorkflowAppPublishUi } from "./app-publish-ui.js";
import { createWorkflowFlowIoUi } from "./flow-io-ui.js";
import { createWorkflowPaletteUi } from "./palette-ui.js";
import { createWorkflowConnectivityUi } from "./connectivity-ui.js";
import { createWorkflowCanvasViewUi } from "./canvas-view-ui.js";
import { createWorkflowGraphShellUi } from "./graph-shell-ui.js";
import { createWorkflowRunPayloadUi } from "./run-payload-ui.js";
import { createWorkflowRunControllerUi } from "./run-controller-ui.js";
import { setupWorkflowDebugApi } from "./debug-api-ui.js";
import { createWorkflowStatusUi } from "./status-ui.js";
import { createWorkflowSupportUi } from "./support-ui.js";
import { createWorkflowQualityGateUi } from "./quality-gate-ui.js";
import { createWorkflowSandboxUi } from "./sandbox-ui.js";
import { createWorkflowAuditUi } from "./audit-ui.js";
import { createWorkflowVersionCacheUi } from "./version-cache-ui.js";
import { createWorkflowRunQueueUi } from "./run-queue-ui.js";
import { createWorkflowReviewQueueUi } from "./review-queue-ui.js";
import { createWorkflowQualityRuleSetUi } from "./quality-rule-set-ui.js";

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
const { setStatus } = createWorkflowStatusUi(els);
const { renderNodeRuns, renderDiagRuns, fetchRustRuntimeStats } = createWorkflowDiagnosticsUi(els);
const { refreshDiagnostics } = createWorkflowDiagnosticsPanelUi({
  renderDiagRuns,
  fetchRustRuntimeStats,
});

let cfgViewMode = "form";
let selectedEdge = null;
let lastCompareResult = null;

let lastPreflightReport = null;
let lastAutoFixSummary = null;
let lastTemplateAcceptanceReport = null;
let renderMigrationReport = () => {};
const graphShellApi = {
  applyRestoredWorkflowGraph: () => {},
  resetWorkflow: () => {},
  clearWorkflow: () => {},
};

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

setupWorkflowDebugApi(window, {
  store,
  canvas,
  renderAll,
});

const { refreshOfflineBoundaryHint, applyDeepSeekDefaults } = createWorkflowConnectivityUi(els, {
  setStatus,
  exportGraph: () => store.exportGraph(),
});

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
  saveCurrentRunAsBaseline,
  compareWithLatestBaseline,
  loadLineageForRunA,
  renderCompareResult,
  currentSandboxPresetPayload,
  applySandboxPresetPayload,
} = createWorkflowSupportUi(els, {
  setStatus,
  getLastCompareResult: () => lastCompareResult,
  setLastCompareResult: (out) => { lastCompareResult = out; },
});

const {
  graphPayload,
  runPayload,
} = createWorkflowRunPayloadUi(els, {
  store,
  sandboxDedupWindowSec,
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
  renderMigrationReport: renderMigrationReportImpl,
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
  refreshRunHistory: () => refreshRunHistory(),
  refreshReviewQueue: () => refreshReviewQueue(),
  refreshReviewHistory,
  refreshQueue: () => refreshQueue(),
  refreshDiagnostics,
  refreshSandboxRuleVersions: () => refreshSandboxRuleVersions(),
  refreshSandboxAlerts: () => refreshSandboxAlerts(),
  applySandboxRulesToUi,
  applyRestoredGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
  renderSandboxHealth,
  normalizeAppSchemaObject,
  renderAppSchemaForm,
  appSchemaRowsFromObject,
  renderRunParamsFormBySchema,
  collectRunParamsForm,
  runPayload,
});

Object.assign(graphShellApi, createWorkflowGraphShellUi(els, {
  store,
  setStatus,
  renderAll,
  setSelectedEdge: (edge) => { selectedEdge = edge; },
  getResetWorkflowName: () => String(store.state.graph?.name || "自由编排流程"),
  renderMigrationReport: renderMigrationReportImpl,
}));
const {
  applyRestoredWorkflowGraph,
  resetWorkflow,
  clearWorkflow,
} = graphShellApi;

const {
  saveQualityGatePrefs,
  loadQualityGatePrefs,
  refreshQualityGateReports,
  exportQualityGateReports,
} = createWorkflowQualityGateUi(els, {
  setStatus,
  prefsStorageKey: QUALITY_GATE_PREFS_KEY,
  qualityGatePrefsPayload,
  qualityGateFilterPayload,
  renderQualityGateRows,
});

const {
  refreshSandboxAlerts,
  exportSandboxAudit,
  loadSandboxRules,
  refreshSandboxRuleVersions,
  saveSandboxRules,
  applySandboxPreset,
  applySandboxMute,
  exportSandboxPreset,
  importSandboxPreset,
  refreshSandboxAutoFixLog,
} = createWorkflowSandboxUi(els, {
  setStatus,
  sandboxThresholdsPayload,
  sandboxDedupWindowSec,
  sandboxRulesPayloadFromUi,
  applySandboxRulesToUi,
  applySandboxPresetToUi,
  currentSandboxPresetPayload,
  applySandboxPresetPayload,
  renderSandboxRows,
  renderSandboxRuleVersionRows,
  renderSandboxAutoFixRows,
});

const {
  refreshTimeline,
  refreshFailureSummary,
  refreshAudit,
} = createWorkflowAuditUi(els, {
  setStatus,
  renderTimelineRows,
  renderFailureRows,
  renderAuditRows,
});

const {
  refreshRunHistory,
  refreshQueue,
  pauseQueue,
  resumeQueue,
} = createWorkflowRunQueueUi({
  setStatus,
  renderRunHistoryRows,
  renderQueueRows,
  renderQueueControl,
});

const { refreshReviewQueue } = createWorkflowReviewQueueUi({
  renderReviewRows,
});

const {
  refreshVersions,
  compareVersions,
  refreshCacheStats,
  clearCache,
} = createWorkflowVersionCacheUi(els, {
  setStatus,
  renderVersionRows,
  renderVersionCompare,
  renderCacheStats,
});

const {
  handleQualityRuleSetSelectChange,
  refreshQualityRuleSets,
  saveQualityRuleSetFromGraph,
  removeQualityRuleSetCurrent,
} = createWorkflowQualityRuleSetUi(els, {
  setStatus,
  exportGraph: () => store.exportGraph(),
});

const {
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
});

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
  installTemplatePack,
  removeTemplatePackByCurrentTemplate,
  exportTemplatePackByCurrentTemplate,
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

const {
  exportJson,
  saveFlow,
  loadFlow,
} = createWorkflowFlowIoUi(els, {
  setStatus,
  graphPayload,
  refreshVersions: () => refreshVersions(),
  migrateLoadedWorkflowGraph,
  applyLoadedWorkflowGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
  getLoadedWorkflowName: () => String(store.state.graph?.name || ""),
  renderMigrationReport: renderMigrationReportImpl,
});

const {
  handleAddNode,
  renderPalette,
  handleCanvasDragOver,
  handleCanvasDrop,
} = createWorkflowPaletteUi(els, {
  setStatus,
  nodeCatalog: NODE_CATALOG,
  defaultNodeConfigFn: defaultNodeConfig,
  createNode: (type, x, y, config) => store.addNode(type, x, y, config),
  selectNodeIds: (ids) => canvas.setSelectedIds(ids),
  renderAll,
  computeDropPosition: (evt) => {
    const snapEnabled = !!els.snapGrid?.checked;
    const grid = 24;
    const world = canvas.clientToWorld(evt.clientX, evt.clientY);
    const rawX = world.x - 105;
    const rawY = world.y - 43;
    return {
      x: snapEnabled ? Math.round(rawX / grid) * grid : rawX,
      y: snapEnabled ? Math.round(rawY / grid) * grid : rawY,
    };
  },
});

const {
  syncCanvasPanels,
  setZoom,
  fitCanvasToView,
  applyArrange,
  focusNodeInCanvas,
} = createWorkflowCanvasViewUi(els, {
  canvas,
  setStatus,
  renderNodeConfigEditor: () => renderNodeConfigEditor(),
  renderEdgeConfigEditor: () => renderEdgeConfigEditor(),
  refreshOfflineBoundaryHint,
  getNode: (id) => store.getNode(id),
  selectNodeIds: (ids) => canvas.setSelectedIds(ids),
  renderAll,
});

const { computePreflightRisk, renderPreflightReport, renderAutoFixDiff } = createWorkflowPreflightUi(els, { focusNodeInCanvas });

const {
  autoFixGraphStructure,
  runWorkflowPreflight,
} = createWorkflowPreflightControllerUi(els, {
  graphPayload,
  exportGraph: () => store.exportGraph(),
  applyGraph: (graph) => graphShellApi.applyRestoredWorkflowGraph(graph),
  computePreflightRisk,
  renderPreflightReport,
  setLastPreflightReport: (report) => { lastPreflightReport = report; },
});

const {
  exportPreflightReport,
  runTemplateAcceptance,
  exportTemplateAcceptanceReport,
} = createWorkflowPreflightActionsUi(els, {
  setStatus,
  runWorkflowPreflight,
  allTemplates,
  currentTemplateGovernance,
  autoFixGraphStructure,
  renderAutoFixDiff,
  getLastPreflightReport: () => lastPreflightReport,
  getLastTemplateAcceptanceReport: () => lastTemplateAcceptanceReport,
  setLastAutoFixSummary: (summary) => { lastAutoFixSummary = summary; },
  setLastTemplateAcceptanceReport: (report) => { lastTemplateAcceptanceReport = report; },
});

const {
  handleAppSchemaAdd,
  handleAppSchemaSyncJson,
  handleAppSchemaFromJson,
  handleAppRunSyncJson,
  handleAppRunFromJson,
  publishApp,
  refreshApps,
} = createWorkflowAppPublishUi(els, {
  setStatus,
  graphPayload,
  runWorkflowPreflight,
  collectAppSchemaFromForm,
  normalizeAppSchemaObject,
  currentTemplateGovernance,
  parseRunParamsLoose,
  getLastPreflightReport: () => lastPreflightReport,
  getLastTemplateAcceptanceReport: () => lastTemplateAcceptanceReport,
  renderAppRows,
  appSchemaRowsFromObject,
  renderAppSchemaForm,
  syncAppSchemaJsonFromForm,
  syncAppSchemaFormFromJson,
  syncRunParamsJsonFromForm,
  syncRunParamsFormFromJson,
});

const {
  runWorkflow,
  enqueueWorkflowRun,
} = createWorkflowRunControllerUi(els, {
  setStatus,
  runWorkflowPreflight,
  runPayload,
  renderNodeRuns,
  refreshDiagnostics,
  refreshRunHistory,
  refreshReviewQueue,
  refreshQueue,
});


function renderAll() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.render();
  syncCanvasPanels();
}

els.btnAdd.addEventListener("click", handleAddNode);

els.btnReset.addEventListener("click", resetWorkflow);

els.btnClear.addEventListener("click", clearWorkflow);

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
if (els.btnAppSchemaAdd) els.btnAppSchemaAdd.addEventListener("click", handleAppSchemaAdd);
if (els.btnAppSchemaSyncJson) els.btnAppSchemaSyncJson.addEventListener("click", handleAppSchemaSyncJson);
if (els.btnAppSchemaFromJson) els.btnAppSchemaFromJson.addEventListener("click", handleAppSchemaFromJson);
if (els.btnAppRunSyncJson) els.btnAppRunSyncJson.addEventListener("click", handleAppRunSyncJson);
if (els.btnAppRunFromJson) els.btnAppRunFromJson.addEventListener("click", handleAppRunFromJson);
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
  els.qualityRuleSetSelect.addEventListener("change", handleQualityRuleSetSelectChange);
}
if (els.btnQualityRuleSetsRefresh) els.btnQualityRuleSetsRefresh.addEventListener("click", refreshQualityRuleSets);
if (els.btnQualityRuleSetSave) els.btnQualityRuleSetSave.addEventListener("click", saveQualityRuleSetFromGraph);
if (els.btnQualityRuleSetRemove) els.btnQualityRuleSetRemove.addEventListener("click", removeQualityRuleSetCurrent);
if (els.paletteMode) els.paletteMode.addEventListener("change", renderPalette);
if (els.paletteSearch) els.paletteSearch.addEventListener("input", renderPalette);
if (els.btnUseDeepSeek) els.btnUseDeepSeek.addEventListener("click", applyDeepSeekDefaults);
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

els.canvasWrap.addEventListener("dragover", handleCanvasDragOver);

els.canvasWrap.addEventListener("drop", handleCanvasDrop);

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
