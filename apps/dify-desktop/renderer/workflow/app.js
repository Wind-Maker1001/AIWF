import { defaultNodeConfig, NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { validateGraph } from "./graph.js";
import { WorkflowCanvas } from "./canvas.js";

const $ = (id) => document.getElementById(id);

const store = createWorkflowStore();
const QUALITY_GATE_PREFS_KEY = "aiwf.workflow.qualityGatePrefs.v1";

const els = {
  palette: $("palette"),
  nodeType: $("nodeType"),
  workflowName: $("workflowName"),
  templateSelect: $("templateSelect"),
  templateParamsForm: $("templateParamsForm"),
  templateParams: $("templateParams"),
  btnApplyTemplate: $("btnApplyTemplate"),
  btnSaveTemplate: $("btnSaveTemplate"),
  inputFiles: $("inputFiles"),
  reportTitle: $("reportTitle"),
  exportCanonicalBundle: $("exportCanonicalBundle"),
  canonicalTitle: $("canonicalTitle"),
  aiEndpoint: $("aiEndpoint"),
  aiKey: $("aiKey"),
  aiModel: $("aiModel"),
  rustEndpoint: $("rustEndpoint"),
  rustRequired: $("rustRequired"),
  chipletIsolationEnabled: $("chipletIsolationEnabled"),
  chipletIsolationMode: $("chipletIsolationMode"),
  chipletIsolatedTypes: $("chipletIsolatedTypes"),
  sandboxMaxDurationMs: $("sandboxMaxDurationMs"),
  sandboxMaxCpuMs: $("sandboxMaxCpuMs"),
  sandboxMaxRssMb: $("sandboxMaxRssMb"),
  sandboxMaxOutputBytes: $("sandboxMaxOutputBytes"),
  status: $("status"),
  nodeRuns: $("nodeRuns"),
  runHistoryRows: $("runHistoryRows"),
  queueRows: $("queueRows"),
  queueControlText: $("queueControlText"),
  versionRows: $("versionRows"),
  versionCompareA: $("versionCompareA"),
  versionCompareB: $("versionCompareB"),
  versionCompareSummary: $("versionCompareSummary"),
  versionCompareRows: $("versionCompareRows"),
  reviewRows: $("reviewRows"),
  cacheStatsText: $("cacheStatsText"),
  appPublishName: $("appPublishName"),
  appSchemaForm: $("appSchemaForm"),
  appSchemaJson: $("appSchemaJson"),
  appRunParamsForm: $("appRunParamsForm"),
  appRunParams: $("appRunParams"),
  appRows: $("appRows"),
  timelineRunId: $("timelineRunId"),
  timelineRows: $("timelineRows"),
  failureRows: $("failureRows"),
  sandboxThresholdYellow: $("sandboxThresholdYellow"),
  sandboxThresholdRed: $("sandboxThresholdRed"),
  sandboxDedupWindowSec: $("sandboxDedupWindowSec"),
  sandboxWhitelistCodes: $("sandboxWhitelistCodes"),
  sandboxWhitelistNodeTypes: $("sandboxWhitelistNodeTypes"),
  sandboxPreset: $("sandboxPreset"),
  sandboxAutoFixEnabled: $("sandboxAutoFixEnabled"),
  sandboxAutoFixPauseQueue: $("sandboxAutoFixPauseQueue"),
  sandboxAutoFixRequireReview: $("sandboxAutoFixRequireReview"),
  sandboxAutoFixForceIsolation: $("sandboxAutoFixForceIsolation"),
  sandboxAutoFixRedThreshold: $("sandboxAutoFixRedThreshold"),
  sandboxAutoFixWindowSec: $("sandboxAutoFixWindowSec"),
  sandboxAutoFixForceMinutes: $("sandboxAutoFixForceMinutes"),
  sandboxAutoFixForceMode: $("sandboxAutoFixForceMode"),
  sandboxMuteNodeType: $("sandboxMuteNodeType"),
  sandboxMuteNodeId: $("sandboxMuteNodeId"),
  sandboxMuteCode: $("sandboxMuteCode"),
  sandboxMuteMinutes: $("sandboxMuteMinutes"),
  sandboxHealthText: $("sandboxHealthText"),
  sandboxExportFormat: $("sandboxExportFormat"),
  sandboxRows: $("sandboxRows"),
  sandboxRuleVersionRows: $("sandboxRuleVersionRows"),
  sandboxAutoFixRows: $("sandboxAutoFixRows"),
  qualityGateRunIdFilter: $("qualityGateRunIdFilter"),
  qualityGateStatusFilter: $("qualityGateStatusFilter"),
  qualityGateExportFormat: $("qualityGateExportFormat"),
  qualityGateRows: $("qualityGateRows"),
  auditRows: $("auditRows"),
  diagRuns: $("diagRuns"),
  log: $("log"),
  canvasWrap: $("canvasWrap"),
  canvasSurface: $("canvasSurface"),
  nodesLayer: $("nodesLayer"),
  guideLayer: $("guideLayer"),
  minimap: $("minimap"),
  edges: $("edges"),
  btnAdd: $("btnAdd"),
  btnReset: $("btnReset"),
  btnClear: $("btnClear"),
  btnRun: $("btnRun"),
  btnEnqueueRun: $("btnEnqueueRun"),
  btnQueueRefresh: $("btnQueueRefresh"),
  btnQueuePause: $("btnQueuePause"),
  btnQueueResume: $("btnQueueResume"),
  btnVersionsRefresh: $("btnVersionsRefresh"),
  btnCompareVersions: $("btnCompareVersions"),
  btnCacheRefresh: $("btnCacheRefresh"),
  btnCacheClear: $("btnCacheClear"),
  btnAppSchemaAdd: $("btnAppSchemaAdd"),
  btnAppSchemaSyncJson: $("btnAppSchemaSyncJson"),
  btnAppSchemaFromJson: $("btnAppSchemaFromJson"),
  btnAppRunSyncJson: $("btnAppRunSyncJson"),
  btnAppRunFromJson: $("btnAppRunFromJson"),
  btnPublishApp: $("btnPublishApp"),
  btnAppsRefresh: $("btnAppsRefresh"),
  btnTimelineRefresh: $("btnTimelineRefresh"),
  btnFailureSummaryRefresh: $("btnFailureSummaryRefresh"),
  btnSandboxAlertsRefresh: $("btnSandboxAlertsRefresh"),
  btnSandboxExport: $("btnSandboxExport"),
  btnSandboxRulesLoad: $("btnSandboxRulesLoad"),
  btnSandboxRulesSave: $("btnSandboxRulesSave"),
  btnSandboxPresetApply: $("btnSandboxPresetApply"),
  btnSandboxMuteApply: $("btnSandboxMuteApply"),
  btnSandboxRuleVersions: $("btnSandboxRuleVersions"),
  btnSandboxPresetExport: $("btnSandboxPresetExport"),
  btnSandboxPresetImport: $("btnSandboxPresetImport"),
  btnSandboxAutoFixLog: $("btnSandboxAutoFixLog"),
  btnQualityGateRefresh: $("btnQualityGateRefresh"),
  btnQualityGateExport: $("btnQualityGateExport"),
  btnAuditRefresh: $("btnAuditRefresh"),
  btnDiagRefresh: $("btnDiagRefresh"),
  btnRunsRefresh: $("btnRunsRefresh"),
  btnReviewsRefresh: $("btnReviewsRefresh"),
  btnExport: $("btnExport"),
  btnSaveFlow: $("btnSaveFlow"),
  btnLoadFlow: $("btnLoadFlow"),
  snapGrid: $("snapGrid"),
  btnZoomOut: $("btnZoomOut"),
  btnZoomIn: $("btnZoomIn"),
  btnZoomReset: $("btnZoomReset"),
  zoomText: $("zoomText"),
  btnAlignLeft: $("btnAlignLeft"),
  btnAlignTop: $("btnAlignTop"),
  btnDistributeH: $("btnDistributeH"),
  btnDistributeV: $("btnDistributeV"),
  btnUnlinkSelected: $("btnUnlinkSelected"),
  selectedNodeInfo: $("selectedNodeInfo"),
  selectedEdgeInfo: $("selectedEdgeInfo"),
  edgeWhenKind: $("edgeWhenKind"),
  edgeWhenBoolWrap: $("edgeWhenBoolWrap"),
  edgeWhenBool: $("edgeWhenBool"),
  edgeWhenPathWrap: $("edgeWhenPathWrap"),
  edgeWhenPath: $("edgeWhenPath"),
  edgePathHints: $("edgePathHints"),
  edgePathHintSelect: $("edgePathHintSelect"),
  edgeWhenRuleWrap: $("edgeWhenRuleWrap"),
  edgeWhenField: $("edgeWhenField"),
  edgeFieldHints: $("edgeFieldHints"),
  edgeFieldHintSelect: $("edgeFieldHintSelect"),
  edgeWhenOp: $("edgeWhenOp"),
  edgeWhenValue: $("edgeWhenValue"),
  btnBuildEdgeWhen: $("btnBuildEdgeWhen"),
  btnParseEdgeWhen: $("btnParseEdgeWhen"),
  edgeWhenText: $("edgeWhenText"),
  btnApplyEdgeCfg: $("btnApplyEdgeCfg"),
  btnClearEdgeCfg: $("btnClearEdgeCfg"),
  nodeConfig: $("nodeConfig"),
  btnApplyNodeCfg: $("btnApplyNodeCfg"),
  btnResetNodeCfg: $("btnResetNodeCfg"),
  btnFormatNodeCfg: $("btnFormatNodeCfg"),
  btnCfgForm: $("btnCfgForm"),
  btnCfgJson: $("btnCfgJson"),
  nodeConfigForm: $("nodeConfigForm"),
  inputMapRows: $("inputMapRows"),
  outputMapRows: $("outputMapRows"),
  btnAddInputMap: $("btnAddInputMap"),
  btnAddOutputMap: $("btnAddOutputMap"),
  compareRunA: $("compareRunA"),
  compareRunB: $("compareRunB"),
  compareOnlyChanged: $("compareOnlyChanged"),
  compareOnlyStatusChanged: $("compareOnlyStatusChanged"),
  compareMinDelta: $("compareMinDelta"),
  compareReportFormat: $("compareReportFormat"),
  btnCompareRuns: $("btnCompareRuns"),
  btnExportCompareReport: $("btnExportCompareReport"),
  compareSummary: $("compareSummary"),
  compareRows: $("compareRows"),
  reviewHistoryRunId: $("reviewHistoryRunId"),
  reviewHistoryReviewer: $("reviewHistoryReviewer"),
  reviewHistoryStatus: $("reviewHistoryStatus"),
  reviewHistoryDateFrom: $("reviewHistoryDateFrom"),
  reviewHistoryDateTo: $("reviewHistoryDateTo"),
  btnReviewHistoryRefresh: $("btnReviewHistoryRefresh"),
  btnReviewHistoryExport: $("btnReviewHistoryExport"),
  reviewHistoryRows: $("reviewHistoryRows"),
  migrationSummary: $("migrationSummary"),
  migrationRows: $("migrationRows"),
};

let cfgViewMode = "form";
let selectedEdge = null;
let lastCompareResult = null;

const TEMPLATE_STORAGE_KEY = "aiwf.workflow.templates.v1";
const BUILTIN_TEMPLATES = [
  {
    id: "debate_evidence_v1",
    name: "辩论证据流水线 v1",
    params_schema: {
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "本次任务标题，用于流程名与熟肉标题。" },
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
      title: { type: "string", required: true, min_length: 1, group: "基础", description: "本次任务标题，用于流程名与熟肉标题。" },
      language: {
        type: "string",
        required: true,
        enum: ["zh-CN", "en-US"],
        default: "zh-CN",
        group: "AI参数",
        description: "AI整理输出的语言偏好。",
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
  office_slot_fill_v1: ["template_kind", "slots", "binding_path"],
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
    { key: "chart_source_node", label: "图表来源节点类型", type: "text" },
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

function renderPalette() {
  els.palette.innerHTML = "";
  NODE_CATALOG.forEach((n) => {
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

function loadLocalTemplates() {
  try {
    const raw = window.localStorage.getItem(TEMPLATE_STORAGE_KEY);
    if (!raw) return [];
    const arr = JSON.parse(raw);
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

function saveLocalTemplates(items) {
  try {
    window.localStorage.setItem(TEMPLATE_STORAGE_KEY, JSON.stringify(Array.isArray(items) ? items : []));
  } catch {}
}

function allTemplates() {
  return [...BUILTIN_TEMPLATES, ...loadLocalTemplates()];
}

function renderTemplateSelect() {
  if (!els.templateSelect) return;
  const items = allTemplates();
  els.templateSelect.innerHTML = '<option value="">选择模板...</option>';
  items.forEach((t) => {
    const op = document.createElement("option");
    op.value = String(t.id || "");
    op.textContent = String(t.name || t.id || "template");
    els.templateSelect.appendChild(op);
  });
  renderTemplateParamsForm();
}

function defaultTemplateParamValue(rule) {
  if (rule && Object.prototype.hasOwnProperty.call(rule, "default")) return rule.default;
  const t = String(rule?.type || "");
  if (t === "number") return 0;
  if (t === "boolean") return false;
  if (t === "object") return {};
  if (t === "array") return [];
  return "";
}

function matchesTemplateRuleCondition(cond, params) {
  if (!cond || typeof cond !== "object") return true;
  const key = String(cond.field || "").trim();
  if (!key) return true;
  const has = Object.prototype.hasOwnProperty.call(params || {}, key);
  const value = params ? params[key] : undefined;
  if (Object.prototype.hasOwnProperty.call(cond, "exists")) {
    if (!!cond.exists !== has) return false;
  }
  if (Object.prototype.hasOwnProperty.call(cond, "equals")) {
    if (value !== cond.equals) return false;
  }
  if (Object.prototype.hasOwnProperty.call(cond, "not_equals")) {
    if (value === cond.not_equals) return false;
  }
  if (Array.isArray(cond.in)) {
    if (!cond.in.some((x) => x === value)) return false;
  }
  if (Array.isArray(cond.not_in)) {
    if (cond.not_in.some((x) => x === value)) return false;
  }
  return true;
}

function resolveTemplateRule(rule, params) {
  const resolved = { ...(rule || {}) };
  if (rule && rule.depends_on) {
    const deps = Array.isArray(rule.depends_on) ? rule.depends_on : [rule.depends_on];
    resolved.__active = deps.every((d) => matchesTemplateRuleCondition(d, params));
  } else {
    resolved.__active = true;
  }
  if (Array.isArray(rule?.conditional)) {
    for (const c of rule.conditional) {
      if (!c || typeof c !== "object") continue;
      if (matchesTemplateRuleCondition(c.when || {}, params)) {
        Object.assign(resolved, c);
        break;
      }
    }
  }
  return resolved;
}

function readTemplateParamsLoose() {
  const text = String(els.templateParams?.value || "").trim();
  if (!text) return {};
  try {
    const obj = JSON.parse(text);
    if (obj && typeof obj === "object" && !Array.isArray(obj)) return obj;
  } catch {}
  return {};
}

function renderTemplateParamsForm() {
  if (!els.templateParamsForm) return;
  const id = String(els.templateSelect?.value || "").trim();
  const tpl = allTemplates().find((x) => String(x.id || "") === id);
  const schema = tpl?.params_schema;
  if (!schema || typeof schema !== "object" || !Object.keys(schema).length) {
    els.templateParamsForm.innerHTML = '<div class="cfg-help">当前模板无参数定义，可直接编辑 JSON。</div>';
    return;
  }
  const existing = readTemplateParamsLoose();
  const merged = {};
  Object.entries(schema).forEach(([key, rule]) => {
    if (Object.prototype.hasOwnProperty.call(existing, key)) merged[key] = existing[key];
    else merged[key] = defaultTemplateParamValue(rule);
  });
  if (els.templateParams) els.templateParams.value = JSON.stringify(merged, null, 2);
  els.templateParamsForm.innerHTML = "";
  const grouped = {};
  Object.entries(schema).forEach(([key, baseRule]) => {
    const rule = resolveTemplateRule(baseRule, merged);
    const g = String(rule?.group || baseRule?.group || "未分组");
    if (!grouped[g]) grouped[g] = [];
    grouped[g].push([key, baseRule, rule]);
  });
  Object.entries(grouped).forEach(([groupName, entries]) => {
    const box = document.createElement("div");
    box.style.border = "1px solid #dfe8f3";
    box.style.borderRadius = "8px";
    box.style.padding = "8px";
    box.style.marginBottom = "8px";
    const title = document.createElement("div");
    title.className = "cfg-help";
    title.style.fontWeight = "700";
    title.style.color = "#41566b";
    title.textContent = groupName;
    box.appendChild(title);
    entries.forEach(([key, baseRule, rule]) => {
      const wrap = document.createElement("div");
      wrap.className = "cfg-field";
      const label = document.createElement("label");
      const req = rule?.required ? " *" : "";
      label.textContent = `${key}${req}`;
      wrap.appendChild(label);
      if (rule?.description || baseRule?.description) {
        const desc = document.createElement("div");
        desc.className = "cfg-help";
        desc.style.marginTop = "-2px";
        desc.style.marginBottom = "5px";
        desc.dataset.tparamTip = key;
        desc.textContent = String(rule?.description || baseRule?.description || "");
        wrap.appendChild(desc);
      }
      const type = String(rule?.type || "string");
      let ctrl = null;
      if (Array.isArray(rule?.enum) && rule.enum.length) {
        ctrl = document.createElement("select");
        rule.enum.forEach((v) => {
          const op = document.createElement("option");
          op.value = String(v);
          op.textContent = String(v);
          ctrl.appendChild(op);
        });
        const current = merged[key] == null ? "" : String(merged[key]);
        if (current) ctrl.value = current;
      } else if (type === "boolean") {
        ctrl = document.createElement("select");
        ctrl.innerHTML = '<option value="true">true</option><option value="false">false</option>';
        ctrl.value = merged[key] ? "true" : "false";
      } else if (type === "object" || type === "array") {
        ctrl = document.createElement("textarea");
        ctrl.value = JSON.stringify(merged[key], null, 2);
        ctrl.style.minHeight = "66px";
      } else {
        ctrl = document.createElement("input");
        ctrl.type = type === "number" ? "number" : "text";
        if (type === "number") {
          if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
          if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
          if (Number.isFinite(rule?.step)) ctrl.step = String(rule.step);
        }
        ctrl.value = merged[key] == null ? "" : String(merged[key]);
      }
      ctrl.dataset.tparam = key;
      ctrl.dataset.ttype = String(baseRule?.type || type);
      ctrl.addEventListener("input", syncTemplateParamsJsonFromForm);
      wrap.appendChild(ctrl);
      box.appendChild(wrap);
    });
    els.templateParamsForm.appendChild(box);
  });
  applyTemplateDependencyState();
}

function collectTemplateParamsFromForm() {
  if (!els.templateParamsForm) return null;
  const controls = els.templateParamsForm.querySelectorAll("[data-tparam]");
  if (!controls.length) return null;
  const out = {};
  controls.forEach((ctrl) => {
    if (ctrl.disabled) return;
    const key = String(ctrl.dataset.tparam || "").trim();
    const type = String(ctrl.dataset.ttype || "string");
    if (!key) return;
    const raw = String(ctrl.value || "");
    if (type === "number") {
      const n = Number(raw);
      out[key] = Number.isFinite(n) ? n : raw;
      return;
    }
    if (type === "boolean") {
      out[key] = raw === "true";
      return;
    }
    if (type === "object" || type === "array") {
      try {
        out[key] = raw.trim() ? JSON.parse(raw) : (type === "array" ? [] : {});
      } catch {
        out[key] = raw;
      }
      return;
    }
    out[key] = raw;
  });
  return out;
}

function syncTemplateParamsJsonFromForm() {
  const params = collectTemplateParamsFromForm();
  if (!params || !els.templateParams) return;
  els.templateParams.value = JSON.stringify(params, null, 2);
  applyTemplateDependencyState();
}

function applyTemplateDependencyState() {
  if (!els.templateParamsForm) return;
  const id = String(els.templateSelect?.value || "").trim();
  if (!id) return;
  const tpl = allTemplates().find((x) => String(x.id || "") === id);
  const schema = tpl?.params_schema;
  if (!schema || typeof schema !== "object") return;
  const params = collectTemplateParamsFromForm() || readTemplateParamsLoose();
  const controls = els.templateParamsForm.querySelectorAll("[data-tparam]");
  controls.forEach((ctrl) => {
    const key = String(ctrl.dataset.tparam || "").trim();
    if (!key) return;
    const baseRule = schema[key];
    const rule = resolveTemplateRule(baseRule, params);
    const active = rule.__active !== false;
    ctrl.disabled = !active;
    if (String(ctrl.tagName || "").toLowerCase() === "input" && String(ctrl.type || "") === "number") {
      if (Number.isFinite(rule?.min)) ctrl.min = String(rule.min);
      else ctrl.removeAttribute("min");
      if (Number.isFinite(rule?.max)) ctrl.max = String(rule.max);
      else ctrl.removeAttribute("max");
    }
    const tip = els.templateParamsForm.querySelector(`[data-tparam-tip="${key}"]`);
    if (tip) {
      if (!active) tip.textContent = "该参数当前不生效（受联动条件控制）";
      else tip.textContent = String(rule?.description || baseRule?.description || "");
    }
  });
}

function applySelectedTemplate() {
  const id = String(els.templateSelect?.value || "").trim();
  if (!id) {
    setStatus("请先选择模板", false);
    return;
  }
  const it = allTemplates().find((x) => String(x.id || "") === id);
  if (!it || !it.graph) {
    setStatus("模板不存在", false);
    return;
  }
  let params = {};
  try {
    params = parseTemplateParams();
    validateTemplateParams(it.params_schema, params);
  } catch (e) {
    setStatus(String(e?.message || e || "模板参数错误"), false);
    return;
  }
  const graph = applyTemplateVars(it.graph, params);
  store.importGraph(graph);
  selectedEdge = null;
  els.workflowName.value = store.state.graph.name || String(it.name || "模板流程");
  renderAll();
  renderMigrationReport({ migrated: false });
  setStatus(`已应用模板: ${it.name || id}`, true);
}

function saveCurrentAsTemplate() {
  const name = String(prompt("请输入模板名称", String(els.workflowName?.value || "自定义模板")) || "").trim();
  if (!name) return;
  const custom = loadLocalTemplates();
  const id = `custom_${Date.now()}`;
  custom.push({
    id,
    name,
    graph: graphPayload(),
    created_at: new Date().toISOString(),
  });
  saveLocalTemplates(custom);
  renderTemplateSelect();
  if (els.templateSelect) els.templateSelect.value = id;
  setStatus(`模板已保存: ${name}`, true);
}

function parseTemplateParams() {
  const fromForm = collectTemplateParamsFromForm();
  if (fromForm) return fromForm;
  const text = String(els.templateParams?.value || "").trim();
  if (!text) return {};
  const obj = JSON.parse(text);
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) throw new Error("模板参数必须是 JSON 对象");
  return obj;
}

function validateTemplateParams(schema, params) {
  if (!schema || typeof schema !== "object") return;
  const errs = [];
  Object.entries(schema).forEach(([key, baseRule]) => {
    if (!baseRule || typeof baseRule !== "object") return;
    const rule = resolveTemplateRule(baseRule, params);
    if (rule.__active === false) return;
    const has = Object.prototype.hasOwnProperty.call(params, key);
    const val = params[key];
    if (rule.required && !has) {
      errs.push(`缺少参数: ${key}`);
      return;
    }
    if (!has) return;
    if (rule.type === "string") {
      if (typeof val !== "string") errs.push(`${key} 必须是字符串`);
      if (typeof val === "string" && Number.isFinite(rule.min_length) && val.length < Number(rule.min_length)) {
        errs.push(`${key} 长度不能小于 ${Number(rule.min_length)}`);
      }
    } else if (rule.type === "number") {
      if (typeof val !== "number" || !Number.isFinite(val)) {
        errs.push(`${key} 必须是数字`);
      } else {
        if (Number.isFinite(rule.min) && val < Number(rule.min)) errs.push(`${key} 不能小于 ${Number(rule.min)}`);
        if (Number.isFinite(rule.max) && val > Number(rule.max)) errs.push(`${key} 不能大于 ${Number(rule.max)}`);
      }
    } else if (rule.type === "boolean") {
      if (typeof val !== "boolean") errs.push(`${key} 必须是布尔值`);
    } else if (rule.type === "object") {
      if (!val || typeof val !== "object" || Array.isArray(val)) errs.push(`${key} 必须是对象`);
    } else if (rule.type === "array") {
      if (!Array.isArray(val)) errs.push(`${key} 必须是数组`);
    }
    if (Array.isArray(rule.enum) && rule.enum.length) {
      const ok = rule.enum.some((it) => it === val);
      if (!ok) errs.push(`${key} 必须是枚举值: ${rule.enum.join(", ")}`);
    }
  });
  if (errs.length) throw new Error(`模板参数校验失败: ${errs.join("; ")}`);
}

function applyTemplateVars(value, params) {
  if (Array.isArray(value)) return value.map((x) => applyTemplateVars(x, params));
  if (value && typeof value === "object") {
    const out = {};
    Object.entries(value).forEach(([k, v]) => {
      out[k] = applyTemplateVars(v, params);
    });
    return out;
  }
  if (typeof value !== "string") return value;
  const exact = value.match(/^\{\{\s*([A-Za-z0-9_]+)\s*\}\}$/);
  if (exact) {
    const key = exact[1];
    if (Object.prototype.hasOwnProperty.call(params, key)) return params[key];
    return value;
  }
  return value.replace(/\{\{\s*([A-Za-z0-9_]+)\s*\}\}/g, (_m, key) => {
    if (!Object.prototype.hasOwnProperty.call(params, key)) return `{{${key}}}`;
    return String(params[key]);
  });
}

function cloneJson(v) {
  return JSON.parse(JSON.stringify(v));
}

function previewValue(v) {
  if (v === undefined) return "(undefined)";
  if (typeof v === "string") return v;
  try {
    return JSON.stringify(v);
  } catch {
    return String(v);
  }
}

function migrateLoadedWorkflowGraph(graph) {
  const g = graph && typeof graph === "object" ? cloneJson(graph) : {};
  const notes = [];
  const changes = [];
  const pushChange = (path, before, after, reason) => {
    changes.push({ path, before: previewValue(before), after: previewValue(after), reason: String(reason || "") });
  };
  const wf = String(g.workflow_id || "");
  if (wf === "debate_evidence_v1") {
    pushChange("workflow_id", g.workflow_id, "debate_evidence_v2", "模板主版本升级");
    g.workflow_id = "debate_evidence_v2";
    pushChange("template_key", g.template_key, "debate_evidence", "补齐模板标识");
    g.template_key = "debate_evidence";
    pushChange("template_version", g.template_version, 2, "补齐模板版本");
    g.template_version = 2;
    const nodes = Array.isArray(g.nodes) ? g.nodes : [];
    nodes.forEach((n) => {
      if (String(n?.type || "") === "ai_refine") {
        const cfg = n.config && typeof n.config === "object" ? { ...n.config } : {};
        if (!cfg.language) {
          pushChange(`nodes.${String(n.id || "")}.config.language`, cfg.language, "zh-CN", "v2 新增语言参数，补默认值");
          cfg.language = "zh-CN";
        }
        n.config = cfg;
      }
    });
    notes.push("debate_evidence v1 -> v2");
  }
  if (wf === "finance_report_v1") {
    pushChange("workflow_id", g.workflow_id, "finance_report_v2", "模板主版本升级");
    g.workflow_id = "finance_report_v2";
    pushChange("template_key", g.template_key, "finance_report", "补齐模板标识");
    g.template_key = "finance_report";
    pushChange("template_version", g.template_version, 2, "补齐模板版本");
    g.template_version = 2;
    const nodes = Array.isArray(g.nodes) ? g.nodes : [];
    nodes.forEach((n) => {
      if (String(n?.type || "") === "anomaly_explain_v1") {
        const cfg = n.config && typeof n.config === "object" ? { ...n.config } : {};
        if (!cfg.score_field) {
          pushChange(`nodes.${String(n.id || "")}.config.score_field`, cfg.score_field, "anomaly_score", "v2 新增评分字段，补默认值");
          cfg.score_field = "anomaly_score";
        }
        if (!Number.isFinite(Number(cfg.threshold))) {
          pushChange(`nodes.${String(n.id || "")}.config.threshold`, cfg.threshold, 0.8, "阈值无效，修正为默认值");
          cfg.threshold = 0.8;
        }
        n.config = cfg;
      }
    });
    notes.push("finance_report v1 -> v2");
  }
  return { graph: g, migrated: notes.length > 0, notes, changes };
}

function renderMigrationReport(migrated) {
  if (!els.migrationSummary || !els.migrationRows) return;
  if (!migrated || !migrated.migrated) {
    els.migrationSummary.textContent = "暂无迁移";
    els.migrationRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
    return;
  }
  const changes = Array.isArray(migrated.changes) ? migrated.changes : [];
  els.migrationSummary.textContent = `已迁移: ${migrated.notes.join(", ")} | 变更 ${changes.length} 项`;
  if (!changes.length) {
    els.migrationRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">无逐字段变更</td></tr>';
    return;
  }
  els.migrationRows.innerHTML = "";
  changes.forEach((c) => {
    const tr = document.createElement("tr");
    const tdPath = document.createElement("td");
    const tdBefore = document.createElement("td");
    const tdAfter = document.createElement("td");
    const tdReason = document.createElement("td");
    tdPath.textContent = String(c.path || "");
    tdBefore.textContent = String(c.before || "");
    tdAfter.textContent = String(c.after || "");
    tdReason.textContent = String(c.reason || "");
    tr.append(tdPath, tdBefore, tdAfter, tdReason);
    els.migrationRows.appendChild(tr);
  });
}

function renderRunHistoryRows(items = []) {
  if (!els.runHistoryRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.runHistoryRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.runHistoryRows.innerHTML = "";
  items.slice(0, 30).forEach((it) => {
    const tr = document.createElement("tr");
    const tdRun = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdOp = document.createElement("td");
    tdRun.textContent = String(it.run_id || "").slice(0, 10);
    tdStatus.textContent = String(it.status || "");
    const nodeRuns = Array.isArray(it?.result?.node_runs) ? it.result.node_runs : [];
    const failedNodes = nodeRuns.filter((n) => String(n?.status || "") === "failed");
    const preferred = failedNodes.length ? failedNodes : nodeRuns;
    const select = document.createElement("select");
    select.style.maxWidth = "120px";
    select.style.marginRight = "4px";
    if (!preferred.length) {
      const op = document.createElement("option");
      op.value = "";
      op.textContent = "无节点";
      select.appendChild(op);
    } else {
      preferred.forEach((n) => {
        const op = document.createElement("option");
        op.value = String(n.id || "");
        op.textContent = `${String(n.id || "")}(${String(n.type || "")})`;
        select.appendChild(op);
      });
    }
    const btn = document.createElement("button");
    btn.className = "mini";
    btn.textContent = "续跑";
    const btnA = document.createElement("button");
    btnA.className = "mini";
    btnA.style.marginRight = "4px";
    btnA.textContent = "设为A";
    btnA.onclick = () => {
      if (els.compareRunA) els.compareRunA.value = String(it.run_id || "");
    };
    const btnB = document.createElement("button");
    btnB.className = "mini";
    btnB.style.marginRight = "4px";
    btnB.textContent = "设为B";
    btnB.onclick = () => {
      if (els.compareRunB) els.compareRunB.value = String(it.run_id || "");
    };
    btn.onclick = async () => {
      const nodeId = String(select.value || "").trim();
      if (!nodeId) return;
      const out = await window.aiwfDesktop.replayWorkflowRun({ run_id: it.run_id, node_id: nodeId }, {});
      els.log.textContent = JSON.stringify(out, null, 2);
      await refreshRunHistory();
      await refreshReviewQueue();
      setStatus(out?.ok ? `续跑完成: ${it.run_id}` : `续跑失败: ${out?.error || "unknown"}`, !!out?.ok);
    };
    tdOp.append(select, btnA, btnB, btn);
    tr.append(tdRun, tdStatus, tdOp);
    els.runHistoryRows.appendChild(tr);
  });
}

function renderQueueRows(items = []) {
  if (!els.queueRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.queueRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.queueRows.innerHTML = "";
  items.slice(0, 80).forEach((it) => {
    const tr = document.createElement("tr");
    const tdTask = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdOp = document.createElement("td");
    tdTask.textContent = `${String(it.label || "task")} (${String(it.task_id || "").slice(0, 8)})`;
    tdStatus.textContent = String(it.status || "");
    if (String(it.status || "") === "failed") tdStatus.style.color = "#b42318";
    if (String(it.status || "") === "done") tdStatus.style.color = "#087443";
    const cancelBtn = document.createElement("button");
    cancelBtn.className = "mini del";
    cancelBtn.textContent = "取消";
    cancelBtn.onclick = async () => {
      const out = await window.aiwfDesktop.cancelWorkflowTask({ task_id: it.task_id });
      setStatus(out?.ok ? "已取消任务" : `取消失败: ${out?.error || "unknown"}`, !!out?.ok);
      await refreshQueue();
    };
    const retryBtn = document.createElement("button");
    retryBtn.className = "mini";
    retryBtn.style.marginLeft = "4px";
    retryBtn.textContent = "重试";
    retryBtn.onclick = async () => {
      const out = await window.aiwfDesktop.retryWorkflowTask({ task_id: it.task_id });
      setStatus(out?.ok ? "已加入重试队列" : `重试失败: ${out?.error || "unknown"}`, !!out?.ok);
      await refreshQueue();
    };
    tdOp.append(cancelBtn, retryBtn);
    tr.append(tdTask, tdStatus, tdOp);
    els.queueRows.appendChild(tr);
  });
}

function renderQueueControl(control) {
  if (!els.queueControlText) return;
  const paused = !!control?.paused;
  const quotas = control?.quotas && typeof control.quotas === "object" ? control.quotas : {};
  const quotaText = Object.keys(quotas).length
    ? Object.entries(quotas).map(([k, v]) => `${k}:${v}`).join(", ")
    : "默认";
  els.queueControlText.textContent = `队列状态: ${paused ? "暂停" : "运行"} | 并发配额: ${quotaText}`;
}

function renderVersionRows(items = []) {
  if (!els.versionRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.versionRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.versionRows.innerHTML = "";
  items.slice(0, 80).forEach((it) => {
    const tr = document.createElement("tr");
    const tdName = document.createElement("td");
    const tdTs = document.createElement("td");
    const tdOp = document.createElement("td");
    tdName.textContent = `${String(it.workflow_name || "")} (${String(it.version_id || "").slice(0, 8)})`;
    tdTs.textContent = String(it.ts || "");
    const restoreBtn = document.createElement("button");
    restoreBtn.className = "mini";
    restoreBtn.textContent = "恢复";
    restoreBtn.onclick = async () => {
      const out = await window.aiwfDesktop.restoreWorkflowVersion({ version_id: it.version_id });
      if (!out?.ok) {
        setStatus(`恢复版本失败: ${out?.error || "unknown"}`, false);
        return;
      }
      store.importGraph(out.graph || {});
      selectedEdge = null;
      renderAll();
      setStatus(`已恢复版本: ${String(it.version_id || "").slice(0, 8)}`, true);
    };
    const setABtn = document.createElement("button");
    setABtn.className = "mini";
    setABtn.style.marginLeft = "4px";
    setABtn.textContent = "设A";
    setABtn.onclick = () => {
      if (els.versionCompareA) els.versionCompareA.value = String(it.version_id || "");
    };
    const setBBtn = document.createElement("button");
    setBBtn.className = "mini";
    setBBtn.style.marginLeft = "4px";
    setBBtn.textContent = "设B";
    setBBtn.onclick = () => {
      if (els.versionCompareB) els.versionCompareB.value = String(it.version_id || "");
    };
    tdOp.append(restoreBtn, setABtn, setBBtn);
    tr.append(tdName, tdTs, tdOp);
    els.versionRows.appendChild(tr);
  });
}

function renderVersionCompare(out) {
  if (!els.versionCompareSummary || !els.versionCompareRows) return;
  if (!out?.ok) {
    els.versionCompareSummary.textContent = `版本对比失败: ${out?.error || "unknown"}`;
    els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  const s = out.summary || {};
  els.versionCompareSummary.textContent = `A:${String(s.version_a || "").slice(0, 8)} B:${String(s.version_b || "").slice(0, 8)} 节点变化:${Number(s.changed_nodes || 0)} 边新增:${Number(s.added_edges || 0)} 边删除:${Number(s.removed_edges || 0)}`;
  const rows = Array.isArray(out.node_diff) ? out.node_diff.filter((x) => String(x.change || "") !== "same") : [];
  if (!rows.length) {
    els.versionCompareRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无节点变化</td></tr>';
    return;
  }
  els.versionCompareRows.innerHTML = "";
  rows.slice(0, 80).forEach((r) => {
    const tr = document.createElement("tr");
    const tdId = document.createElement("td");
    const tdChange = document.createElement("td");
    const tdType = document.createElement("td");
    tdId.textContent = String(r.id || "");
    tdChange.textContent = String(r.change || "");
    tdType.textContent = `${String(r.type_a || "-")} / ${String(r.type_b || "-")}`;
    if (String(r.change || "") === "removed") tdChange.style.color = "#b42318";
    if (String(r.change || "") === "added") tdChange.style.color = "#087443";
    tr.append(tdId, tdChange, tdType);
    els.versionCompareRows.appendChild(tr);
  });
}

function renderCacheStats(stats) {
  if (!els.cacheStatsText) return;
  if (!stats || typeof stats !== "object") {
    els.cacheStatsText.textContent = "缓存状态: -";
    return;
  }
  els.cacheStatsText.textContent = `缓存项:${Number(stats.entries || 0)} 命中:${Number(stats.hits || 0)} 未命中:${Number(stats.misses || 0)} 命中率:${Number(stats.hit_rate || 0)}`;
}

function renderAppRows(items = []) {
  if (!els.appRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.appRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.appRows.innerHTML = "";
  items.slice(0, 80).forEach((it) => {
    const tr = document.createElement("tr");
    const tdName = document.createElement("td");
    const tdTs = document.createElement("td");
    const tdOp = document.createElement("td");
    tdName.textContent = `${String(it.name || "")} (${String(it.app_id || "").slice(0, 8)})`;
    tdTs.textContent = String(it.updated_at || it.created_at || "");
    const runBtn = document.createElement("button");
    runBtn.className = "mini";
    runBtn.textContent = "运行";
    runBtn.onclick = async () => {
      renderRunParamsFormBySchema(it?.params_schema || {}, collectRunParamsForm());
      let params = collectRunParamsForm();
      if (!params || !Object.keys(params).length) {
        try { params = JSON.parse(String(els.appRunParams?.value || "{}") || "{}"); } catch { params = {}; }
      }
      const out = await window.aiwfDesktop.runWorkflowApp({
        app_id: it.app_id,
        params,
        payload: runPayload(),
      }, {});
      if (out?.result) {
        els.log.textContent = JSON.stringify(out.result, null, 2);
        if (els.timelineRunId) els.timelineRunId.value = String(out.result.run_id || "");
      }
      setStatus(out?.ok ? "应用运行完成" : `应用运行失败: ${out?.error || "unknown"}`, !!out?.ok);
      await refreshRunHistory();
      await refreshDiagnostics();
    };
    const cfgBtn = document.createElement("button");
    cfgBtn.className = "mini";
    cfgBtn.style.marginLeft = "4px";
    cfgBtn.textContent = "载入参数";
    cfgBtn.onclick = () => {
      const schemaObj = normalizeAppSchemaObject(it?.params_schema || {});
      if (els.appSchemaJson) els.appSchemaJson.value = JSON.stringify(schemaObj, null, 2);
      renderAppSchemaForm(appSchemaRowsFromObject(schemaObj));
      renderRunParamsFormBySchema(schemaObj, (() => {
        try { return JSON.parse(String(els.appRunParams?.value || "{}")); } catch { return {}; }
      })());
      setStatus(`已载入应用参数: ${String(it.name || "").trim() || String(it.app_id || "").slice(0, 8)}`, true);
    };
    tdOp.append(runBtn, cfgBtn);
    tr.append(tdName, tdTs, tdOp);
    els.appRows.appendChild(tr);
  });
}

function renderTimelineRows(out) {
  if (!els.timelineRows) return;
  if (!out?.ok) {
    els.timelineRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  const rows = Array.isArray(out.timeline) ? out.timeline : [];
  if (!rows.length) {
    els.timelineRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.timelineRows.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    const tdNode = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdSec = document.createElement("td");
    tdNode.textContent = `${String(r.node_id || "")}(${String(r.type || "")})`;
    tdStatus.textContent = String(r.status || "");
    tdSec.textContent = Number(r.seconds || 0).toFixed(3);
    tr.append(tdNode, tdStatus, tdSec);
    els.timelineRows.appendChild(tr);
  });
}

function renderFailureRows(out) {
  if (!els.failureRows) return;
  const byNode = out?.by_node && typeof out.by_node === "object" ? out.by_node : {};
  const entries = Object.entries(byNode);
  if (!entries.length) {
    els.failureRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.failureRows.innerHTML = "";
  entries.sort((a, b) => Number(b[1]?.failed || 0) - Number(a[1]?.failed || 0)).slice(0, 50).forEach(([k, v]) => {
    const tr = document.createElement("tr");
    const tdNode = document.createElement("td");
    const tdCnt = document.createElement("td");
    const tdErr = document.createElement("td");
    tdNode.textContent = String(k);
    tdCnt.textContent = String(v?.failed || 0);
    tdErr.textContent = Array.isArray(v?.samples) ? String(v.samples[0] || "") : "";
    tr.append(tdNode, tdCnt, tdErr);
    els.failureRows.appendChild(tr);
  });
}

function renderSandboxRows(out) {
  if (!els.sandboxRows) return;
  renderSandboxHealth(out?.health || {});
  const rows = Array.isArray(out?.by_node) ? out.by_node : [];
  if (!rows.length) {
    els.sandboxRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.sandboxRows.innerHTML = "";
  rows.slice(0, 50).forEach((r) => {
    const tr = document.createElement("tr");
    const tdNode = document.createElement("td");
    const tdCount = document.createElement("td");
    const tdRun = document.createElement("td");
    const tdOp = document.createElement("td");
    tdNode.textContent = `${String(r.node_type || "")}(${String(r.node_id || "")})`;
    tdCount.textContent = String(r.count || 0);
    tdRun.textContent = String(r.last_run_id || "").slice(0, 12);
    const muteBtn = document.createElement("button");
    muteBtn.className = "mini";
    muteBtn.textContent = "静默60m";
    muteBtn.onclick = async () => {
      const outMute = await window.aiwfDesktop.muteWorkflowSandboxAlert({
        node_type: String(r.node_type || "*"),
        node_id: String(r.node_id || "*"),
        code: "*",
        minutes: 60,
      });
      if (!outMute?.ok) {
        setStatus(`静默失败: ${outMute?.error || "unknown"}`, false);
        return;
      }
      setStatus(`已静默 ${String(r.node_type || "")}/${String(r.node_id || "")} 60分钟`, true);
      await refreshSandboxAlerts();
    };
    tdOp.appendChild(muteBtn);
    tr.append(tdNode, tdCount, tdRun, tdOp);
    els.sandboxRows.appendChild(tr);
  });
}

function renderSandboxRuleVersionRows(items = []) {
  if (!els.sandboxRuleVersionRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.sandboxRuleVersionRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.sandboxRuleVersionRows.innerHTML = "";
  items.slice(0, 60).forEach((it) => {
    const tr = document.createElement("tr");
    const tdVer = document.createElement("td");
    const tdTs = document.createElement("td");
    const tdOp = document.createElement("td");
    const vid = String(it.version_id || "");
    tdVer.textContent = vid.slice(0, 10);
    tdTs.textContent = String(it.ts || "");
    const rollbackBtn = document.createElement("button");
    rollbackBtn.className = "mini";
    rollbackBtn.textContent = "回滚";
    rollbackBtn.onclick = async () => {
      const out = await window.aiwfDesktop.rollbackWorkflowSandboxRuleVersion({ version_id: vid });
      if (!out?.ok) {
        setStatus(`回滚规则失败: ${out?.error || "unknown"}`, false);
        return;
      }
      applySandboxRulesToUi(out.rules || {});
      setStatus(`已回滚规则版本: ${vid.slice(0, 10)}`, true);
      await refreshSandboxRuleVersions();
      await refreshSandboxAlerts();
    };
    tdOp.appendChild(rollbackBtn);
    tr.append(tdVer, tdTs, tdOp);
    els.sandboxRuleVersionRows.appendChild(tr);
  });
}

function renderSandboxAutoFixRows(items = []) {
  if (!els.sandboxAutoFixRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.sandboxAutoFixRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.sandboxAutoFixRows.innerHTML = "";
  items.slice(0, 80).forEach((it) => {
    const tr = document.createElement("tr");
    const tdTs = document.createElement("td");
    const tdCnt = document.createElement("td");
    const tdAct = document.createElement("td");
    tdTs.textContent = String(it.ts || "");
    tdCnt.textContent = String(it.count || 0);
    tdAct.textContent = Array.isArray(it.actions) ? it.actions.join(",") : "";
    tr.append(tdTs, tdCnt, tdAct);
    els.sandboxAutoFixRows.appendChild(tr);
  });
}

function renderQualityGateRows(items = []) {
  if (!els.qualityGateRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.qualityGateRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.qualityGateRows.innerHTML = "";
  items.slice(0, 100).forEach((it) => {
    const tr = document.createElement("tr");
    const tdRun = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdIssues = document.createElement("td");
    const tdTs = document.createElement("td");
    tdRun.textContent = String(it.run_id || "").slice(0, 12);
    const gate = it.quality_gate && typeof it.quality_gate === "object" ? it.quality_gate : {};
    const status = gate.blocked ? "blocked" : (gate.passed ? "pass" : "unknown");
    tdStatus.textContent = status;
    tdStatus.style.color = status === "pass" ? "#087443" : (status === "blocked" ? "#b42318" : "#5c6b7a");
    tdIssues.textContent = Array.isArray(gate.issues) ? gate.issues.join(",") : "";
    tdTs.textContent = String(it.ts || "");
    tr.append(tdRun, tdStatus, tdIssues, tdTs);
    els.qualityGateRows.appendChild(tr);
  });
}

function qualityGateFilterPayload() {
  return {
    run_id: String(els.qualityGateRunIdFilter?.value || "").trim(),
    status: String(els.qualityGateStatusFilter?.value || "all").trim().toLowerCase() || "all",
  };
}

function qualityGatePrefsPayload() {
  const filter = qualityGateFilterPayload();
  const format = String(els.qualityGateExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md";
  return { filter, format };
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

function sandboxThresholdsPayload() {
  const y = Number(els.sandboxThresholdYellow?.value || 1);
  const r = Number(els.sandboxThresholdRed?.value || 3);
  const yellow = Number.isFinite(y) ? Math.max(1, Math.floor(y)) : 1;
  const red = Number.isFinite(r) ? Math.max(yellow + 1, Math.floor(r)) : Math.max(3, yellow + 1);
  return { yellow, red };
}

function sandboxDedupWindowSec() {
  const n = Number(els.sandboxDedupWindowSec?.value || 600);
  return Number.isFinite(n) ? Math.max(0, Math.floor(n)) : 600;
}

function parseCsvList(text) {
  return String(text || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim().toLowerCase())
    .filter(Boolean);
}

const SANDBOX_RULE_PRESETS = {
  strict: {
    yellow: 1,
    red: 2,
    dedup_window_sec: 60,
    whitelist_codes: [],
    whitelist_node_types: [],
  },
  balanced: {
    yellow: 1,
    red: 3,
    dedup_window_sec: 600,
    whitelist_codes: [],
    whitelist_node_types: [],
  },
  loose: {
    yellow: 3,
    red: 8,
    dedup_window_sec: 1800,
    whitelist_codes: ["sandbox_limit_exceeded:output"],
    whitelist_node_types: [],
  },
};

function sandboxRulesPayloadFromUi() {
  return {
    whitelist_codes: parseCsvList(els.sandboxWhitelistCodes?.value || ""),
    whitelist_node_types: parseCsvList(els.sandboxWhitelistNodeTypes?.value || ""),
    whitelist_keys: [],
    mute_until_by_key: {},
  };
}

function applySandboxRulesToUi(rules) {
  const r = rules && typeof rules === "object" ? rules : {};
  if (els.sandboxWhitelistCodes) {
    const codes = Array.isArray(r.whitelist_codes) ? r.whitelist_codes : [];
    els.sandboxWhitelistCodes.value = codes.join(",");
  }
  if (els.sandboxWhitelistNodeTypes) {
    const types = Array.isArray(r.whitelist_node_types) ? r.whitelist_node_types : [];
    els.sandboxWhitelistNodeTypes.value = types.join(",");
  }
}

function applySandboxPresetToUi(name) {
  const key = String(name || "balanced").trim().toLowerCase();
  const p = SANDBOX_RULE_PRESETS[key] || SANDBOX_RULE_PRESETS.balanced;
  if (els.sandboxThresholdYellow) els.sandboxThresholdYellow.value = String(p.yellow);
  if (els.sandboxThresholdRed) els.sandboxThresholdRed.value = String(p.red);
  if (els.sandboxDedupWindowSec) els.sandboxDedupWindowSec.value = String(p.dedup_window_sec);
  if (els.sandboxWhitelistCodes) els.sandboxWhitelistCodes.value = (p.whitelist_codes || []).join(",");
  if (els.sandboxWhitelistNodeTypes) els.sandboxWhitelistNodeTypes.value = (p.whitelist_node_types || []).join(",");
}

function renderSandboxHealth(health) {
  if (!els.sandboxHealthText) return;
  const level = String(health?.level || "green");
  const total = Number(health?.total || 0);
  const y = Number(health?.thresholds?.yellow || sandboxThresholdsPayload().yellow);
  const r = Number(health?.thresholds?.red || sandboxThresholdsPayload().red);
  const dedup = Number(health?.dedup_window_sec || sandboxDedupWindowSec());
  const suppressed = Number(health?.suppressed || 0);
  els.sandboxHealthText.textContent = `Sandbox状态: ${level.toUpperCase()} | 告警:${total} | 抑制:${suppressed} | 阈值 y=${y}, r=${r} | 去重窗=${dedup}s`;
  els.sandboxHealthText.style.color = level === "red" ? "#b42318" : (level === "yellow" ? "#b54708" : "#087443");
}

function renderAuditRows(items = []) {
  if (!els.auditRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.auditRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.auditRows.innerHTML = "";
  items.slice(0, 100).forEach((it) => {
    const tr = document.createElement("tr");
    const tdTs = document.createElement("td");
    const tdAction = document.createElement("td");
    const tdDetail = document.createElement("td");
    tdTs.textContent = String(it.ts || "");
    tdAction.textContent = String(it.action || "");
    tdDetail.textContent = JSON.stringify(it.detail || {});
    tr.append(tdTs, tdAction, tdDetail);
    els.auditRows.appendChild(tr);
  });
}

function renderReviewRows(items = []) {
  if (!els.reviewRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.reviewRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.reviewRows.innerHTML = "";
  items.slice(0, 30).forEach((it) => {
    const tr = document.createElement("tr");
    const tdKey = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdOp = document.createElement("td");
    tdKey.textContent = `${String(it.review_key || "")} (${String(it.run_id || "").slice(0, 8)})`;
    tdStatus.textContent = String(it.status || "pending");
    const okBtn = document.createElement("button");
    okBtn.className = "mini";
    okBtn.textContent = "批准";
    okBtn.onclick = async () => {
      const reviewer = String(prompt("审核人", String(it.reviewer || "reviewer")) || "").trim();
      const comment = String(prompt("备注", "") || "").trim();
      const out = await window.aiwfDesktop.submitManualReview({
        run_id: it.run_id,
        review_key: it.review_key,
        approved: true,
        reviewer,
        comment,
        auto_resume: true,
      });
      if (out?.resumed) els.log.textContent = JSON.stringify(out.resumed, null, 2);
      setStatus(out?.ok ? "审核已批准并自动续跑" : `审核失败: ${out?.error || "unknown"}`, !!out?.ok);
      await refreshReviewQueue();
      await refreshRunHistory();
      await refreshReviewHistory();
    };
    const noBtn = document.createElement("button");
    noBtn.className = "mini del";
    noBtn.style.marginLeft = "4px";
    noBtn.textContent = "驳回";
    noBtn.onclick = async () => {
      const reviewer = String(prompt("审核人", String(it.reviewer || "reviewer")) || "").trim();
      const comment = String(prompt("备注", "") || "").trim();
      const out = await window.aiwfDesktop.submitManualReview({
        run_id: it.run_id,
        review_key: it.review_key,
        approved: false,
        reviewer,
        comment,
        auto_resume: true,
      });
      if (out?.resumed) els.log.textContent = JSON.stringify(out.resumed, null, 2);
      setStatus(out?.ok ? "审核已驳回并自动续跑" : `审核失败: ${out?.error || "unknown"}`, !!out?.ok);
      await refreshReviewQueue();
      await refreshRunHistory();
      await refreshReviewHistory();
    };
    tdOp.append(okBtn, noBtn);
    tr.append(tdKey, tdStatus, tdOp);
    els.reviewRows.appendChild(tr);
  });
}

function renderNodeRuns(nodeRuns) {
  if (!Array.isArray(nodeRuns) || nodeRuns.length === 0) {
    els.nodeRuns.innerHTML = '<tr><td colspan="3" style="color:#74879b">未运行</td></tr>';
    return;
  }
  els.nodeRuns.innerHTML = "";
  nodeRuns.forEach((n) => {
    const tr = document.createElement("tr");
    const tdType = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdSec = document.createElement("td");
    const sec = Number.isFinite(Number(n.seconds)) ? `${Number(n.seconds).toFixed(3)}s` : "-";
    tdType.textContent = String(n.type || "");
    tdStatus.textContent = String(n.status || "");
    tdSec.textContent = sec;
    tr.append(tdType, tdStatus, tdSec);
    els.nodeRuns.appendChild(tr);
  });
}

function renderDiagRuns(summary, rustStats) {
  const by = summary && typeof summary === "object" ? summary.by_chiplet : null;
  const entries = by && typeof by === "object" ? Object.entries(by) : [];
  const rustEntries = Array.isArray(rustStats?.items) ? rustStats.items : [];
  if (!entries.length) {
    els.diagRuns.innerHTML = "";
  } else {
    els.diagRuns.innerHTML = "";
    entries
      .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
      .forEach(([chiplet, item]) => {
        const tr = document.createElement("tr");
        const tdType = document.createElement("td");
        const tdFail = document.createElement("td");
        const tdSec = document.createElement("td");
        const fr = Number(item.failure_rate || 0) * 100;
        tdType.textContent = chiplet;
        tdFail.textContent = `${fr.toFixed(1)}%`;
        tdSec.textContent = `${Number(item.seconds_avg || 0).toFixed(3)}s`;
        tr.append(tdType, tdFail, tdSec);
        els.diagRuns.appendChild(tr);
      });
  }
  rustEntries
    .sort((a, b) => String(a.operator || "").localeCompare(String(b.operator || "")))
    .forEach((item) => {
      const tr = document.createElement("tr");
      const tdType = document.createElement("td");
      const tdFail = document.createElement("td");
      const tdSec = document.createElement("td");
      const calls = Number(item.calls || 0);
      const errs = Number(item.err || 0);
      const fr = calls > 0 ? (errs / calls) * 100 : 0;
      tdType.textContent = `rust:${String(item.operator || "")}`;
      tdFail.textContent = `${fr.toFixed(1)}%`;
      tdSec.textContent = `${(Number(item.p95_ms || 0) / 1000).toFixed(3)}s`;
      tr.append(tdType, tdFail, tdSec);
      els.diagRuns.appendChild(tr);
    });
  if (!entries.length && !rustEntries.length) {
    els.diagRuns.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无诊断</td></tr>';
  }
}

async function fetchRustRuntimeStats() {
  try {
    const endpoint = String(els.rustEndpoint.value || "").trim().replace(/\/$/, "");
    if (!endpoint) return null;
    const resp = await fetch(`${endpoint}/operators/runtime_stats_v1`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ op: "summary" }),
    });
    if (!resp.ok) return null;
    return await resp.json();
  } catch {
    return null;
  }
}

function graphPayload() {
  store.setWorkflowName(els.workflowName.value);
  return store.exportGraph();
}

function singleSelectedNode() {
  const ids = canvas.getSelectedIds();
  if (!Array.isArray(ids) || ids.length !== 1) return null;
  return store.getNode(ids[0]);
}

function prettyJson(value) {
  return JSON.stringify(value || {}, null, 2);
}

function parseNodeConfigText() {
  const text = String(els.nodeConfig.value || "").trim();
  if (!text) return {};
  let obj = null;
  try {
    obj = JSON.parse(text);
  } catch {
    throw new Error("配置必须是合法 JSON");
  }
  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    throw new Error("配置必须是 JSON 对象");
  }
  return obj;
}

function parseEdgeWhenText() {
  const text = String(els.edgeWhenText?.value || "").trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    throw new Error("连线条件必须是合法 JSON");
  }
}

function parseLooseJsonValue(raw) {
  const text = String(raw || "").trim();
  if (!text) return null;
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function setEdgeWhenBuilderVisibility(kind) {
  const k = String(kind || "none");
  if (els.edgeWhenBoolWrap) els.edgeWhenBoolWrap.style.display = k === "bool" ? "block" : "none";
  if (els.edgeWhenPathWrap) els.edgeWhenPathWrap.style.display = k === "path" ? "block" : "none";
  if (els.edgeWhenRuleWrap) els.edgeWhenRuleWrap.style.display = k === "rule" ? "block" : "none";
}

function edgeWhenFromBuilder() {
  const kind = String(els.edgeWhenKind?.value || "none");
  if (kind === "none") return null;
  if (kind === "bool") return String(els.edgeWhenBool?.value || "true") === "true";
  if (kind === "path") return String(els.edgeWhenPath?.value || "").trim();
  const field = String(els.edgeWhenField?.value || "").trim();
  const op = String(els.edgeWhenOp?.value || "eq").trim() || "eq";
  const out = { field, op };
  if (op !== "exists" && op !== "not_exists") out.value = parseLooseJsonValue(els.edgeWhenValue?.value || "");
  return out;
}

function applyEdgeWhenToBuilder(when) {
  if (!els.edgeWhenKind) return;
  if (when === null || typeof when === "undefined") {
    els.edgeWhenKind.value = "none";
    if (els.edgeWhenPath) els.edgeWhenPath.value = "";
    if (els.edgeWhenField) els.edgeWhenField.value = "";
    if (els.edgeWhenValue) els.edgeWhenValue.value = "";
    if (els.edgeWhenOp) els.edgeWhenOp.value = "eq";
    if (els.edgeWhenBool) els.edgeWhenBool.value = "true";
    setEdgeWhenBuilderVisibility("none");
    return;
  }
  if (typeof when === "boolean") {
    els.edgeWhenKind.value = "bool";
    if (els.edgeWhenBool) els.edgeWhenBool.value = when ? "true" : "false";
    setEdgeWhenBuilderVisibility("bool");
    return;
  }
  if (typeof when === "string") {
    els.edgeWhenKind.value = "path";
    if (els.edgeWhenPath) els.edgeWhenPath.value = when;
    setEdgeWhenBuilderVisibility("path");
    return;
  }
  els.edgeWhenKind.value = "rule";
  if (els.edgeWhenField) els.edgeWhenField.value = String(when.field || "");
  if (els.edgeWhenOp) els.edgeWhenOp.value = String(when.op || "eq");
  if (els.edgeWhenValue) {
    const v = typeof when.value === "undefined" ? "" : JSON.stringify(when.value);
    els.edgeWhenValue.value = v;
  }
  setEdgeWhenBuilderVisibility("rule");
}

function syncEdgeTextFromBuilder() {
  if (!els.edgeWhenText) return;
  const when = edgeWhenFromBuilder();
  els.edgeWhenText.value = when === null ? "" : JSON.stringify(when, null, 2);
}

function rebuildEdgeHints(edge) {
  const fieldMap = new Map();
  function addHint(value, source) {
    const v = String(value || "").trim();
    if (!v) return;
    if (!fieldMap.has(v)) fieldMap.set(v, new Set());
    fieldMap.get(v).add(String(source || "system"));
  }
  [
    "ok",
    "status",
    "detail",
    "detail.ok",
    "detail.status",
    "output.ok",
    "output.status",
  ].forEach((k) => addHint(k, "system"));
  if (edge && edge.from) {
    const fromNode = store.getNode(edge.from);
    if (fromNode) {
      const byType = EDGE_HINTS_BY_NODE_TYPE[String(fromNode.type || "")] || [];
      byType.forEach((k) => addHint(k, `type:${fromNode.type}`));
      const cfg = fromNode.config && typeof fromNode.config === "object" ? fromNode.config : {};
      Object.keys(cfg).forEach((k) => addHint(k, "config"));
    }
  }
  const arr = Array.from(fieldMap.keys()).filter(Boolean).sort((a, b) => a.localeCompare(b));
  const labelFor = (value) => {
    const src = Array.from(fieldMap.get(value) || []).sort().join("|");
    return `${value}  [from:${src}]`;
  };
  if (els.edgeFieldHints) {
    els.edgeFieldHints.innerHTML = "";
    arr.forEach((v) => {
      const op = document.createElement("option");
      op.value = v;
      els.edgeFieldHints.appendChild(op);
    });
  }
  if (els.edgePathHints) {
    els.edgePathHints.innerHTML = "";
    arr
      .map((v) => (v.includes(".") ? v : `detail.${v}`))
      .concat(arr)
      .filter((v, i, xs) => xs.indexOf(v) === i)
      .forEach((v) => {
        const op = document.createElement("option");
        op.value = v;
        els.edgePathHints.appendChild(op);
      });
  }
  if (els.edgeFieldHintSelect) {
    els.edgeFieldHintSelect.innerHTML = '<option value="">选择建议...</option>';
    arr.forEach((v) => {
      const op = document.createElement("option");
      op.value = v;
      op.textContent = labelFor(v);
      els.edgeFieldHintSelect.appendChild(op);
    });
  }
  if (els.edgePathHintSelect) {
    const pathArr = arr
      .map((v) => (v.includes(".") ? v : `detail.${v}`))
      .concat(arr)
      .filter((v, i, xs) => xs.indexOf(v) === i);
    els.edgePathHintSelect.innerHTML = '<option value="">选择建议...</option>';
    pathArr.forEach((v) => {
      const base = v.startsWith("detail.") ? v.slice("detail.".length) : v;
      const op = document.createElement("option");
      op.value = v;
      op.textContent = labelFor(base).replace(`${base}  `, `${v}  `);
      els.edgePathHintSelect.appendChild(op);
    });
  }
}

function renderEdgeConfigEditor() {
  if (!els.selectedEdgeInfo || !els.edgeWhenText) return;
  if (!selectedEdge || !selectedEdge.from || !selectedEdge.to) {
    els.selectedEdgeInfo.textContent = "未选中连线";
    els.edgeWhenText.value = "";
    rebuildEdgeHints(null);
    applyEdgeWhenToBuilder(null);
    if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = true;
    if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = true;
    if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = true;
    if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = true;
    if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = true;
    if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = true;
    return;
  }
  const alive = store.getEdge(selectedEdge.from, selectedEdge.to);
  if (!alive) {
    selectedEdge = null;
    els.selectedEdgeInfo.textContent = "未选中连线";
    els.edgeWhenText.value = "";
    rebuildEdgeHints(null);
    applyEdgeWhenToBuilder(null);
    if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = true;
    if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = true;
    if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = true;
    if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = true;
    if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = true;
    if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = true;
    return;
  }
  selectedEdge = { ...alive };
  rebuildEdgeHints(selectedEdge);
  els.selectedEdgeInfo.textContent = `当前连线: ${selectedEdge.from} -> ${selectedEdge.to}`;
  els.edgeWhenText.value = selectedEdge.when === null || typeof selectedEdge.when === "undefined"
    ? ""
    : JSON.stringify(selectedEdge.when, null, 2);
  applyEdgeWhenToBuilder(selectedEdge.when);
  if (els.btnApplyEdgeCfg) els.btnApplyEdgeCfg.disabled = false;
  if (els.btnClearEdgeCfg) els.btnClearEdgeCfg.disabled = false;
  if (els.btnBuildEdgeWhen) els.btnBuildEdgeWhen.disabled = false;
  if (els.btnParseEdgeWhen) els.btnParseEdgeWhen.disabled = false;
  if (els.edgeFieldHintSelect) els.edgeFieldHintSelect.disabled = false;
  if (els.edgePathHintSelect) els.edgePathHintSelect.disabled = false;
}

function formSchemaForNodeType(type) {
  return NODE_FORM_SCHEMAS[String(type || "")] || [];
}

function setCfgMode(mode) {
  cfgViewMode = mode === "json" ? "json" : "form";
  const isJson = cfgViewMode === "json";
  els.nodeConfig.style.display = isJson ? "block" : "none";
  els.nodeConfigForm.style.display = isJson ? "none" : "block";
  els.btnCfgJson.className = `btn ${isJson ? "" : "secondary"}`.trim();
  els.btnCfgForm.className = `btn ${isJson ? "secondary" : ""}`.trim();
}

function toFieldDisplayValue(type, v) {
  if (type === "json") return prettyJson(v ?? (Array.isArray(v) ? [] : {}));
  if (type === "csv") return Array.isArray(v) ? v.join(",") : "";
  if (type === "bool") return v === true ? "true" : "false";
  if (v === null || v === undefined) return "";
  return String(v);
}

function parseFieldValue(type, text) {
  const raw = String(text || "");
  if (type === "number") {
    const n = Number(raw);
    if (!Number.isFinite(n)) throw new Error(`数字字段格式错误: ${raw}`);
    return n;
  }
  if (type === "csv") {
    return raw
      .split(",")
      .map((s) => s.trim())
      .filter((s) => !!s);
  }
  if (type === "bool") {
    const s = raw.trim().toLowerCase();
    if (s === "true" || s === "1" || s === "yes") return true;
    if (s === "false" || s === "0" || s === "no" || s === "") return false;
    throw new Error(`布尔字段格式错误: ${raw}`);
  }
  if (type === "json") {
    const t = raw.trim();
    if (!t) return {};
    return JSON.parse(t);
  }
  return raw;
}

function renderNodeConfigForm(node, cfg) {
  const schema = formSchemaForNodeType(node?.type);
  const generic = [
    { key: "isolation_level", label: "隔离等级(none/process/sandbox)", type: "text" },
    { key: "sandbox_limits", label: "Sandbox限制(JSON对象)", type: "json" },
    { key: "input_map", label: "输入映射(JSON对象: 目标字段->来源路径)", type: "json" },
    { key: "output_map", label: "输出映射(JSON对象: 别名->输出路径)", type: "json" },
  ];
  const fullSchema = [...schema, ...generic];
  els.nodeConfigForm.innerHTML = "";
  if (!node || !fullSchema.length) {
    els.nodeConfigForm.innerHTML = '<div class="cfg-help">该节点暂无预置表单，请切到 JSON 模式编辑。</div>';
    return;
  }
  fullSchema.forEach((f) => {
    const wrap = document.createElement("div");
    wrap.className = "cfg-field";
    const label = document.createElement("label");
    label.textContent = f.label;
    const control = f.type === "json" ? document.createElement("textarea") : document.createElement("input");
    control.dataset.key = f.key;
    control.dataset.kind = f.type;
    if (f.type !== "json") control.type = "text";
    if (f.type === "number") control.inputMode = "numeric";
    control.value = toFieldDisplayValue(f.type, cfg?.[f.key]);
    wrap.append(label, control);
    els.nodeConfigForm.appendChild(wrap);
  });
}

function parseNodeConfigForm(node, baseCfg = {}) {
  const schema = [
    ...formSchemaForNodeType(node?.type),
    { key: "isolation_level", type: "text" },
    { key: "sandbox_limits", type: "json" },
    { key: "input_map", type: "json" },
    { key: "output_map", type: "json" },
  ];
  if (!schema.length) return { ...baseCfg };
  const out = { ...(baseCfg || {}) };
  const controls = els.nodeConfigForm.querySelectorAll("[data-key]");
  controls.forEach((el) => {
    const key = String(el.dataset.key || "");
    const kind = String(el.dataset.kind || "text");
    if (!key) return;
    const val = parseFieldValue(kind, el.value);
    out[key] = val;
  });
  return out;
}

function readMapTableRows(tbody) {
  const out = {};
  if (!tbody) return out;
  const rows = tbody.querySelectorAll("tr[data-map-row='1']");
  rows.forEach((tr) => {
    const k = String(tr.querySelector("input[data-role='key']")?.value || "").trim();
    const v = String(tr.querySelector("input[data-role='value']")?.value || "").trim();
    if (!k || !v) return;
    out[k] = v;
  });
  return out;
}

function renderMapTableRows(tbody, mapObj, onChange) {
  if (!tbody) return;
  const obj = mapObj && typeof mapObj === "object" && !Array.isArray(mapObj) ? mapObj : {};
  const items = Object.entries(obj);
  tbody.innerHTML = "";
  if (!items.length) {
    tbody.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
    return;
  }
  items.forEach(([k, v]) => {
    const tr = document.createElement("tr");
    tr.dataset.mapRow = "1";
    const tdK = document.createElement("td");
    const tdV = document.createElement("td");
    const tdOp = document.createElement("td");
    const inK = document.createElement("input");
    inK.dataset.role = "key";
    inK.value = String(k || "");
    const inV = document.createElement("input");
    inV.dataset.role = "value";
    inV.value = String(v || "");
    inK.oninput = onChange;
    inV.oninput = onChange;
    const del = document.createElement("button");
    del.className = "mini del";
    del.textContent = "删";
    del.onclick = () => {
      tr.remove();
      onChange();
    };
    tdK.appendChild(inK);
    tdV.appendChild(inV);
    tdOp.appendChild(del);
    tr.append(tdK, tdV, tdOp);
    tbody.appendChild(tr);
  });
}

function syncIoMapFromTables() {
  const node = singleSelectedNode();
  if (!node) return;
  const current = node.config && typeof node.config === "object" ? { ...node.config } : {};
  current.input_map = readMapTableRows(els.inputMapRows);
  current.output_map = readMapTableRows(els.outputMapRows);
  store.updateNodeConfig(node.id, current);
  els.nodeConfig.value = prettyJson(current);
}

function renderIoMapEditor(node, cfg) {
  if (!els.inputMapRows || !els.outputMapRows) return;
  if (!node) {
    els.inputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
    els.outputMapRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">无</td></tr>';
    return;
  }
  const onChange = () => syncIoMapFromTables();
  renderMapTableRows(els.inputMapRows, cfg?.input_map, onChange);
  renderMapTableRows(els.outputMapRows, cfg?.output_map, onChange);
}

function renderNodeConfigEditor() {
  const ids = canvas.getSelectedIds();
  const many = Array.isArray(ids) && ids.length > 1;
  const node = singleSelectedNode();
  if (!node) {
    els.selectedNodeInfo.textContent = many
      ? `当前选中 ${ids.length} 个节点。请只保留 1 个节点后编辑配置。`
      : "未选中节点";
    els.nodeConfig.value = "";
    els.nodeConfig.disabled = true;
    els.btnApplyNodeCfg.disabled = true;
    els.btnResetNodeCfg.disabled = true;
    els.btnFormatNodeCfg.disabled = true;
    els.nodeConfigForm.innerHTML = '<div class="cfg-help">请先选中 1 个节点。</div>';
    renderIoMapEditor(null, null);
    return;
  }
  const cfg = node.config && typeof node.config === "object" ? node.config : defaultNodeConfig(node.type);
  els.selectedNodeInfo.textContent = `当前节点: ${node.id} (${node.type})`;
  els.nodeConfig.value = prettyJson(cfg);
  renderNodeConfigForm(node, cfg);
  els.nodeConfig.disabled = false;
  els.btnApplyNodeCfg.disabled = false;
  els.btnResetNodeCfg.disabled = false;
  els.btnFormatNodeCfg.disabled = false;
  renderIoMapEditor(node, cfg);
}

function runPayload() {
  const graph = graphPayload();
  const isolatedTypes = String(els.chipletIsolatedTypes?.value || "")
    .split(/[;,]/)
    .map((x) => String(x || "").trim())
    .filter(Boolean);
  const toNum = (v, d) => {
    const n = Number(v);
    return Number.isFinite(n) && n > 0 ? Math.floor(n) : d;
  };
  return {
    workflow_id: graph.workflow_id || "custom_v1",
    workflow: graph,
    params: {
      report_title: String(els.reportTitle.value || "").trim(),
      input_files: String(els.inputFiles.value || "").trim(),
      md_only: true,
      paper_markdown_enabled: true,
      export_canonical_bundle: !!els.exportCanonicalBundle.checked,
      canonical_title: String(els.canonicalTitle.value || "").trim() || "AIWF 熟肉语料",
      ocr_lang: "chi_sim+eng",
    },
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
}

function renderAll() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.render();
  renderNodeConfigEditor();
  renderEdgeConfigEditor();
  const pct = Math.round(canvas.getZoom() * 100);
  if (els.zoomText) els.zoomText.textContent = `${pct}%`;
}

function exportJson() {
  const json = JSON.stringify(graphPayload(), null, 2);
  els.log.textContent = json;
  setStatus("已导出流程 JSON 到右侧日志区", true);
}

function setZoom(z) {
  canvas.setZoom(z);
  renderAll();
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
  const graph = graphPayload();
  const valid = validateGraph(graph);
  if (!valid.ok) {
    setStatus(`流程校验失败: ${valid.errors.join(" | ")}`, false);
    return;
  }
  setStatus("工作流运行中...");
  try {
    const out = await window.aiwfDesktop.runWorkflow(runPayload(), {});
    els.log.textContent = JSON.stringify(out, null, 2);
    renderNodeRuns(out.node_runs || []);
    await refreshDiagnostics();
    await refreshRunHistory();
    await refreshReviewQueue();
    setStatus(out.ok ? `运行完成: ${out.run_id}` : `运行结束: ${out.status || "failed"}`, !!out.ok);
  } catch (e) {
    setStatus(`运行失败: ${e}`, false);
  }
}

async function enqueueWorkflowRun() {
  const graph = graphPayload();
  const valid = validateGraph(graph);
  if (!valid.ok) {
    setStatus(`流程校验失败: ${valid.errors.join(" | ")}`, false);
    return;
  }
  try {
    const out = await window.aiwfDesktop.enqueueWorkflowTask({
      label: String(els.workflowName?.value || "workflow_task"),
      payload: runPayload(),
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
    const out = await window.aiwfDesktop.getWorkflowDiagnostics({ limit: 80 });
    const rust = await fetchRustRuntimeStats();
    renderDiagRuns(out || {}, rust || {});
  } catch {}
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

function normalizeAppSchemaObject(obj) {
  const src = obj && typeof obj === "object" && !Array.isArray(obj) ? obj : {};
  const out = {};
  Object.entries(src).forEach(([key, raw]) => {
    const k = String(key || "").trim();
    if (!k) return;
    const rule = raw && typeof raw === "object" && !Array.isArray(raw) ? { ...raw } : {};
    const type = String(rule.type || "string").trim() || "string";
    const item = { type };
    if (rule.required === true) item.required = true;
    if (Object.prototype.hasOwnProperty.call(rule, "default")) item.default = rule.default;
    if (rule.description != null && String(rule.description).trim()) item.description = String(rule.description);
    out[k] = item;
  });
  return out;
}

function appSchemaRowsFromObject(schemaObj) {
  return Object.entries(normalizeAppSchemaObject(schemaObj)).map(([key, rule]) => ({
    key,
    type: String(rule.type || "string"),
    required: rule.required === true,
    defaultText: Object.prototype.hasOwnProperty.call(rule, "default") ? JSON.stringify(rule.default) : "",
    description: String(rule.description || ""),
  }));
}

function collectAppSchemaFromForm() {
  if (!els.appSchemaForm) return {};
  const rows = els.appSchemaForm.querySelectorAll("div[data-app-schema-row='1']");
  const out = {};
  rows.forEach((row) => {
    const key = String(row.querySelector("[data-app-schema='key']")?.value || "").trim();
    const type = String(row.querySelector("[data-app-schema='type']")?.value || "string").trim() || "string";
    const required = !!row.querySelector("[data-app-schema='required']")?.checked;
    const defaultText = String(row.querySelector("[data-app-schema='default']")?.value || "").trim();
    const description = String(row.querySelector("[data-app-schema='description']")?.value || "").trim();
    if (!key) return;
    const item = { type };
    if (required) item.required = true;
    if (defaultText) {
      try {
        item.default = JSON.parse(defaultText);
      } catch {
        item.default = defaultText;
      }
    }
    if (description) item.description = description;
    out[key] = item;
  });
  return out;
}

function renderAppSchemaForm(rows) {
  if (!els.appSchemaForm) return;
  const list = Array.isArray(rows) ? rows : [];
  els.appSchemaForm.innerHTML = "";
  if (!list.length) {
    els.appSchemaForm.innerHTML = '<div class="cfg-help">暂无参数，可点击“新增参数”。</div>';
    return;
  }
  list.forEach((r) => {
    const row = document.createElement("div");
    row.dataset.appSchemaRow = "1";
    row.style.border = "1px solid #dfe8f3";
    row.style.borderRadius = "8px";
    row.style.padding = "6px";
    row.style.marginBottom = "6px";
    row.innerHTML = `
      <div style="display:grid;grid-template-columns:1.2fr 0.9fr 0.7fr auto;gap:6px">
        <input data-app-schema="key" placeholder="参数名" value="${String(r.key || "").replace(/"/g, "&quot;")}" />
        <select data-app-schema="type">
          <option value="string">string</option>
          <option value="number">number</option>
          <option value="boolean">boolean</option>
          <option value="object">object</option>
          <option value="array">array</option>
        </select>
        <label style="margin:0;display:flex;align-items:center;font-size:12px"><input data-app-schema="required" type="checkbox" style="width:auto;margin-right:6px" />必填</label>
        <button data-app-schema="del" class="mini del" type="button">删除</button>
      </div>
      <input data-app-schema="default" placeholder="默认值(JSON或纯文本)" value="${String(r.defaultText || "").replace(/"/g, "&quot;")}" style="margin-top:6px" />
      <input data-app-schema="description" placeholder="说明" value="${String(r.description || "").replace(/"/g, "&quot;")}" style="margin-top:6px" />
    `;
    const typeSel = row.querySelector("[data-app-schema='type']");
    if (typeSel) typeSel.value = String(r.type || "string");
    const req = row.querySelector("[data-app-schema='required']");
    if (req) req.checked = !!r.required;
    const delBtn = row.querySelector("[data-app-schema='del']");
    if (delBtn) {
      delBtn.addEventListener("click", () => {
        row.remove();
        syncAppSchemaJsonFromForm();
        syncRunParamsFormFromJson();
        if (!els.appSchemaForm.querySelector("div[data-app-schema-row='1']")) renderAppSchemaForm([]);
      });
    }
    row.querySelectorAll("[data-app-schema]").forEach((ctrl) => {
      if (String(ctrl.getAttribute("data-app-schema")) === "del") return;
      ctrl.addEventListener("input", () => {
        syncAppSchemaJsonFromForm();
        syncRunParamsFormFromJson();
      });
      ctrl.addEventListener("change", () => {
        syncAppSchemaJsonFromForm();
        syncRunParamsFormFromJson();
      });
    });
    els.appSchemaForm.appendChild(row);
  });
}

function syncAppSchemaJsonFromForm() {
  if (!els.appSchemaJson) return;
  const schema = collectAppSchemaFromForm();
  els.appSchemaJson.value = JSON.stringify(schema, null, 2);
}

function syncAppSchemaFormFromJson() {
  const text = String(els.appSchemaJson?.value || "").trim();
  if (!text) {
    renderAppSchemaForm([]);
    return;
  }
  let parsed = {};
  try {
    parsed = JSON.parse(text);
  } catch (e) {
    setStatus(`Schema JSON 解析失败: ${e}`, false);
    return;
  }
  renderAppSchemaForm(appSchemaRowsFromObject(parsed));
  syncAppSchemaJsonFromForm();
}

function defaultRunParamValue(rule) {
  if (rule && Object.prototype.hasOwnProperty.call(rule, "default")) return rule.default;
  const t = String(rule?.type || "string");
  if (t === "number") return 0;
  if (t === "boolean") return false;
  if (t === "object") return {};
  if (t === "array") return [];
  return "";
}

function collectRunParamsForm() {
  if (!els.appRunParamsForm) return {};
  const controls = els.appRunParamsForm.querySelectorAll("[data-app-run-param]");
  const out = {};
  controls.forEach((ctrl) => {
    const key = String(ctrl.dataset.appRunParam || "").trim();
    const type = String(ctrl.dataset.appRunType || "string");
    if (!key) return;
    if (type === "boolean") {
      out[key] = String(ctrl.value || "false") === "true";
      return;
    }
    if (type === "number") {
      const n = Number(ctrl.value || 0);
      out[key] = Number.isFinite(n) ? n : 0;
      return;
    }
    if (type === "object" || type === "array") {
      const raw = String(ctrl.value || "").trim();
      if (!raw) {
        out[key] = type === "array" ? [] : {};
        return;
      }
      try {
        out[key] = JSON.parse(raw);
      } catch {
        out[key] = raw;
      }
      return;
    }
    out[key] = String(ctrl.value || "");
  });
  return out;
}

function syncRunParamsJsonFromForm() {
  if (!els.appRunParams) return;
  els.appRunParams.value = JSON.stringify(collectRunParamsForm(), null, 2);
}

function renderRunParamsFormBySchema(schemaObj, preferredParams) {
  if (!els.appRunParamsForm) return;
  const schema = normalizeAppSchemaObject(schemaObj);
  const entries = Object.entries(schema);
  const preferred = preferredParams && typeof preferredParams === "object" && !Array.isArray(preferredParams) ? preferredParams : {};
  if (!entries.length) {
    els.appRunParamsForm.innerHTML = '<div class="cfg-help">当前应用无参数定义，可直接编辑 JSON。</div>';
    return;
  }
  const params = {};
  entries.forEach(([k, rule]) => {
    if (Object.prototype.hasOwnProperty.call(preferred, k)) params[k] = preferred[k];
    else params[k] = defaultRunParamValue(rule);
  });
  els.appRunParamsForm.innerHTML = "";
  entries.forEach(([key, rule]) => {
    const wrap = document.createElement("div");
    wrap.className = "cfg-field";
    const label = document.createElement("label");
    label.textContent = `${key}${rule.required ? " *" : ""}`;
    wrap.appendChild(label);
    if (rule.description) {
      const tip = document.createElement("div");
      tip.className = "cfg-help";
      tip.textContent = String(rule.description);
      wrap.appendChild(tip);
    }
    const type = String(rule.type || "string");
    let ctrl;
    if (type === "boolean") {
      ctrl = document.createElement("select");
      ctrl.innerHTML = '<option value="true">true</option><option value="false">false</option>';
      ctrl.value = params[key] ? "true" : "false";
    } else if (type === "object" || type === "array") {
      ctrl = document.createElement("textarea");
      ctrl.style.minHeight = "66px";
      ctrl.value = JSON.stringify(params[key], null, 2);
    } else {
      ctrl = document.createElement("input");
      ctrl.type = type === "number" ? "number" : "text";
      ctrl.value = params[key] == null ? "" : String(params[key]);
    }
    ctrl.dataset.appRunParam = key;
    ctrl.dataset.appRunType = type;
    ctrl.addEventListener("input", syncRunParamsJsonFromForm);
    ctrl.addEventListener("change", syncRunParamsJsonFromForm);
    wrap.appendChild(ctrl);
    els.appRunParamsForm.appendChild(wrap);
  });
  syncRunParamsJsonFromForm();
}

function syncRunParamsFormFromJson() {
  const schema = normalizeAppSchemaObject((() => {
    try { return JSON.parse(String(els.appSchemaJson?.value || "{}")); } catch { return {}; }
  })());
  let params = {};
  try {
    const raw = String(els.appRunParams?.value || "{}").trim();
    params = raw ? JSON.parse(raw) : {};
  } catch (e) {
    setStatus(`运行参数 JSON 解析失败: ${e}`, false);
    return;
  }
  renderRunParamsFormBySchema(schema, params);
}

async function publishApp() {
  const graph = graphPayload();
  const name = String(els.appPublishName?.value || graph?.name || "").trim();
  if (!name) {
    setStatus("应用名称不能为空", false);
    return;
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

function currentSandboxPresetPayload() {
  return {
    thresholds: sandboxThresholdsPayload(),
    dedup_window_sec: sandboxDedupWindowSec(),
    rules: sandboxRulesPayloadFromUi(),
    autofix: {
      enabled: !!els.sandboxAutoFixEnabled?.checked,
      pause_queue: !!els.sandboxAutoFixPauseQueue?.checked,
      require_review: !!els.sandboxAutoFixRequireReview?.checked,
      force_isolation: !!els.sandboxAutoFixForceIsolation?.checked,
      red_threshold: Number(els.sandboxAutoFixRedThreshold?.value || 3),
      window_sec: Number(els.sandboxAutoFixWindowSec?.value || 900),
      force_minutes: Number(els.sandboxAutoFixForceMinutes?.value || 60),
      force_mode: String(els.sandboxAutoFixForceMode?.value || "process").trim() || "process",
    },
  };
}

function applySandboxPresetPayload(preset) {
  const p = preset && typeof preset === "object" ? preset : {};
  const t = p.thresholds && typeof p.thresholds === "object" ? p.thresholds : {};
  if (els.sandboxThresholdYellow && Number.isFinite(Number(t.yellow))) els.sandboxThresholdYellow.value = String(Math.floor(Number(t.yellow)));
  if (els.sandboxThresholdRed && Number.isFinite(Number(t.red))) els.sandboxThresholdRed.value = String(Math.floor(Number(t.red)));
  if (els.sandboxDedupWindowSec && Number.isFinite(Number(p.dedup_window_sec))) els.sandboxDedupWindowSec.value = String(Math.floor(Number(p.dedup_window_sec)));
  if (p.rules) applySandboxRulesToUi(p.rules);
  const af = p.autofix && typeof p.autofix === "object" ? p.autofix : {};
  if (els.sandboxAutoFixEnabled) els.sandboxAutoFixEnabled.checked = af.enabled !== false;
  if (els.sandboxAutoFixPauseQueue) els.sandboxAutoFixPauseQueue.checked = af.pause_queue !== false;
  if (els.sandboxAutoFixRequireReview) els.sandboxAutoFixRequireReview.checked = af.require_review !== false;
  if (els.sandboxAutoFixForceIsolation) els.sandboxAutoFixForceIsolation.checked = af.force_isolation !== false;
  if (els.sandboxAutoFixRedThreshold && Number.isFinite(Number(af.red_threshold))) els.sandboxAutoFixRedThreshold.value = String(Math.floor(Number(af.red_threshold)));
  if (els.sandboxAutoFixWindowSec && Number.isFinite(Number(af.window_sec))) els.sandboxAutoFixWindowSec.value = String(Math.floor(Number(af.window_sec)));
  if (els.sandboxAutoFixForceMinutes && Number.isFinite(Number(af.force_minutes))) els.sandboxAutoFixForceMinutes.value = String(Math.floor(Number(af.force_minutes)));
  if (els.sandboxAutoFixForceMode && af.force_mode) els.sandboxAutoFixForceMode.value = String(af.force_mode);
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

async function refreshQualityGateReports() {
  try {
    saveQualityGatePrefs();
    const out = await window.aiwfDesktop.listWorkflowQualityGateReports({
      limit: 120,
      filter: qualityGateFilterPayload(),
    });
    renderQualityGateRows(out?.items || []);
  } catch {
    renderQualityGateRows([]);
  }
}

async function exportQualityGateReports() {
  saveQualityGatePrefs();
  const out = await window.aiwfDesktop.exportWorkflowQualityGateReports({
    limit: 500,
    format: String(els.qualityGateExportFormat?.value || "md").trim().toLowerCase() === "json" ? "json" : "md",
    filter: qualityGateFilterPayload(),
  });
  if (!out?.ok) {
    if (!out?.canceled) setStatus(`导出质量门禁失败: ${out?.error || "unknown"}`, false);
    return;
  }
  setStatus(`质量门禁报告已导出: ${out.path}`, true);
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

function reviewHistoryFilterPayload() {
  return {
    run_id: String(els.reviewHistoryRunId?.value || "").trim(),
    reviewer: String(els.reviewHistoryReviewer?.value || "").trim(),
    status: String(els.reviewHistoryStatus?.value || "").trim(),
    date_from: String(els.reviewHistoryDateFrom?.value || "").trim(),
    date_to: String(els.reviewHistoryDateTo?.value || "").trim(),
  };
}

function renderReviewHistoryRows(items = []) {
  if (!els.reviewHistoryRows) return;
  if (!Array.isArray(items) || !items.length) {
    els.reviewHistoryRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.reviewHistoryRows.innerHTML = "";
  items.slice(0, 100).forEach((it) => {
    const tr = document.createElement("tr");
    const tdKey = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdReviewer = document.createElement("td");
    const tdTime = document.createElement("td");
    tdKey.textContent = String(it.review_key || "");
    const status = String(it.status || "");
    tdStatus.textContent = status;
    if (status === "approved") tdStatus.style.color = "#087443";
    if (status === "rejected") tdStatus.style.color = "#b42318";
    tdReviewer.textContent = String(it.reviewer || "");
    tdTime.textContent = String(it.decided_at || "");
    tr.append(tdKey, tdStatus, tdReviewer, tdTime);
    els.reviewHistoryRows.appendChild(tr);
  });
}

async function refreshReviewHistory() {
  try {
    const out = await window.aiwfDesktop.listManualReviewHistory({
      limit: 300,
      filter: reviewHistoryFilterPayload(),
    });
    renderReviewHistoryRows(out?.items || []);
  } catch {
    renderReviewHistoryRows([]);
  }
}

async function exportReviewHistory() {
  try {
    const out = await window.aiwfDesktop.exportManualReviewHistory({
      filter: reviewHistoryFilterPayload(),
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出审核历史失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`审核历史已导出: ${out.path} (${out.total} 条)`, true);
  } catch (e) {
    setStatus(`导出审核历史失败: ${e}`, false);
  }
}

async function compareRuns() {
  const runA = String(els.compareRunA?.value || "").trim();
  const runB = String(els.compareRunB?.value || "").trim();
  if (!runA || !runB) {
    setStatus("请填写 Run A / Run B", false);
    return;
  }
  const out = await window.aiwfDesktop.compareWorkflowRuns({ run_a: runA, run_b: runB });
  lastCompareResult = out;
  els.log.textContent = JSON.stringify(out, null, 2);
  renderCompareResult(out);
  setStatus(out?.ok ? "运行对比完成" : `运行对比失败: ${out?.error || "unknown"}`, !!out?.ok);
}

async function exportCompareReport() {
  const runA = String(els.compareRunA?.value || "").trim();
  const runB = String(els.compareRunB?.value || "").trim();
  if (!runA || !runB) {
    setStatus("请先填写 Run A / Run B", false);
    return;
  }
  try {
    const format = String(els.compareReportFormat?.value || "md").trim() || "md";
    const out = await window.aiwfDesktop.exportCompareReport({
      run_a: runA,
      run_b: runB,
      format,
    });
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`导出对比报告失败: ${out?.error || "unknown"}`, false);
      return;
    }
    setStatus(`对比报告已导出: ${out.path}`, true);
  } catch (e) {
    setStatus(`导出对比报告失败: ${e}`, false);
  }
}

function renderCompareResult(out) {
  if (!els.compareSummary || !els.compareRows) return;
  if (!out?.ok) {
    els.compareSummary.textContent = `对比失败: ${out?.error || "unknown"}`;
    els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
    return;
  }
  const s = out.summary || {};
  const onlyChanged = !!els.compareOnlyChanged?.checked;
  const onlyStatusChanged = !!els.compareOnlyStatusChanged?.checked;
  const minDelta = Number(els.compareMinDelta?.value || 0.001);
  const safeDelta = Number.isFinite(minDelta) ? Math.max(0, minDelta) : 0.001;
  const srcRows = Array.isArray(out.node_diff) ? out.node_diff : [];
  const rows = srcRows.filter((r) => {
    const delta = Math.abs(Number(r.seconds_delta || 0));
    if (onlyStatusChanged && !r.status_changed) return false;
    if (onlyChanged && !(r.status_changed || delta >= safeDelta)) return false;
    return true;
  });
  els.compareSummary.textContent = `A:${s.run_a || "-"} | B:${s.run_b || "-"} | changed_nodes:${s.changed_nodes || 0} | visible:${rows.length}/${srcRows.length}`;
  if (!rows.length) {
    els.compareRows.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无</td></tr>';
    return;
  }
  els.compareRows.innerHTML = "";
  rows.forEach((r) => {
    const tr = document.createElement("tr");
    const tdNode = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdA = document.createElement("td");
    const tdB = document.createElement("td");
    const tdD = document.createElement("td");
    tdNode.textContent = `${String(r.id || "")}(${String(r.type || "")})`;
    tdStatus.textContent = `${String(r.status_a || "")} / ${String(r.status_b || "")}`;
    tdA.textContent = Number(r.seconds_a || 0).toFixed(3);
    tdB.textContent = Number(r.seconds_b || 0).toFixed(3);
    const delta = Number(r.seconds_delta || 0);
    tdD.textContent = delta.toFixed(3);
    if (r.status_changed) tdStatus.style.color = "#b42318";
    if (delta > 0.001) tdD.style.color = "#b42318";
    if (delta < -0.001) tdD.style.color = "#087443";
    if (r.status_changed || Math.abs(delta) >= safeDelta) tr.style.background = "#fff8f2";
    tr.append(tdNode, tdStatus, tdA, tdB, tdD);
    els.compareRows.appendChild(tr);
  });
}

els.btnAdd.addEventListener("click", () => {
  const t = String(els.nodeType.value || "").trim();
  if (!t) {
    setStatus("节点类型不能为空", false);
    return;
  }
  const id = store.addNode(t, 60, 60);
  canvas.setSelectedIds([id]);
  renderAll();
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
if (els.btnExportCompareReport) els.btnExportCompareReport.addEventListener("click", exportCompareReport);
if (els.compareOnlyChanged) els.compareOnlyChanged.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
if (els.compareOnlyStatusChanged) els.compareOnlyStatusChanged.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
if (els.compareMinDelta) els.compareMinDelta.addEventListener("change", () => renderCompareResult(lastCompareResult || { ok: false, error: "请先执行对比" }));
els.btnExport.addEventListener("click", exportJson);
els.btnSaveFlow.addEventListener("click", saveFlow);
els.btnLoadFlow.addEventListener("click", loadFlow);
if (els.btnApplyTemplate) els.btnApplyTemplate.addEventListener("click", applySelectedTemplate);
if (els.btnSaveTemplate) els.btnSaveTemplate.addEventListener("click", saveCurrentAsTemplate);
if (els.templateSelect) els.templateSelect.addEventListener("change", renderTemplateParamsForm);
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
    setZoom(canvas.getZoom() + (evt.deltaY < 0 ? 0.08 : -0.08));
  },
  { passive: false }
);

els.canvasWrap.addEventListener("dragover", (evt) => {
  evt.preventDefault();
  evt.dataTransfer.dropEffect = "copy";
});

els.canvasWrap.addEventListener("drop", (evt) => {
  evt.preventDefault();
  const t = String(evt.dataTransfer.getData("text/plain") || "").trim();
  if (!t) return;
  const snapEnabled = !!els.snapGrid.checked;
  const grid = 24;
  const world = canvas.clientToWorld(evt.clientX, evt.clientY);
  const rawX = world.x - 105;
  const rawY = world.y - 43;
  const x = snapEnabled ? Math.round(rawX / grid) * grid : rawX;
  const y = snapEnabled ? Math.round(rawY / grid) * grid : rawY;
  const id = store.addNode(t, x, y);
  canvas.setSelectedIds([id]);
  renderAll();
  setStatus(`已拖入节点: ${t}`, true);
});

renderPalette();
renderTemplateSelect();
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
