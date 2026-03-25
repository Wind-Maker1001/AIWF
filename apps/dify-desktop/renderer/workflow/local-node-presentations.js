export const LOCAL_NODE_PRESENTATIONS = Object.freeze({
  ingest_files: { name: "接收文件", desc: "接收原始文件输入并标准化路径" },
  clean_md: { name: "清洗 Markdown", desc: "提取正文并生成可供 AI 使用的 Markdown" },
  compute_rust: { name: "计算 (Rust)", desc: "调用 Rust 服务计算指标" },
  manual_review: { name: "人工审核", desc: "人工门禁节点，可批准/驳回后分支" },
  sql_chart_v1: { name: "SQL图表 v1", desc: "将行数据转换为图表 categories/series" },
  office_slot_fill_v1: { name: "Office插槽填充 v1", desc: "将图表/文本数据绑定到模板插槽，供 DOCX/PPTX/XLSX 渲染" },
  ai_strategy_v1: { name: "AI策略路由 v1", desc: "按主备策略路由外部AI并降级重试" },
  ds_refine: { name: "DS提炼（推荐）", desc: "使用 DeepSeek API 提炼 PDF/图片抽取文本（内部映射到 ai_refine）" },
  ai_refine: { name: "AI 提炼", desc: "外部 AI 对内容进行摘要提炼" },
  ai_audit: { name: "AI 审核", desc: "审核门禁，不通过则阻断后续流程" },
  md_output: { name: "Markdown 输出", desc: "写出 workflow_summary 与产物索引" },
});
