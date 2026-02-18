export const NODE_CATALOG = [
  { type: "ingest_files", name: "接收文件", desc: "接收原始文件输入并标准化路径" },
  { type: "clean_md", name: "清洗 Markdown", desc: "提取正文并生成可供 AI 使用的 Markdown" },
  { type: "compute_rust", name: "计算 (Rust)", desc: "调用 Rust 服务计算指标" },
  { type: "ai_refine", name: "AI 提炼", desc: "外部 AI 对内容进行摘要提炼" },
  { type: "ai_audit", name: "AI 审核", desc: "审核门禁，不通过则阻断后续流程" },
  { type: "md_output", name: "Markdown 输出", desc: "写出 workflow_summary 与产物索引" },
];

export function defaultWorkflowGraph() {
  return {
    workflow_id: "minimal_v1",
    name: "生肉到 AI 备用语料",
    nodes: [
      { id: "n1", type: "ingest_files", x: 40, y: 40 },
      { id: "n2", type: "clean_md", x: 320, y: 40 },
      { id: "n3", type: "compute_rust", x: 600, y: 40 },
      { id: "n4", type: "ai_refine", x: 880, y: 40 },
      { id: "n5", type: "ai_audit", x: 880, y: 240 },
      { id: "n6", type: "md_output", x: 600, y: 240 },
    ],
    edges: [
      { from: "n1", to: "n2" },
      { from: "n2", to: "n3" },
      { from: "n3", to: "n4" },
      { from: "n4", to: "n5" },
      { from: "n5", to: "n6" },
    ],
  };
}
