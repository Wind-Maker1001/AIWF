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

export {
  cloneJson,
  migrateLoadedWorkflowGraph,
  previewValue,
};
