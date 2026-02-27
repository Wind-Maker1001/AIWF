function registerOutputDomainChiplets(registry, deps = {}) {
  const {
    fs,
    path,
    summarizeCorpus,
    writeWorkflowSummary,
    sha256Text,
    nodeOutputByType,
  } = deps;

  registry.register("sql_chart_v1", {
    id: "chiplet.sql_chart_v1",
    priority: 71,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const inRows = Array.isArray(cfg.rows) ? cfg.rows : [];
      const rows = inRows.length ? inRows : (nodeOutputByType(ctx, "load_rows_v3")?.detail?.rows || []);
      const categoryField = String(cfg.category_field || "category");
      const valueField = String(cfg.value_field || "value");
      const seriesField = String(cfg.series_field || "series");
      const topN = Number.isFinite(Number(cfg.top_n)) ? Math.max(1, Math.floor(Number(cfg.top_n))) : 100;
      const grouped = new Map();
      for (const r of rows) {
        const c = String((r && r[categoryField]) ?? "");
        const s = String((r && r[seriesField]) ?? "default");
        const v = Number((r && r[valueField]) ?? 0);
        if (!grouped.has(c)) grouped.set(c, {});
        const cur = grouped.get(c);
        cur[s] = Number(cur[s] || 0) + (Number.isFinite(v) ? v : 0);
      }
      const cats = Array.from(grouped.keys()).slice(0, topN);
      const seriesKeys = new Set();
      cats.forEach((c) => Object.keys(grouped.get(c) || {}).forEach((k) => seriesKeys.add(k)));
      const series = Array.from(seriesKeys).map((k) => ({
        name: k,
        data: cats.map((c) => Number((grouped.get(c) || {})[k] || 0)),
      }));
      return {
        ok: true,
        chart_type: String(cfg.chart_type || "bar"),
        categories: cats,
        series,
        rows_in: rows.length,
      };
    },
  });

  registry.register("office_slot_fill_v1", {
    id: "chiplet.office_slot_fill_v1",
    priority: 70,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx, node) {
      const cfg = node?.config && typeof node.config === "object" ? node.config : {};
      const sourceType = String(cfg.chart_source_node || "sql_chart_v1").trim() || "sql_chart_v1";
      const chart = nodeOutputByType(ctx, sourceType) || {};
      const slots = cfg.slots && typeof cfg.slots === "object" && !Array.isArray(cfg.slots) ? { ...cfg.slots } : {};
      const templateVersion = String(cfg.template_version || "v1");
      const requiredSlots = Array.isArray(cfg.required_slots) ? cfg.required_slots.map((x) => String(x || "").trim()).filter(Boolean) : ["chart_main"];
      if (!slots.chart_main) {
        slots.chart_main = {
          categories: Array.isArray(chart?.categories) ? chart.categories : [],
          series: Array.isArray(chart?.series) ? chart.series : [],
        };
      }
      const artifactRoot = path.join(ctx.outputRoot, ctx.runId, "artifacts");
      fs.mkdirSync(artifactRoot, { recursive: true });
      const bindingPath = path.join(artifactRoot, "office_slot_binding.json");
      const validationPath = path.join(artifactRoot, "office_template_validation.json");
      const missingSlots = requiredSlots.filter((k) => !(k in slots));
      const emptySlots = requiredSlots.filter((k) => {
        const v = slots[k];
        if (v === null || v === undefined) return true;
        if (Array.isArray(v)) return v.length === 0;
        if (typeof v === "object") return Object.keys(v).length === 0;
        return String(v).trim() === "";
      });
      const payload = {
        run_id: String(ctx.runId || ""),
        workflow_id: String(ctx.workflowId || ""),
        template_kind: String(cfg.template_kind || "pptx"),
        template_version: templateVersion,
        required_slots: requiredSlots,
        slots,
      };
      fs.writeFileSync(bindingPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
      const validation = {
        ok: missingSlots.length === 0,
        run_id: String(ctx.runId || ""),
        workflow_id: String(ctx.workflowId || ""),
        template_kind: payload.template_kind,
        template_version: templateVersion,
        required_slots: requiredSlots,
        missing_slots: missingSlots,
        empty_slots: emptySlots,
        checked_at: new Date().toISOString(),
      };
      fs.writeFileSync(validationPath, `${JSON.stringify(validation, null, 2)}\n`, "utf8");
      const warnings = [];
      if (missingSlots.length > 0) warnings.push(`missing_slots:${missingSlots.join(",")}`);
      if (emptySlots.length > 0) warnings.push(`empty_slots:${emptySlots.join(",")}`);
      return {
        ok: true,
        template_kind: payload.template_kind,
        template_version: templateVersion,
        slots,
        binding_path: bindingPath,
        validation_path: validationPath,
        warnings,
      };
    },
  });

  registry.register("md_output", {
    id: "chiplet.md_output.v1",
    priority: 50,
    timeout_ms: 60000,
    retries: 0,
    async run(ctx) {
      const source = nodeOutputByType(ctx, "clean_md");
      const artDir = source?.ai_corpus_path
        ? path.dirname(source.ai_corpus_path)
        : path.join(ctx.outputRoot, ctx.runId, "artifacts");
      fs.mkdirSync(artDir, { recursive: true });
      const summaryPath = path.join(artDir, "workflow_summary.md");
      writeWorkflowSummary(summaryPath, {
        run_id: ctx.runId,
        workflow_id: ctx.workflowId,
        clean_job_id: ctx.cleanResult?.job_id || "",
        metrics: ctx.metrics || summarizeCorpus(ctx.corpusText || ""),
        audit: ctx.audit || { passed: false, reasons: ["未执行审核节点"] },
        ai_text: ctx.aiText || "",
      });
      const summaryArtifact = {
        artifact_id: "md_workflow_summary_001",
        kind: "md",
        path: summaryPath,
        sha256: sha256Text(fs.readFileSync(summaryPath, "utf8")),
      };
      ctx.workflowSummaryArtifact = summaryArtifact;
      return summaryArtifact;
    },
  });
}

module.exports = {
  registerOutputDomainChiplets,
};
