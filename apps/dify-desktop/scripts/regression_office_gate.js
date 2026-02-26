const fs = require("fs");
const path = require("path");
const os = require("os");
const { runOfflineCleaning } = require("../offline_engine");
const {
  auditOfficeArtifact,
  gateThresholds,
  evaluateOfficeArtifacts,
} = require("./lib/office_artifact_audit");

function listTextFixtures(dir, limit = 6) {
  if (!dir || !fs.existsSync(dir)) return [];
  return fs.readdirSync(dir)
    .map((n) => path.join(dir, n))
    .filter((p) => fs.statSync(p).isFile() && [".txt", ".docx", ".pdf"].includes(path.extname(p).toLowerCase()))
    .sort()
    .slice(0, limit);
}

function byKind(arts, kind) {
  return (arts || []).find((a) => String(a?.kind || "").toLowerCase() === kind);
}

async function runOne(filePath, outRoot) {
  const out = await runOfflineCleaning({
    output_root: outRoot,
    params: {
      report_title: "office_gate",
      input_files: filePath,
      office_lang: "zh",
      office_theme: "assignment",
      office_quality_mode: "high",
      md_only: false,
      strict_output_gate: false,
      content_quality_gate_enabled: false,
      office_quality_gate_enabled: false,
      xlsx_embed_charts: true,
    },
  });
  const arts = Array.isArray(out?.artifacts) ? out.artifacts : [];
  const xlsx = byKind(arts, "xlsx");
  const docx = byKind(arts, "docx");
  const pptx = byKind(arts, "pptx");
  const audits = [xlsx, docx, pptx].filter(Boolean).map((a) => auditOfficeArtifact(String(a.path || "")));
  const gate = evaluateOfficeArtifacts(audits, gateThresholds());
  return {
    file: path.basename(filePath),
    ok: !!out?.ok,
    warnings: Array.isArray(out?.warnings) ? out.warnings.length : 0,
    quality_score: Number(out?.quality_score?.score || 0),
    audits,
    gate,
  };
}

function toMd(report) {
  const lines = [];
  lines.push("# Office Output Gate Report");
  lines.push("");
  lines.push(`- generated_at: ${report.generated_at}`);
  lines.push(`- fixture_dir: ${report.fixture_dir}`);
  lines.push(`- total_cases: ${report.rows.length}`);
  lines.push(`- failed_cases: ${report.failures.length}`);
  lines.push("");
  lines.push("## Thresholds");
  Object.entries(report.thresholds || {}).forEach(([k, v]) => lines.push(`- ${k}: ${v}`));
  lines.push("");
  lines.push("## Cases");
  report.rows.forEach((r) => {
    lines.push(`### ${r.file}`);
    lines.push(`- run_ok: ${r.ok}`);
    lines.push(`- gate_ok: ${r.gate?.ok}`);
    lines.push(`- quality_score: ${r.quality_score}`);
    lines.push(`- gate_failures: ${(r.gate?.failures || []).join(", ") || "none"}`);
    (r.audits || []).forEach((a) => {
      lines.push(`- ${a.kind}: size=${a.size || 0}, charts=${a.chart_count || 0}, slides=${a.slide_count || 0}, headings=${a.heading_count || 0}, tables=${a.table_count || 0}, images=${a.image_count || 0}`);
      if (a.kind === "docx") {
        lines.push(`- docx_text: len=${a.text_len || 0}, bad=${a.bad_char_count || 0}, qmark=${a.qmark_count || 0}, cjk_ratio=${Number(a.cjk_ratio || 0).toFixed(4)}`);
      }
    });
    lines.push("");
  });
  if (report.failures.length) {
    lines.push("## Failures");
    report.failures.forEach((f) => lines.push(`- ${f}`));
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function main() {
  const fixtureDir = process.env.AIWF_OFFICE_FIXTURES_DIR
    || path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples");
  const files = listTextFixtures(fixtureDir, Number(process.env.AIWF_OFFICE_FIXTURES_LIMIT || 6));
  if (!files.length) {
    console.log(`office_gate skipped: no fixtures in ${fixtureDir}`);
    return;
  }
  const outRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-office-gate-"));
  const rows = [];
  for (const fp of files) {
    // eslint-disable-next-line no-await-in-loop
    rows.push(await runOne(fp, outRoot));
  }
  const failures = [];
  rows.forEach((r) => {
    if (!r.ok) failures.push(`${r.file}: run_failed`);
    if (!r.gate?.ok) failures.push(`${r.file}: ${(r.gate?.failures || []).join(",")}`);
  });
  const report = {
    generated_at: new Date().toISOString(),
    fixture_dir: fixtureDir,
    thresholds: gateThresholds(),
    rows,
    failures,
  };
  const jsonPath = path.join(outRoot, "office_gate_report.json");
  const mdPath = path.join(outRoot, "office_gate_report.md");
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, toMd(report), "utf8");
  console.log(`office gate report: ${jsonPath}`);
  console.log(`office gate markdown: ${mdPath}`);
  if (failures.length) throw new Error(`office gate failed: ${failures.length}`);
  console.log("office gate ok");
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});

