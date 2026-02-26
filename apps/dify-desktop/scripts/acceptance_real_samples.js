const fs = require("fs");
const path = require("path");
const os = require("os");
const { runOfflineCleaning } = require("../offline_engine");
const {
  auditOfficeArtifact,
  gateThresholds,
  evaluateOfficeArtifacts,
} = require("./lib/office_artifact_audit");

function detectInputDir() {
  const fromEnv = String(process.env.AIWF_ACCEPTANCE_INPUT_DIR || "").trim();
  if (fromEnv) return fromEnv;
  const outRoot = String(process.env.AIWF_ACCEPTANCE_OUTPUT_ROOT || "").trim() || "E:\\Desktop_Real\\AIWF";
  const cands = [
    path.join(outRoot, "sample_pool"),
    "E:\\Desktop_Real\\samples",
    "E:\\Downloads",
    "E:\\Desktop_Real",
    path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples"),
  ];
  return cands.find((p) => fs.existsSync(p)) || "";
}

function detectOutputRoot() {
  const fromEnv = String(process.env.AIWF_ACCEPTANCE_OUTPUT_ROOT || "").trim();
  if (fromEnv) return fromEnv;
  if (fs.existsSync("E:\\Desktop_Real")) return "E:\\Desktop_Real\\AIWF";
  return fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-acceptance-"));
}

function listInputs(dir, limit = 6) {
  if (!dir || !fs.existsSync(dir)) return [];
  const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx"]);
  return fs.readdirSync(dir)
    .map((n) => path.join(dir, n))
    .filter((p) => fs.statSync(p).isFile() && exts.has(path.extname(p).toLowerCase()))
    .sort()
    .slice(0, limit);
}

function readManifest(fp) {
  try {
    if (!fs.existsSync(fp)) return null;
    const obj = JSON.parse(fs.readFileSync(fp, "utf8"));
    if (!Array.isArray(obj?.files)) return null;
    return obj;
  } catch {
    return null;
  }
}

function writeManifest(fp, inputDir, files) {
  const payload = {
    generated_at: new Date().toISOString(),
    input_dir: inputDir,
    files: files.map((p) => {
      const st = fs.statSync(p);
      return {
        path: p,
        name: path.basename(p),
        size: Number(st.size || 0),
        mtime_ms: Number(st.mtimeMs || 0),
      };
    }),
  };
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
}

function resolveInputs(inputDir, outputRoot, limit = 6) {
  const reportDir = path.join(outputRoot, "reports");
  const manifestPath = String(process.env.AIWF_ACCEPTANCE_MANIFEST || path.join(reportDir, "acceptance_manifest.json")).trim();
  const lockEnabled = String(process.env.AIWF_ACCEPTANCE_LOCK || "1").trim() !== "0";
  if (lockEnabled) {
    const old = readManifest(manifestPath);
    if (old && Array.isArray(old.files) && old.files.length) {
      const files = old.files
        .map((x) => String(x?.path || "").trim())
        .filter((p) => p && fs.existsSync(p))
        .slice(0, limit);
      if (files.length) return { files, manifestPath, fromLockedManifest: true };
    }
  }
  const files = listInputs(inputDir, limit);
  if (lockEnabled && files.length) writeManifest(manifestPath, inputDir, files);
  return { files, manifestPath, fromLockedManifest: false };
}

function byKind(arts, kind) {
  return (arts || []).find((a) => String(a?.kind || "").toLowerCase() === kind);
}

function md(report) {
  const lines = [];
  lines.push("# Real Samples Acceptance Report");
  lines.push("");
  lines.push(`- generated_at: ${report.generated_at}`);
  lines.push(`- input_dir: ${report.input_dir}`);
  lines.push(`- output_root: ${report.output_root}`);
  lines.push(`- manifest_path: ${report.manifest_path}`);
  lines.push(`- from_locked_manifest: ${report.from_locked_manifest}`);
  lines.push(`- total_cases: ${report.rows.length}`);
  lines.push(`- failed_cases: ${report.failures.length}`);
  lines.push("");
  lines.push("## Cases");
  report.rows.forEach((r) => {
    lines.push(`### ${r.file}`);
    lines.push(`- job_id: ${r.job_id}`);
    lines.push(`- run_ok: ${r.ok}`);
    lines.push(`- quality_score: ${r.quality_score}`);
    lines.push(`- gate_ok: ${r.gate?.ok}`);
    lines.push(`- gate_failures: ${(r.gate?.failures || []).join(", ") || "none"}`);
    lines.push(`- artifacts_dir: ${r.artifacts_dir || ""}`);
    lines.push("");
  });
  if (report.failures.length) {
    lines.push("## Failures");
    report.failures.forEach((f) => lines.push(`- ${f}`));
    lines.push("");
  }
  return `${lines.join("\n")}\n`;
}

async function runCase(filePath, outRoot) {
  const out = await runOfflineCleaning({
    output_root: outRoot,
    params: {
      report_title: `acceptance_${path.basename(filePath)}`,
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
  const jobId = String(out?.job_id || "");
  return {
    file: path.basename(filePath),
    ok: !!out?.ok,
    job_id: jobId,
    quality_score: Number(out?.quality_score?.score || 0),
    warnings: Array.isArray(out?.warnings) ? out.warnings : [],
    artifacts: arts,
    audits,
    gate,
    artifacts_dir: path.join(outRoot, jobId, "artifacts"),
  };
}

async function main() {
  const inputDir = detectInputDir();
  if (!inputDir) throw new Error("acceptance_input_dir_not_found");
  const limit = Number.isFinite(Number(process.env.AIWF_ACCEPTANCE_LIMIT))
    ? Math.max(1, Math.min(30, Math.floor(Number(process.env.AIWF_ACCEPTANCE_LIMIT))))
    : 6;
  const outputRoot = detectOutputRoot();
  fs.mkdirSync(outputRoot, { recursive: true });
  const ts = Date.now();
  const reportDir = path.join(outputRoot, "reports");
  fs.mkdirSync(reportDir, { recursive: true });

  const resolved = resolveInputs(inputDir, outputRoot, limit);
  const files = resolved.files;
  if (!files.length) throw new Error(`no_input_files_in:${inputDir}`);

  const rows = [];
  for (const fp of files) {
    // eslint-disable-next-line no-await-in-loop
    rows.push(await runCase(fp, outputRoot));
  }
  const failures = [];
  rows.forEach((r) => {
    if (!r.ok) failures.push(`${r.file}: run_failed`);
    if (!r.gate?.ok) failures.push(`${r.file}: ${String((r.gate?.failures || []).join(","))}`);
  });
  const report = {
    generated_at: new Date().toISOString(),
    input_dir: inputDir,
    output_root: outputRoot,
    manifest_path: resolved.manifestPath,
    from_locked_manifest: !!resolved.fromLockedManifest,
    thresholds: gateThresholds(),
    rows,
    failures,
  };
  const jsonPath = path.join(reportDir, `acceptance_report_${ts}.json`);
  const mdPath = path.join(reportDir, `acceptance_report_${ts}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  fs.writeFileSync(mdPath, md(report), "utf8");
  console.log(`acceptance report json: ${jsonPath}`);
  console.log(`acceptance report md: ${mdPath}`);
  if (failures.length) throw new Error(`acceptance_failed:${failures.length}`);
  console.log("acceptance ok");
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
