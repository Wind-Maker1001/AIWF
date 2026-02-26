const fs = require("fs");
const path = require("path");
const os = require("os");
const { runOfflineCleaning } = require("../offline_engine");

function listFixtureFiles(dir) {
  if (!dir || !fs.existsSync(dir)) return [];
  const exts = new Set([".pdf", ".docx", ".txt", ".png", ".jpg", ".jpeg", ".xlsx"]);
  return fs.readdirSync(dir)
    .map((name) => path.join(dir, name))
    .filter((p) => fs.statSync(p).isFile() && exts.has(path.extname(p).toLowerCase()))
    .sort()
    .slice(0, 6);
}

function loadBaseline(fp) {
  if (!fp || !fs.existsSync(fp)) {
    return { min_quality_score: 50, max_gibberish_ratio: 0.35, min_artifacts: 4, case_expectations: {} };
  }
  try {
    const txt = fs.readFileSync(fp, "utf8").replace(/^\uFEFF/, "");
    const raw = JSON.parse(txt);
    return {
      min_quality_score: Number(raw.min_quality_score || 50),
      max_gibberish_ratio: Number(raw.max_gibberish_ratio || 0.35),
      min_artifacts: Number(raw.min_artifacts || 4),
      case_expectations: raw.case_expectations && typeof raw.case_expectations === "object" ? raw.case_expectations : {},
    };
  } catch {
    return { min_quality_score: 50, max_gibberish_ratio: 0.35, min_artifacts: 4, case_expectations: {} };
  }
}

async function runCase(filePath, outRoot) {
  const out = await runOfflineCleaning({
    output_root: outRoot,
    params: {
      report_title: "regression_baseline",
      input_files: filePath,
      office_lang: "zh",
      office_theme: "assignment",
      md_only: false,
      paper_markdown_enabled: true,
      strict_output_gate: false,
      content_quality_gate_enabled: false,
      office_quality_gate_enabled: false,
    },
  });
  return {
    file: path.basename(filePath),
    ok: !!out?.ok,
    quality_score: Number(out?.quality_score?.score || 0),
    gibberish_ratio: Number(out?.quality_score?.gibberish_ratio || 0),
    artifacts: Array.isArray(out?.artifacts) ? out.artifacts.length : 0,
    has_filtered_noise: Array.isArray(out?.artifacts) && out.artifacts.some((a) => a.artifact_id === "md_filtered_noise_001"),
    warnings: Array.isArray(out?.warnings) ? out.warnings.length : 0,
  };
}

async function main() {
  const fixtureDir = process.env.AIWF_REGRESSION_FIXTURES_DIR || path.resolve(__dirname, "..", "tests", "fixtures", "regression_samples");
  const defaultBaseline = /dirty/i.test(path.basename(fixtureDir))
    ? path.resolve(__dirname, "..", "tests", "fixtures", "regression_baseline_dirty.json")
    : path.resolve(__dirname, "..", "tests", "fixtures", "regression_baseline.json");
  const baselinePath = process.env.AIWF_REGRESSION_BASELINE || defaultBaseline;
  const baseline = loadBaseline(baselinePath);
  const files = listFixtureFiles(fixtureDir);

  if (files.length === 0) {
    console.log(`regression skipped: no fixtures in ${fixtureDir}`);
    return;
  }

  const outRoot = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-regression-"));
  const rows = [];
  for (const fp of files) {
    // eslint-disable-next-line no-await-in-loop
    rows.push(await runCase(fp, outRoot));
  }

  const failures = [];
  rows.forEach((r) => {
    if (!r.ok) failures.push(`${r.file}: run failed`);
    if (r.quality_score < baseline.min_quality_score) failures.push(`${r.file}: quality_score ${r.quality_score} < ${baseline.min_quality_score}`);
    if (r.gibberish_ratio > baseline.max_gibberish_ratio) failures.push(`${r.file}: gibberish_ratio ${r.gibberish_ratio} > ${baseline.max_gibberish_ratio}`);
    if (r.artifacts < baseline.min_artifacts) failures.push(`${r.file}: artifacts ${r.artifacts} < ${baseline.min_artifacts}`);
    const exp = baseline.case_expectations?.[r.file];
    if (exp && typeof exp === "object") {
      if (exp.expect_filtered_noise === true && !r.has_filtered_noise) {
        failures.push(`${r.file}: expected filtered noise artifact`);
      }
      if (exp.min_warnings !== undefined) {
        const n = Number(exp.min_warnings);
        if (Number.isFinite(n) && r.warnings < n) failures.push(`${r.file}: warnings ${r.warnings} < ${n}`);
      }
      if (exp.max_quality_score !== undefined) {
        const n = Number(exp.max_quality_score);
        if (Number.isFinite(n) && r.quality_score > n) failures.push(`${r.file}: quality_score ${r.quality_score} > ${n}`);
      }
    }
  });

  const report = {
    generated_at: new Date().toISOString(),
    fixture_dir: fixtureDir,
    baseline,
    total_cases: rows.length,
    rows,
    failures,
  };
  const reportPath = path.join(outRoot, "regression_report.json");
  fs.writeFileSync(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  console.log(`regression report: ${reportPath}`);
  console.log(JSON.stringify(report, null, 2));
  if (failures.length > 0) {
    throw new Error(`regression failed: ${failures.length} issues`);
  }
  console.log("regression ok");
}

main().catch((e) => {
  console.error(String(e && e.stack ? e.stack : e));
  process.exit(1);
});
