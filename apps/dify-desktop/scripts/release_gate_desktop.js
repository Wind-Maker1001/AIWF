const { spawnSync } = require("child_process");
const path = require("path");
const fs = require("fs");

const cwd = path.resolve(__dirname, "..");
const npmBin = process.platform === "win32" ? "npm.cmd" : "npm";
const SKIP_ARTIFACT_CHECK = String(process.env.AIWF_SKIP_BUILD_ARTIFACT_CHECK || "") === "1";
const RECENT_WINDOW_MS = Number(process.env.AIWF_BUILD_ARTIFACT_RECENT_MS || 7_200_000);
const RETRY_COUNT = Number(process.env.AIWF_BUILD_ARTIFACT_RETRY_COUNT || 4);
const RETRY_INTERVAL_MS = Number(process.env.AIWF_BUILD_ARTIFACT_RETRY_INTERVAL_MS || 1200);
const MODE = String(process.env.AIWF_RELEASE_GATE_MODE || "full").trim().toLowerCase();
const RUN_TEST_CHECKS = MODE !== "artifact_only";
const RUN_ARTIFACT_CHECK = MODE !== "tests_only";

const checks = [
  "test:unit",
  "smoke",
  "test:regression",
  "test:regression:dirty",
  "test:office-gate",
];

if (RUN_TEST_CHECKS) {
  for (const script of checks) {
    // eslint-disable-next-line no-console
    console.log(`\n[release-gate] running npm run ${script}\n`);
    const r = spawnSync(npmBin, ["run", script], {
      cwd,
      stdio: "inherit",
      windowsHide: true,
      env: process.env,
    });
    if ((r.status || 0) !== 0) {
      // eslint-disable-next-line no-console
      console.error(`[release-gate] failed at ${script}`);
      process.exit(r.status || 1);
    }
  }
}

function detectDesktopDir() {
  const home = process.env.USERPROFILE || process.env.HOME || "";
  const candidates = [
    path.join(home, "Desktop"),
    path.join(home, "OneDrive", "Desktop"),
  ];
  for (const p of candidates) {
    try {
      if (p && fs.existsSync(p)) return p;
    } catch {}
  }
  return candidates[0];
}

function collectFiles(root, out = []) {
  try {
    if (!root || !fs.existsSync(root)) return out;
    const items = fs.readdirSync(root, { withFileTypes: true });
    items.forEach((it) => {
      const fp = path.join(root, it.name);
      if (it.isDirectory()) collectFiles(fp, out);
      else out.push(fp);
    });
  } catch {}
  return out;
}

function latestFiles(files, pattern) {
  return files
    .filter((p) => pattern.test(path.basename(p)))
    .map((p) => ({ path: p, mtime: Number(fs.statSync(p).mtimeMs || 0) }))
    .sort((a, b) => b.mtime - a.mtime);
}

function sleepMs(ms) {
  const t = Number(ms || 0);
  if (t <= 0) return;
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, t);
}

function assertDesktopArtifacts() {
  const desktop = detectDesktopDir();
  const buildDir = path.join(desktop, "AIWF_Builds");
  const reportsDir = path.join(buildDir, "reports");
  let latestExe = null;
  let latestMd = null;
  let latestJson = null;
  let missing = [];

  for (let i = 0; i <= Math.max(0, RETRY_COUNT); i += 1) {
    const allBuildFiles = collectFiles(buildDir, []);
    const allReportFiles = collectFiles(reportsDir, []);
    const exes = latestFiles(allBuildFiles, /\.exe$/i);
    const mdReports = latestFiles(allReportFiles, /^release_report_.*\.md$/i);
    const jsonReports = latestFiles(allReportFiles, /^release_report_.*\.json$/i);
    const now = Date.now();
    const isRecent = (item) => !!item && (now - Number(item.mtime || 0) <= RECENT_WINDOW_MS);
    latestExe = exes[0] || null;
    latestMd = mdReports[0] || null;
    latestJson = jsonReports[0] || null;
    missing = [];
    if (!latestExe || !isRecent(latestExe)) missing.push("desktop_exe_recent");
    if (!latestMd || !isRecent(latestMd)) missing.push("release_report_md_recent");
    if (!latestJson || !isRecent(latestJson)) missing.push("release_report_json_recent");
    if (missing.length === 0) break;
    sleepMs(RETRY_INTERVAL_MS);
  }

  // eslint-disable-next-line no-console
  console.log(`[release-gate] desktop artifacts dir: ${buildDir}`);
  // eslint-disable-next-line no-console
  console.log(`[release-gate] latest exe: ${latestExe ? latestExe.path : "N/A"}`);
  // eslint-disable-next-line no-console
  console.log(`[release-gate] latest md report: ${latestMd ? latestMd.path : "N/A"}`);
  // eslint-disable-next-line no-console
  console.log(`[release-gate] latest json report: ${latestJson ? latestJson.path : "N/A"}`);

  if (missing.length > 0) {
    // eslint-disable-next-line no-console
    console.error(`[release-gate] desktop artifact check failed: ${missing.join(", ")}`);
    process.exit(2);
  }
}

if (RUN_ARTIFACT_CHECK) {
  if (!SKIP_ARTIFACT_CHECK) {
    assertDesktopArtifacts();
  } else {
    // eslint-disable-next-line no-console
    console.log("[release-gate] skip desktop artifact check (AIWF_SKIP_BUILD_ARTIFACT_CHECK=1)");
  }
}

// eslint-disable-next-line no-console
console.log("\n[release-gate] all checks passed");
