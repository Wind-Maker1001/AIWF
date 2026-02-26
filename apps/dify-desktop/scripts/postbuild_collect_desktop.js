const fs = require("fs");
const path = require("path");

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

function main() {
  const projectRoot = path.resolve(__dirname, "..");
  const distDir = path.join(projectRoot, "dist");
  const desktopBuildDir = path.join(detectDesktopDir(), "AIWF_Builds");
  const reportsDir = path.join(desktopBuildDir, "reports");
  fs.mkdirSync(desktopBuildDir, { recursive: true });
  fs.mkdirSync(reportsDir, { recursive: true });

  const all = collectFiles(distDir, []);
  const exes = all
    .filter((p) => /\.exe$/i.test(path.basename(p)))
    .map((p) => ({ path: p, mtime: Number(fs.statSync(p).mtimeMs || 0) }))
    .sort((a, b) => b.mtime - a.mtime)
    .slice(0, 8);

  if (exes.length === 0) {
    // eslint-disable-next-line no-console
    console.error("[postbuild-collect] no exe found under dist/");
    process.exit(2);
  }

  const copied = [];
  exes.forEach((x) => {
    try {
      const dst = path.join(desktopBuildDir, path.basename(x.path));
      fs.copyFileSync(x.path, dst);
      copied.push(dst);
    } catch {}
  });

  const payload = {
    generated_at: new Date().toISOString(),
    source: "cli_postbuild_collect",
    dist_dir: distDir,
    desktop_dir: desktopBuildDir,
    copied_files: copied,
  };
  const ts = Date.now();
  const jsonPath = path.join(reportsDir, `release_report_${ts}.json`);
  const mdPath = path.join(reportsDir, `release_report_${ts}.md`);
  fs.writeFileSync(jsonPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  const md = [
    "# AIWF 发布报告",
    "",
    `- 生成时间: ${payload.generated_at}`,
    `- 来源: ${payload.source}`,
    `- 产物目录: ${payload.desktop_dir}`,
    "",
    "## 复制的可执行文件",
    ...copied.map((p, i) => `${i + 1}. ${p}`),
    "",
  ].join("\n");
  fs.writeFileSync(mdPath, `\uFEFF${md}\n`, "utf8");

  // eslint-disable-next-line no-console
  console.log(`[postbuild-collect] copied exes: ${copied.length}`);
  // eslint-disable-next-line no-console
  console.log(`[postbuild-collect] md report: ${mdPath}`);
  // eslint-disable-next-line no-console
  console.log(`[postbuild-collect] json report: ${jsonPath}`);
}

main();
