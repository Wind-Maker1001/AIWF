const fs = require("fs");
const path = require("path");
const os = require("os");
const { runOfflineCleaning } = require("./offline_engine");

async function main() {
  const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-desktop-smoke-"));
  const txt = path.join(tmp, "input.txt");
  const txtComma = path.join(tmp, "input,comma.txt");
  fs.writeFileSync(txt, "第一行证据\n第二行证据\n第二行证据\n", "utf8");
  fs.writeFileSync(txtComma, "逗号路径也要被正确读取\n", "utf8");

  const out = await runOfflineCleaning({
    params: {
      report_title: "smoke",
      input_files: JSON.stringify([txt, txtComma]),
      office_lang: "zh",
      office_theme: "debate",
    },
    output_root: tmp,
  });

  if (!out.ok || !Array.isArray(out.artifacts) || out.artifacts.length < 3) {
    throw new Error("offline engine output invalid");
  }

  for (const a of out.artifacts) {
    if (!fs.existsSync(a.path) || fs.statSync(a.path).size <= 0) {
      throw new Error(`artifact invalid: ${a.path}`);
    }
  }

  console.log("smoke ok");
}

main().catch((e) => {
  console.error(`smoke failed: ${e}`);
  process.exit(1);
});
