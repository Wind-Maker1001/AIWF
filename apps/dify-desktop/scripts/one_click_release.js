const { spawnSync } = require("child_process");

function run(cmd, args, opts = {}) {
  const isWin = process.platform === "win32";
  const spawnCmd = isWin ? "cmd.exe" : cmd;
  const spawnArgs = isWin
    ? ["/d", "/s", "/c", [cmd, ...args].join(" ")]
    : args;
  const out = spawnSync(spawnCmd, spawnArgs, {
    stdio: "inherit",
    shell: false,
    windowsHide: true,
    ...opts,
  });
  if (out.error) {
    throw new Error(`${cmd} ${args.join(" ")} failed: ${String(out.error.message || out.error)}`);
  }
  if (out.status !== 0) {
    throw new Error(`${cmd} ${args.join(" ")} failed with exit=${out.status}, signal=${out.signal || ""}`);
  }
}

function checkPythonOpenpyxl() {
  const out = spawnSync("python", ["-c", "import openpyxl;print(openpyxl.__version__)"], {
    encoding: "utf8",
    windowsHide: true,
  });
  if (out.status !== 0) {
    throw new Error("python_openpyxl_missing: please install python and `pip install openpyxl`");
  }
  const v = String(out.stdout || "").trim();
  // eslint-disable-next-line no-console
  console.log(`[one-click] python openpyxl: ${v}`);
}

function main() {
  const withAcceptance = String(process.env.AIWF_ONECLICK_WITH_ACCEPTANCE || "1").trim() !== "0";
  const buildScript = String(process.env.AIWF_ONECLICK_BUILD_SCRIPT || "build:win:installer:release:gated").trim();
  checkPythonOpenpyxl();
  run("npm", ["run", "release:gate"], {
    env: { ...process.env, AIWF_SKIP_BUILD_ARTIFACT_CHECK: "1" },
  });
  if (withAcceptance) {
    run("npm", ["run", "acceptance:real"]);
  }
  run("npm", ["run", buildScript]);
  // eslint-disable-next-line no-console
  console.log("[one-click] release pipeline done");
}

main();
