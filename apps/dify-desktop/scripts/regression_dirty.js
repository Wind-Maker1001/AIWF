const path = require("path");
const { spawn } = require("child_process");

const root = path.resolve(__dirname, "..");
const script = path.join(__dirname, "regression_baseline.js");
const env = {
  ...process.env,
  AIWF_REGRESSION_FIXTURES_DIR: path.join(root, "tests", "fixtures", "regression_samples_dirty"),
  AIWF_REGRESSION_BASELINE: path.join(root, "tests", "fixtures", "regression_baseline_dirty.json"),
};

const child = spawn(process.execPath, [script], {
  cwd: root,
  env,
  stdio: "inherit",
  windowsHide: true,
});

child.on("exit", (code) => {
  process.exit(Number(code || 0));
});

