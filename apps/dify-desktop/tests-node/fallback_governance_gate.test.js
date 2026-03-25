const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

test("fallback governance gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_fallback_governance.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
});
