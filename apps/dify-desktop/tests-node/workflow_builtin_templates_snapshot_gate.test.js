const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const { spawnSync } = require("node:child_process");

function parseJsonLine(output) {
  const lines = String(output || "")
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);
  for (let index = lines.length - 1; index >= 0; index -= 1) {
    const line = lines[index];
    if (!line.startsWith("{") || !line.endsWith("}")) {
      continue;
    }
    return JSON.parse(line);
  }
  return null;
}

test("workflow builtin template snapshot sync gate passes", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const script = path.resolve(repoRoot, "ops/scripts/check_workflow_builtin_templates_snapshot_sync.ps1");
  const result = spawnSync("powershell", ["-ExecutionPolicy", "Bypass", "-File", script], {
    cwd: repoRoot,
    encoding: "utf8",
  });

  assert.equal(result.status, 0, `${result.stdout}\n${result.stderr}`);
  const payload = parseJsonLine(`${result.stdout}\n${result.stderr}`);
  assert.ok(payload, `expected structured JSON payload in output:\n${result.stdout}\n${result.stderr}`);
  assert.equal(payload.status, "passed");
  assert.match(payload.snapshotPath, /contracts[\\/]desktop[\\/]workflow_builtin_templates\.v1\.json/i);
  assert.match(payload.staticConfigPath, /apps[\\/]dify-desktop[\\/]renderer[\\/]workflow[\\/]static-config\.js/i);
  assert.equal(payload.schemaVersion, "workflow_builtin_templates.v1");
  assert.ok(payload.templateCount >= 1);
  assert.equal(payload.equivalent, true);
});

test("workflow builtin template snapshot sync gate stays wired into frontend verification", () => {
  const repoRoot = path.resolve(__dirname, "../../..");
  const frontendConvergence = fs.readFileSync(path.resolve(repoRoot, "ops/scripts/check_frontend_convergence.ps1"), "utf8");
  const verificationDoc = fs.readFileSync(path.resolve(repoRoot, "docs/verification.md"), "utf8");

  assert.match(frontendConvergence, /check_workflow_builtin_templates_snapshot_sync\.ps1/);
  assert.match(frontendConvergence, /workflow builtin template snapshot sync checks failed/i);
  assert.match(verificationDoc, /check_workflow_builtin_templates_snapshot_sync\.ps1/);
  assert.match(verificationDoc, /workflow builtin template snapshot sync gate/i);
  assert.match(verificationDoc, /workflow_builtin_templates\.v1\.json/i);
});
