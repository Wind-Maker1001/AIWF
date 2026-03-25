const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("ci helper scripts surface local release-ready architecture scorecard", () => {
  const getCiStatus = readText("ops/scripts/get_ci_status.ps1");
  const verifyBranch = readText("ops/scripts/verify_branch_ci.ps1");
  const verification = readText("docs/verification.md");

  assert.match(getCiStatus, /architecture_scorecard_release_ready_latest\.json/);
  assert.match(getCiStatus, /ArchitectureScorecard/);
  assert.match(verifyBranch, /Report-ArchitectureScorecard/);
  assert.match(verifyBranch, /ArchitectureScorecard = \$status\.ArchitectureScorecard/);
  assert.match(verification, /get_ci_status\.ps1/i);
  assert.match(verification, /verify_branch_ci\.ps1/i);
  assert.match(verification, /architecture_scorecard_release_ready_latest\.json/i);
});
