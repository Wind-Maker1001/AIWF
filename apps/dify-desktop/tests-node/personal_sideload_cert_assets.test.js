const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("personal sideload certificate scripts and docs stay wired", () => {
  const ensureScript = readText("ops/scripts/ensure_personal_sideload_certificate.ps1");
  const checkScript = readText("ops/scripts/check_personal_sideload_certificate.ps1");
  const doc = readText("docs/personal_sideload_certificate_20260321.md");
  const releaseWrapper = readText("ops/scripts/release_frontend_productize.ps1");

  assert.match(ensureScript, /AIWF Personal Sideload/);
  assert.match(ensureScript, /RotateWhenExpiresInDays/);
  assert.match(ensureScript, /aiwf_personal_sideload\.metadata\.json/);
  assert.match(checkScript, /WarnWhenExpiresInDays/);
  assert.match(checkScript, /FailWhenExpiresInDays/);
  assert.match(checkScript, /days_remaining/);

  assert.match(doc, /ensure_personal_sideload_certificate/i);
  assert.match(doc, /check_personal_sideload_certificate/i);
  assert.match(doc, /FailWhenExpiresInDays/i);
  assert.match(doc, /personal_sideload_certificate/i);

  assert.match(releaseWrapper, /PersonalSideloadCert/);
  assert.match(releaseWrapper, /PersonalSideloadCertFailWhenExpiresInDays/);
  assert.match(releaseWrapper, /AllowExpiringPersonalSideloadCert/);
});
