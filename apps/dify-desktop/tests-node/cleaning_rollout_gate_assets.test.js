const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("cleaning rollout gate is wired into ci, package, and release scripts", () => {
  const ciCheck = readText("ops/scripts/ci_check.ps1");
  const packageOffline = readText("ops/scripts/package_offline_bundle.ps1");
  const releaseLegacy = readText("ops/scripts/release_productize.ps1");
  const releaseFrontend = readText("ops/scripts/release_frontend_productize.ps1");
  const releaseElectron = readText("ops/scripts/release_electron_compatibility.ps1");

  assert.match(ciCheck, /SkipCleaningRustV2RolloutGate/);
  assert.match(ciCheck, /check_cleaning_rust_v2_rollout\.ps1/);
  assert.match(ciCheck, /cleaning_rust_v2_rollout/);

  assert.match(packageOffline, /SkipCleaningRustV2RolloutGate/);
  assert.match(packageOffline, /check_cleaning_rust_v2_rollout\.ps1/);
  assert.match(packageOffline, /cleaning_rust_v2_rollout/);
  assert.match(packageOffline, /RequireRealEvidence/);
  assert.match(packageOffline, /cleaning_shadow_rollout\.json/);
  assert.match(packageOffline, /acceptance_evidence/);

  assert.match(releaseLegacy, /SkipCleaningRustV2RolloutGate/);
  assert.match(releaseLegacy, /check_cleaning_rust_v2_rollout\.ps1/);
  assert.match(releaseLegacy, /cleaning_rust_v2_rollout/);
  assert.match(releaseLegacy, /RequireRealEvidence/);
  assert.match(releaseLegacy, /cleaning_shadow_rollout\.json/);
  assert.match(releaseLegacy, /acceptance_evidence/);

  assert.match(releaseFrontend, /SkipCleaningRustV2RolloutGate/);
  assert.match(releaseFrontend, /check_cleaning_rust_v2_rollout\.ps1/);
  assert.match(releaseFrontend, /cleaning_rust_v2_rollout/);
  assert.match(releaseFrontend, /RequireRealEvidence/);
  assert.match(releaseFrontend, /cleaning_shadow_rollout\.json/);
  assert.match(releaseFrontend, /acceptance_evidence/);

  assert.match(releaseElectron, /SkipCleaningRustV2RolloutGate/);
});

test("cleaning rollout governance docs and defaults are explicit", () => {
  const envExample = readText("ops/config/dev.env.example");
  const verification = readText("docs/verification.md");
  const desktopDoc = readText("docs/dify_desktop_app.md");
  const backendQuickstart = readText("docs/quickstart_backend.md");

  assert.match(envExample, /AIWF_CLEANING_RUST_V2_MODE=off/);
  assert.match(envExample, /AIWF_CLEANING_RUST_V2_VERIFY_ON_DEFAULT=false/);

  assert.match(verification, /check_cleaning_rust_v2_rollout\.ps1/i);
  assert.match(verification, /cleaning rust v2 rollout gate/i);
  assert.match(verification, /run_mode_audit\.jsonl/i);
  assert.match(verification, /sidecar_python_rust_consistency_report\.json/i);
  assert.match(verification, /acceptance evidence/i);
  assert.match(verification, /shadow_compare\.status == "matched"|shadow_compare\.status == `matched`|shadow_compare\.status == matched/i);
  assert.match(verification, /default\+verify|mode = "default"|verify_on_default = true/i);

  assert.match(desktopDoc, /requested_rust_v2_mode/i);
  assert.match(desktopDoc, /effective_rust_v2_mode/i);
  assert.match(desktopDoc, /verify_on_default/i);

  assert.match(backendQuickstart, /check_cleaning_rust_v2_rollout\.ps1/i);
  assert.match(backendQuickstart, /AIWF_CLEANING_RUST_V2_MODE=off\|shadow\|default/i);

});
