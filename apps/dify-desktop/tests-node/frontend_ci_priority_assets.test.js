const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

function readText(relPath) {
  return fs.readFileSync(path.resolve(__dirname, "../../..", relPath), "utf8");
}

test("ci check treats winui as primary frontend and electron package checks as compatibility-only", () => {
  const ciCheck = readText("ops/scripts/ci_check.ps1");
  const dispatch = readText("ops/scripts/dispatch_full_integration_self_hosted.ps1");
  const verifyBranch = readText("ops/scripts/verify_branch_ci.ps1");
  const verification = readText("docs/verification.md");
  const quickSkipMatch = ciCheck.match(/\$quickSkipParams = @\(([\s\S]*?)\n  \)/);
  const compatibilitySkipMatch = ciCheck.match(/\$compatibilitySkipParams = @\(([\s\S]*?)\n  \)/);

  assert.ok(quickSkipMatch, "quick profile skip list not found");
  assert.ok(compatibilitySkipMatch, "compatibility profile skip list not found");
  const quickSkipBlock = quickSkipMatch[1];
  const compatibilitySkipBlock = compatibilitySkipMatch[1];

  assert.match(ciCheck, /ValidateSet\("Default","Quick","Full","Compatibility"\)/);
  assert.match(ciCheck, /frontend_primary_verification_latest\.json/);
  assert.match(ciCheck, /frontend_compatibility_verification_latest\.json/);
  assert.match(quickSkipBlock, /"SkipDesktopPackageTests"/);
  assert.doesNotMatch(quickSkipBlock, /"SkipNativeWinuiSmoke"/);
  assert.doesNotMatch(compatibilitySkipBlock, /"SkipDesktopPackageTests"/);
  assert.match(compatibilitySkipBlock, /"SkipNativeWinuiSmoke"/);
  assert.match(ciCheck, /Use -CiProfile Compatibility/i);
  assert.match(ciCheck, /running native winui primary frontend smoke check/i);
  assert.match(ciCheck, /running Electron compatibility packaged startup check/i);
  assert.match(ciCheck, /skip Electron compatibility packaged startup checks/i);
  assert.match(dispatch, /ValidateSet\("Default","Quick","Full","Compatibility"\)/);
  assert.match(verifyBranch, /ValidateSet\("Default","Quick","Full","Compatibility"\)/);

  assert.match(verification, /native WinUI primary frontend smoke outside CI unless you explicitly skip it/i);
  assert.match(verification, /ci_check\.ps1 -CiProfile Compatibility/i);
  assert.match(verification, /Electron compatibility packaged startup checks:/i);
  assert.match(verification, /These Electron package checks are compatibility-only/i);
  assert.match(verification, /frontend_primary_verification_latest\.json/i);
  assert.match(verification, /frontend_compatibility_verification_latest\.json/i);
});
