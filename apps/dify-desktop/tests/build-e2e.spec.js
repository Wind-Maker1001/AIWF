const path = require("path");
const fs = require("fs");
const { test, expect, _electron: electron } = require("@playwright/test");

const ENABLE_BUILD_E2E = String(process.env.AIWF_ENABLE_BUILD_E2E || "") === "1";
const ENABLE_FULL_BUILD = String(process.env.AIWF_BUILD_E2E_FULL || "") === "1";

async function openMain() {
  const appDir = path.resolve(__dirname, "..");
  const electronApp = await electron.launch({ args: [appDir] });
  const page = await electronApp.firstWindow();
  return { electronApp, page };
}

async function runReleaseGateInRenderer(page) {
  const scripts = ["test:unit", "smoke", "test:regression", "test:regression:dirty", "test:office-gate"];
  for (const script of scripts) {
    // eslint-disable-next-line no-await-in-loop
    const out = await page.evaluate(async (s) => window.aiwfDesktop.runLocalGateCheck({ script: s }), script);
    if (!out || !out.ok) {
      throw new Error(`release gate script failed: ${script}, error=${String(out && out.error ? out.error : "unknown")}`);
    }
  }
}

test("build e2e cancel works in real runtime", async () => {
  test.skip(!ENABLE_BUILD_E2E, "set AIWF_ENABLE_BUILD_E2E=1 to run real build e2e");
  test.setTimeout(30 * 60 * 1000);

  const { electronApp, page } = await openMain();
  await runReleaseGateInRenderer(page);

  const out = await page.evaluate(async () => {
    const started = window.aiwfDesktop.runLocalBuildScript({ script: "build:win:release:gated" });
    await new Promise((r) => setTimeout(r, 4_000));
    const canceled = await window.aiwfDesktop.cancelLocalBuildScript();
    const done = await started;
    return { canceled, done };
  });

  expect(out && out.canceled && out.canceled.ok).toBeTruthy();
  expect(out && out.done && out.done.canceled).toBeTruthy();
  await electronApp.close();
});

test("build e2e full produces desktop artifacts and release reports", async () => {
  test.skip(!ENABLE_BUILD_E2E || !ENABLE_FULL_BUILD, "set AIWF_ENABLE_BUILD_E2E=1 and AIWF_BUILD_E2E_FULL=1 to run full build e2e");
  test.setTimeout(60 * 60 * 1000);

  const { electronApp, page } = await openMain();
  await runReleaseGateInRenderer(page);

  const runOut = await page.evaluate(async () => {
    return await window.aiwfDesktop.runLocalBuildScript({ script: "build:win:release:gated" });
  });
  expect(runOut && runOut.ok).toBeTruthy();

  const copied = Array.isArray(runOut.copied_files) ? runOut.copied_files : [];
  expect(copied.length).toBeGreaterThan(0);
  copied.forEach((p) => {
    expect(fs.existsSync(String(p))).toBeTruthy();
    expect(/\.exe$/i.test(String(p))).toBeTruthy();
  });

  const exported = await page.evaluate(async () => {
    const md = await window.aiwfDesktop.exportReleaseReport({ format: "md" });
    const json = await window.aiwfDesktop.exportReleaseReport({ format: "json" });
    return { md, json };
  });
  expect(exported?.md?.ok).toBeTruthy();
  expect(exported?.json?.ok).toBeTruthy();
  expect(fs.existsSync(String(exported.md.path || ""))).toBeTruthy();
  expect(fs.existsSync(String(exported.json.path || ""))).toBeTruthy();

  await electronApp.close();
});
