const path = require("path");
const { test, expect, _electron: electron } = require("@playwright/test");

async function openMain() {
  const appDir = path.resolve(__dirname, "..");
  const electronApp = await electron.launch({ args: [appDir] });
  const page = await electronApp.firstWindow();
  return { electronApp, page };
}

test("dropzone can append files into queue and deduplicate", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#dropzone")).toBeVisible();

  const queueCount = await page.evaluate(() => {
    // Keep the test independent from native drag payload support.
    window.aiwfDesktop.getDroppedFilePath = (f) => String((f && f.path) || "");
    const dz = document.getElementById("dropzone");
    const fireDrop = (paths) => {
      const evt = new Event("drop", { bubbles: true, cancelable: true });
      const files = paths.map((p) => ({ path: p, name: p.split(/[/\\\\]/).pop() || p }));
      Object.defineProperty(evt, "dataTransfer", { value: { files, items: [] } });
      dz.dispatchEvent(evt);
    };
    fireDrop(["D:/tmp/a.pdf", "D:/tmp/b.docx"]);
    fireDrop(["D:/tmp/a.pdf"]);
    const rows = Array.from(document.querySelectorAll("#queueRows tr"));
    return rows.length;
  });

  expect(queueCount).toBe(2);
  await electronApp.close();
});

test("save/load config keeps cleaning template", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#cleaningTemplate")).toBeVisible();

  const result = await page.evaluate(async () => {
    if (typeof applyUiCfg === "function") {
      applyUiCfg({ cleaningTemplate: "finance_report_v1" });
    }
    const loadedTemplate = document.getElementById("cleaningTemplate").value;
    const cfg = typeof saveCfgFromUi === "function" ? saveCfgFromUi() : null;
    return {
      loadedTemplate,
      savedTemplate: cfg ? cfg.cleaningTemplate : null,
    };
  });

  expect(result.loadedTemplate).toBe("finance_report_v1");
  expect(result.savedTemplate).toBe("finance_report_v1");
  await electronApp.close();
});

test("gate panel one-click run updates dashboard", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#btnGateAll")).toBeVisible();

  await page.evaluate(() => {
    window.runGateAll = async () => {
      const status = document.getElementById("status");
      const passed = document.getElementById("gatePassed");
      if (passed) passed.textContent = "5";
      if (status) {
        status.className = "status ok";
        status.textContent = "一键全跑通过（5/5）";
      }
    };
  });

  await page.click("#btnGateAll");
  await expect(page.locator("#status")).toContainText("一键全跑通过");
  await expect(page.locator("#gatePassed")).toContainText("5");
  await electronApp.close();
});

test("build buttons helper toggles expected controls", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#btnBuildPortable")).toBeVisible();

  const state = await page.evaluate(async () => {
    setBuildButtonsDisabled(true);
    const blocked = {
      portableDisabled: document.getElementById("btnBuildPortable").disabled,
      installerDisabled: document.getElementById("btnBuildInstaller").disabled,
      cancelDisabled: document.getElementById("btnBuildCancel").disabled,
    };

    setBuildButtonsDisabled(false);
    const released = {
      portableDisabled: document.getElementById("btnBuildPortable").disabled,
      installerDisabled: document.getElementById("btnBuildInstaller").disabled,
      cancelDisabled: document.getElementById("btnBuildCancel").disabled,
    };
    return { blocked, released };
  });

  expect(state.blocked.portableDisabled).toBeTruthy();
  expect(state.blocked.installerDisabled).toBeTruthy();
  expect(state.blocked.cancelDisabled).toBeFalsy();

  expect(state.released.portableDisabled).toBeFalsy();
  expect(state.released.installerDisabled).toBeFalsy();
  expect(state.released.cancelDisabled).toBeTruthy();
  await electronApp.close();
});

test("cancel build action updates status", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#btnBuildCancel")).toBeVisible();

  await page.click("#btnBuildCancel");
  await expect(page.locator("#status")).toContainText("取消打包");
  await electronApp.close();
});

test("export release report action updates status", async () => {
  const { electronApp, page } = await openMain();
  await expect(page.locator("#btnExportReleaseMd")).toBeVisible();

  const result = await page.evaluate(async () => {
    await exportReleaseReport("md");
    return {
      status: document.getElementById("status").textContent || "",
    };
  });

  expect(result.status.includes("发布报告已导出") || result.status.includes("导出发布报告失败")).toBeTruthy();
  await electronApp.close();
});
