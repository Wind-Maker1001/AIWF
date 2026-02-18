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
  await expect(page.locator("#status")).toContainText("没有新增文件");
  await electronApp.close();
});
