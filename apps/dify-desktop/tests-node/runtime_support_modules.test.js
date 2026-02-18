const test = require("node:test");
const assert = require("node:assert/strict");
const os = require("os");
const fs = require("fs");
const path = require("path");
const iconv = require("iconv-lite");
const { createRuntimeSupport } = require("../main_runtime_support");

function buildSupport(tempRoot) {
  return createRuntimeSupport({
    app: { getPath: () => tempRoot },
    fs,
    path,
    execFileSync: () => "",
    fork: () => { throw new Error("not needed in this test"); },
    iconv,
  });
}

test("runtime support exports expected capabilities", () => {
  const support = buildSupport(os.tmpdir());
  const keys = [
    "inspectFileEncoding",
    "toUtf8FileIfNeeded",
    "checkChineseOfficeFonts",
    "checkTesseractRuntime",
    "checkPdftoppmRuntime",
    "checkTesseractLangs",
    "runOfflineCleaningInWorker",
    "runViaBaseApi",
    "baseHealth",
  ];
  for (const key of keys) assert.equal(typeof support[key], "function");
});

test("encoding module detects UTF-8 text", () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-enc-"));
  const file = path.join(dir, "a.txt");
  fs.writeFileSync(file, "你好，AIWF", "utf8");
  const support = buildSupport(dir);
  const report = support.inspectFileEncoding(file);
  assert.equal(report.kind, "text");
  assert.ok(["utf-8", "utf-8-bom", "uncertain", "gb18030"].includes(report.encoding));
});
