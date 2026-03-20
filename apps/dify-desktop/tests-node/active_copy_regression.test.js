const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

const repoRoot = path.resolve(__dirname, "..", "..", "..");

function readRepoFile(relativePath) {
  return fs.readFileSync(path.join(repoRoot, relativePath), "utf8");
}

test("home entry copy stays readable in key desktop files", () => {
  const homeApp = readRepoFile("apps/dify-desktop/renderer/home-app.js");
  const homeRuntime = readRepoFile("apps/dify-desktop/renderer/home-runtime.js");
  const homeGate = readRepoFile("apps/dify-desktop/renderer/home-gate.js");

  assert.match(homeApp, /已切换到 Workflow Studio/);
  assert.match(homeApp, /预检通过，可以开始生成/);
  assert.match(homeRuntime, /编码检测：队列为空/);
  assert.match(homeRuntime, /OCR 运行时：未安装 Tesseract/);
  assert.match(homeGate, /最近打包门禁状态：通过/);
  assert.match(homeGate, /打包前检查通过/);

  assert.doesNotMatch(homeApp, /\?\?\?\? Workflow Studio \?\?\?/);
  assert.doesNotMatch(homeRuntime, /缂栫爜妫€娴/);
  assert.doesNotMatch(homeGate, /鏈€杩戞墦鍖呴棬绂/);
});

test("workflow template copy stays readable in workflow static config", () => {
  const workflowApp = readRepoFile("apps/dify-desktop/renderer/workflow/app.js");
  const workflowStaticConfig = readRepoFile("apps/dify-desktop/renderer/workflow/static-config.js");

  assert.match(workflowApp, /static-config\.js/);

  assert.equal(workflowStaticConfig.includes("辩论证据流水线v1"), true);
  assert.equal(workflowStaticConfig.includes("财报清洗流水线v2"), true);
  assert.equal(workflowStaticConfig.includes('group: "基础"'), true);
  assert.equal(workflowStaticConfig.includes('group: "AI 参数"'), true);
  assert.equal(workflowStaticConfig.includes('label: "导出熟肉包"'), true);
  assert.equal(workflowStaticConfig.includes('label: "审核键"'), true);

  assert.equal(workflowStaticConfig.includes("閺夊牃鏅為鎴犳嫚娴ｇ懓绁︽繛缈犵劍閹稿鐥?"), false);
  assert.equal(workflowStaticConfig.includes("闁糕晞娅ｉ、"), false);
  assert.equal(workflowStaticConfig.includes("濮掓稒顭堥濠氬箥閻熸澘娅?"), false);
});

test("key docs keep cleaned onboarding copy", () => {
  const quickstartDesktop = readRepoFile("docs/quickstart_desktop_offline.md");
  const goalFreeze = readRepoFile("docs/archive/v1_1_goal_freeze.md");

  assert.match(quickstartDesktop, /离线本地模式（推荐）/);
  assert.match(quickstartDesktop, /仅输出 Markdown/);
  assert.match(goalFreeze, /工作流编排：支持节点拖拽/);
  assert.match(goalFreeze, /冻结日期：`2026-02-18`/);

  assert.doesNotMatch(quickstartDesktop, /缁傝崵鍤庨張顒€婀村Ο/);
  assert.doesNotMatch(goalFreeze, /瀹搞儰缍斿ù浣虹椽閹?/);
});
