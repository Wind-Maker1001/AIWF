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

  assert.match(homeApp, /已切换到 Legacy Workflow Studio/);
  assert.match(homeApp, /预检通过，可以开始生成/);
  assert.match(homeRuntime, /编码检测：队列为空/);
  assert.match(homeRuntime, /OCR 运行时：未安装 Tesseract/);
  assert.match(homeGate, /最近打包门禁状态：通过/);
  assert.match(homeGate, /打包前检查通过/);

  assert.doesNotMatch(homeApp, /\?\?\?\? Workflow Studio \?\?\?/);
  assert.doesNotMatch(homeRuntime, /缂傛牜鐖滃Λ鈧ù/);
  assert.doesNotMatch(homeGate, /閺堚偓鏉╂垶澧﹂崠鍛存，缁?/);
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

  assert.equal(workflowStaticConfig.includes("闁哄鐗冮弲鐐侯敊閹寸姵瀚氬ù锝囨嚀缁侊附绻涚紙鐘靛妽闁圭顦遍惀?"), false);
  assert.equal(workflowStaticConfig.includes("闂佺硶鏅炲▍锝夈€?"), false);
  assert.equal(workflowStaticConfig.includes("婵帗绋掗…鍫ヮ敇婵犳艾绠ラ柣鐔告緲濞?"), false);
});

test("workflow contracts and reports keep readable workflow copy", () => {
  const workflowContract = readRepoFile("apps/dify-desktop/renderer/workflow/workflow-contract.js");
  const preflightController = readRepoFile("apps/dify-desktop/renderer/workflow/preflight-controller-ui.js");
  const graphShellUi = readRepoFile("apps/dify-desktop/renderer/workflow/graph-shell-ui.js");
  const workflowReports = readRepoFile("apps/dify-desktop/workflow_ipc_reports.js");
  const compareUi = readRepoFile("apps/dify-desktop/renderer/workflow/support-ui-run-compare.js");
  const runPayloadSupport = readRepoFile("apps/dify-desktop/renderer/workflow/run-payload-support.js");

  assert.match(workflowContract, /请补齐该必填字段。/);
  assert.match(workflowContract, /无法解析流程 JSON，请检查文件是否损坏或格式不正确。/);
  assert.match(preflightController, /workflow 契约校验请求失败/);
  assert.match(preflightController, /能力发现失败/);
  assert.match(graphShellUi, /自由编排流程/);
  assert.match(workflowReports, /# AIWF 运行对比报告/);
  assert.match(workflowReports, /# AIWF 预检报告/);
  assert.match(workflowReports, /# AIWF 模板验收报告/);
  assert.match(compareUi, /运行对比完成/);
  assert.match(runPayloadSupport, /AIWF 熟肉语料/);

  assert.doesNotMatch(workflowContract, /鐠囩柉|workflow 濂戠害鏍￠獙/);
  assert.doesNotMatch(preflightController, /Rust Endpoint 娑撹櫣鈹|濂戠害鏍￠獙/);
  assert.doesNotMatch(graphShellUi, /鑷敱缂栨帓娴佺▼/);
  assert.doesNotMatch(workflowReports, /鏉╂劘顢|妫板嫭顥|濡剝婢樻灞炬暪/);
  assert.doesNotMatch(compareUi, /鏉╂劘顢|鐎佃鐦/);
  assert.doesNotMatch(runPayloadSupport, /鐔熻倝璇枡/);
});

test("key docs keep cleaned onboarding copy", () => {
  const quickstartDesktop = readRepoFile("docs/quickstart_desktop_offline.md");
  const goalFreeze = readRepoFile("docs/archive/v1_1_goal_freeze.md");

  assert.match(quickstartDesktop, /离线本地模式（推荐）/);
  assert.match(quickstartDesktop, /仅输出 Markdown/);
  assert.match(goalFreeze, /工作流编排：支持节点拖拽/);
  assert.match(goalFreeze, /冻结日期：`2026-02-18`/);

  assert.doesNotMatch(quickstartDesktop, /缂佸倽宕甸崵搴ㄥ嫉椤掆偓濠€鏉懳?/);
  assert.doesNotMatch(goalFreeze, /鐎规悶鍎扮紞鏂棵规担铏规そ闁?/);
});
