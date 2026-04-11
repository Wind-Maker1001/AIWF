const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");

test("renderer precheck UI keeps Phase 3 recommendation and block messaging", () => {
  const text = fs.readFileSync(path.resolve(__dirname, "../renderer/home-app.js"), "utf8");
  assert.match(text, /precheck_action/);
  assert.match(text, /模板画像不匹配/);
  assert.match(text, /推荐模板/);
  assert.match(text, /将产生空结果/);
  assert.match(text, /推荐信号不足/);
  assert.match(text, /预检拦截/);
  assert.match(text, /建议调整模板后再运行/);
});
