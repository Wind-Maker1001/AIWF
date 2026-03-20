const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadTemplateParamStateModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/template-ui-param-state.js")).href;
  return import(file);
}

test("workflow template param state reads controls with typed values", async () => {
  const { collectTemplateParamsFromControls } = await loadTemplateParamStateModule();
  const out = collectTemplateParamsFromControls([
    { dataset: { tparam: "limit", ttype: "number" }, value: "12", disabled: false },
    { dataset: { tparam: "enabled", ttype: "boolean" }, value: "true", disabled: false },
    { dataset: { tparam: "tags", ttype: "array" }, value: '["a","b"]', disabled: false },
    { dataset: { tparam: "meta", ttype: "object" }, value: '{"x":1}', disabled: false },
    { dataset: { tparam: "name", ttype: "string" }, value: "demo", disabled: false },
    { dataset: { tparam: "skip", ttype: "string" }, value: "x", disabled: true },
  ]);

  assert.deepEqual(out, {
    limit: 12,
    enabled: true,
    tags: ["a", "b"],
    meta: { x: 1 },
    name: "demo",
  });
});

test("workflow template param state merges existing values with schema defaults", async () => {
  const { mergeTemplateParamsWithSchema } = await loadTemplateParamStateModule();
  const out = mergeTemplateParamsWithSchema({
    a: { type: "string", default: "x" },
    b: { type: "number", default: 2 },
    c: { type: "array" },
  }, { b: 9 });

  assert.deepEqual(out, { a: "x", b: 9, c: [] });
});

test("workflow template param state rejects non-object json text", async () => {
  const { parseTemplateParamsText, readTemplateParamsLooseText } = await loadTemplateParamStateModule();

  assert.deepEqual(readTemplateParamsLooseText('{"ok":1}'), { ok: 1 });
  assert.deepEqual(readTemplateParamsLooseText("[1,2]"), {});
  assert.throws(() => parseTemplateParamsText("[1,2]"), /模板参数必须是 JSON 对象/);
});
