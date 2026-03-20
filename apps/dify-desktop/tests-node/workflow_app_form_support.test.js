const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadAppFormSchemaSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-form-schema-support.js")).href;
  return import(file);
}

async function loadAppFormRunParamsSupportModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/app-form-run-params-support.js")).href;
  return import(file);
}

test("workflow app form support normalizes schema objects and rows", async () => {
  const {
    normalizeAppSchemaObjectValue,
    appSchemaRowsFromSchemaObject,
    parseSchemaDefaultValue,
  } = await loadAppFormSchemaSupportModule();

  const normalized = normalizeAppSchemaObjectValue({
    title: { type: "string", required: true, default: "demo", description: "标题" },
    empty: null,
    " ": { type: "number" },
  });

  assert.deepEqual(normalized, {
    title: { type: "string", required: true, default: "demo", description: "标题" },
    empty: { type: "string" },
  });
  assert.deepEqual(appSchemaRowsFromSchemaObject(normalized), [
    { key: "title", type: "string", required: true, defaultText: '"demo"', description: "标题" },
    { key: "empty", type: "string", required: false, defaultText: "", description: "" },
  ]);
  assert.deepEqual(parseSchemaDefaultValue('{"a":1}'), { a: 1 });
  assert.equal(parseSchemaDefaultValue("demo"), "demo");
  assert.equal(parseSchemaDefaultValue(""), undefined);
});

test("workflow app form support builds and collects run params", async () => {
  const {
    defaultRunParamValueForRule,
    buildRunParamsFromSchema,
    collectRunParamsControls,
    parseJsonObjectText,
  } = await loadAppFormRunParamsSupportModule();

  assert.equal(defaultRunParamValueForRule({ type: "number" }), 0);
  assert.deepEqual(defaultRunParamValueForRule({ type: "object" }), {});

  assert.deepEqual(buildRunParamsFromSchema({
    a: { type: "string" },
    b: { type: "boolean", default: true },
  }, { a: "x" }), {
    a: "x",
    b: true,
  });

  const params = collectRunParamsControls([
    { dataset: { appRunParam: "a", appRunType: "string" }, value: "x" },
    { dataset: { appRunParam: "b", appRunType: "boolean" }, value: "true" },
    { dataset: { appRunParam: "c", appRunType: "number" }, value: "3" },
    { dataset: { appRunParam: "d", appRunType: "array" }, value: '["y"]' },
  ]);
  assert.deepEqual(params, { a: "x", b: true, c: 3, d: ["y"] });
  assert.deepEqual(parseJsonObjectText('{"x":1}', {}), { x: 1 });
  assert.deepEqual(parseJsonObjectText("{bad", {}), {});
});
