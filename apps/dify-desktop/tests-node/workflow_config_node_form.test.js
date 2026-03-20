const test = require("node:test");
const assert = require("node:assert/strict");
const path = require("node:path");
const { pathToFileURL } = require("node:url");

async function loadConfigNodeFormModule() {
  const file = pathToFileURL(path.resolve(__dirname, "../renderer/workflow/config-ui-node-form.js")).href;
  return import(file);
}

test("workflow config node form parses config text and field values", async () => {
  const {
    parseNodeConfigTextValue,
    parseFieldValue,
    toFieldDisplayValue,
    buildNodeConfigFormSchema,
  } = await loadConfigNodeFormModule();

  assert.deepEqual(parseNodeConfigTextValue('{"a":1}'), { a: 1 });
  assert.throws(() => parseNodeConfigTextValue("[1,2]"), /配置必须是 JSON 对象/);
  assert.throws(() => parseNodeConfigTextValue("{bad"), /配置必须是合法 JSON/);

  assert.equal(toFieldDisplayValue("csv", ["a", "b"]), "a,b");
  assert.equal(toFieldDisplayValue("bool", true), "true");
  assert.equal(toFieldDisplayValue("number", 3), "3");

  assert.equal(parseFieldValue("number", "5"), 5);
  assert.deepEqual(parseFieldValue("csv", "a, b ,,c"), ["a", "b", "c"]);
  assert.equal(parseFieldValue("bool", "yes"), true);
  assert.equal(parseFieldValue("bool", ""), false);
  assert.deepEqual(parseFieldValue("json", '{"x":1}'), { x: 1 });

  const schema = buildNodeConfigFormSchema("demo", {
    demo: [{ key: "limit", label: "Limit", type: "number" }],
  });
  assert.equal(schema[0].key, "limit");
  assert.equal(schema.at(-1).key, "output_map");
});

test("workflow config node form reads controls into node config object", async () => {
  const { parseNodeConfigFormElement } = await loadConfigNodeFormModule();
  const form = {
    querySelectorAll() {
      return [
        { dataset: { key: "limit", kind: "number" }, value: "9" },
        { dataset: { key: "enabled", kind: "bool" }, value: "true" },
        { dataset: { key: "tags", kind: "csv" }, value: "a,b" },
        { dataset: { key: "meta", kind: "json" }, value: '{"x":2}' },
      ];
    },
  };

  const out = parseNodeConfigFormElement(form, { keep: 1 }, [
    { key: "limit", type: "number" },
    { key: "enabled", type: "bool" },
  ]);

  assert.deepEqual(out, {
    keep: 1,
    limit: 9,
    enabled: true,
    tags: ["a", "b"],
    meta: { x: 2 },
  });
});
