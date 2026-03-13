const test = require("node:test");
const assert = require("node:assert/strict");
const { createOfflineIngest } = require("../offline_ingest");

function normalizeCell(v) {
  if (v === null || v === undefined) return "";
  return String(v).trim();
}

function normalizeAmount(v) {
  if (v === null || v === undefined) return null;
  const s = String(v).replace(/[,，\s$¥￥]/g, "").trim();
  if (!s) return null;
  const n = Number(s);
  return Number.isFinite(n) ? n : null;
}

test("precheckRows reports missing required fields and bad amount rate", () => {
  const ingest = createOfflineIngest({ normalizeCell, normalizeAmount });
  const rawRows = [
    { ID: "1", Amt: "100.5" },
    { ID: "2", Amt: "not-number" },
  ];
  const params = {
    rules: {
      rename_map: { ID: "id", Amt: "amount" },
      required_fields: ["id", "currency"],
      casts: { id: "int", amount: "float", currency: "string" },
      max_invalid_rows: 0,
    },
  };

  const out = ingest.precheckRows(rawRows, params);
  assert.equal(out.ok, false);
  assert.deepEqual(out.missing_required_fields, ["currency"]);
  assert.equal(out.amount_field, "amount");
  assert.equal(out.amount_non_empty, 2);
  assert.equal(out.amount_convertible, 1);
  assert.equal(out.amount_convert_rate, 0.5);
});

test("precheckRows passes for finance-friendly rows", () => {
  const ingest = createOfflineIngest({ normalizeCell, normalizeAmount });
  const rawRows = [
    { ID: "101", Amt: "100.5", currency: "CNY" },
    { ID: "102", Amt: "230.1", currency: "CNY" },
  ];
  const params = {
    rules: {
      rename_map: { ID: "id", Amt: "amount" },
      required_fields: ["id", "amount"],
      casts: { id: "int", amount: "float", currency: "string" },
      filters: [{ field: "amount", op: "gte", value: 0 }],
      max_invalid_rows: 0,
      min_output_rows: 1,
      allow_empty_output: false,
    },
  };

  const out = ingest.precheckRows(rawRows, params);
  assert.equal(out.ok, true);
  assert.equal(out.quality_gate_ok, true);
  assert.equal(out.amount_convert_rate, 1);
});

test("precheckRows honors configurable amount convert rate threshold", () => {
  const ingest = createOfflineIngest({ normalizeCell, normalizeAmount });
  const rawRows = [
    { ID: "1", Amt: "100.0", currency: "CNY" },
    { ID: "2", Amt: "bad-value", currency: "CNY" },
  ];
  const out = ingest.precheckRows(rawRows, {
    rules: {
      rename_map: { ID: "id", Amt: "amount" },
      precheck_amount_convert_rate_min: 0.4,
    },
  });
  assert.equal(out.ok, true);
  assert.equal(out.amount_convert_rate, 0.5);
  assert.equal(out.amount_convert_rate_required, 0.4);
});

test("readInputRows rejects missing input files by default", async () => {
  const ingest = createOfflineIngest({ normalizeCell, normalizeAmount });

  await assert.rejects(
    () => ingest.readInputRows({}, [], {}),
    /未提供输入文件|输入文件/i,
  );
});
