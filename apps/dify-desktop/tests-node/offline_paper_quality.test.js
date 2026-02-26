const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { rowTextForQuality, isLikelyCorruptedText, writeQualityReport } = require("../offline_paper");

test("rowTextForQuality builds evaluable text for table rows without text field", () => {
  const row = {
    source_file: "a.xlsx",
    source_type: "table",
    row_no: 1,
    论点: "短视频提高学习效率",
    证据: "样本 128 人，平均提升 12%",
  };
  const txt = rowTextForQuality(row);
  assert.match(txt, /短视频提高学习效率/);
  assert.equal(isLikelyCorruptedText(txt), false);
});

test("writeQualityReport does not mark all empty-text table rows as gibberish by default", () => {
  const rows = [
    { source_file: "a.xlsx", source_type: "table", row_no: 1, 标题: "研究一", 结论: "有效" },
    { source_file: "a.xlsx", source_type: "table", row_no: 2, 标题: "研究二", 结论: "部分有效" },
  ];
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "aiwf-quality-"));
  const fp = path.join(dir, "quality_report.md");
  writeQualityReport(fp, rows, [], [], {});
  const md = fs.readFileSync(fp, "utf8");
  assert.match(md, /text_evaluable_rows\): 2\/2/);
  assert.match(md, /乱码疑似率\(gibberish_ratio\): 0.0%/);
});

