const test = require("node:test");
const assert = require("node:assert/strict");

const { createOfflineOutputs } = require("../offline_outputs");

function makeOutputs() {
  return createOfflineOutputs({
    resolveOfficeTheme: () => ({ bg: "FFFFFF", title: "test", primary: "0F6CBD", secondary: "115EA3" }),
    resolveOfficeFont: () => "Microsoft YaHei",
    resolveOfficeLayout: () => ({}),
  });
}

test("filterRowsForOffice can filter question-heavy rows derived from non-text fields", () => {
  const out = makeOutputs();
  const rows = [
    { source_file: "a.txt", source_type: "txt", row_no: 1, note: "????????????????????" },
    { source_file: "a.txt", source_type: "txt", row_no: 2, note: "正常句子内容" },
  ];
  const ret = out.filterRowsForOffice(rows);
  assert.equal(ret.filtered, 1);
  assert.equal(ret.rows.length, 1);
});

test("buildDebatePreview works when rows do not have text field", () => {
  const out = makeOutputs();
  const rows = [
    {
      source_file: "b.txt",
      source_type: "txt",
      row_no: 1,
      topic: "校园手机管理",
      claim: "限制无关短视频时长可提升学习效率",
      evidence: "实验班数据显示四周后平均成绩提高 12%",
    },
  ];
  const ret = out.buildDebatePreview(rows, {}, 5);
  assert.equal(ret.total, 1);
  assert.equal(ret.rows.length, 1);
  assert.match(String(ret.rows[0].content || ""), /提高 12%/);
});

test("buildEvidenceHighlights works when rows do not have text field", () => {
  const out = makeOutputs();
  const rows = [
    {
      source_file: "c.txt",
      source_type: "txt",
      row_no: 1,
      section: "研究结论",
      summary: "追踪调查显示，控制娱乐性刷屏后，学习投入时长稳定上升并减少拖延，课堂参与度和作业完成率也同步提升。",
    },
  ];
  const ret = out.buildEvidenceHighlights(rows, 4);
  assert.equal(ret.length, 1);
  assert.match(String(ret[0].text || ""), /学习投入时长/);
});
