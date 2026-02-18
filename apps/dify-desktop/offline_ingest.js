const { createOfflineIngestParsers } = require("./offline_ingest_parsers");

function createOfflineIngest(deps = {}) {
  const {
    normalizeCell,
    normalizeAmount,
  } = deps;

  function splitTextToChunks(text) {
    return String(text || "")
      .split(/\r?\n+/)
      .map((s) => s.trim())
      .filter((s) => s.length > 0)
      .slice(0, 5000);
  }

  function rowFromText(filePath, sourceType, txt, idx) {
    return {
      source_file: filePath,
      source_type: sourceType,
      row_no: idx + 1,
      text: txt,
      amount: "",
    };
  }

  function toRowsFromWorksheet(ws, filePath) {
    const rows = [];
    const headers = [];
    ws.getRow(1).eachCell((cell, idx) => {
      const h = normalizeCell(cell.value);
      headers[idx - 1] = h || `col_${idx}`;
    });

    for (let r = 2; r <= ws.rowCount; r += 1) {
      const row = ws.getRow(r);
      const item = { source_file: filePath, source_type: "table", row_no: r - 1 };
      let nonEmpty = 0;
      for (let c = 1; c <= headers.length; c += 1) {
        const key = headers[c - 1];
        const val = normalizeCell(row.getCell(c).value);
        if (val !== "") nonEmpty += 1;
        item[key] = val;
      }
      if (nonEmpty > 0) rows.push(item);
    }
    return rows;
  }

  const parsers = createOfflineIngestParsers({
    ...deps,
    splitTextToChunks,
    rowFromText,
    toRowsFromWorksheet,
  });

  function collectInputFiles(params = {}) {
    const files = [];
    const add = (p) => {
      let v = p;
      if (v && typeof v === "object") {
        v = v.path || v.file || v.filePath || v.value || v.source_file || "";
      }
      const s = String(v || "").trim();
      if (!s) return;
      files.push(s);
    };

    add(params.input_csv_path);
    add(params.input_path);

    if (Array.isArray(params.input_files)) {
      params.input_files.forEach(add);
    } else if (typeof params.input_files === "string") {
      const raw = String(params.input_files).trim();
      if (raw.startsWith("[") && raw.endsWith("]")) {
        try {
          const arr = JSON.parse(raw);
          if (Array.isArray(arr)) arr.forEach(add);
        } catch {
          raw.split(/\r?\n/).forEach(add);
        }
      } else {
        raw.split(/\r?\n/).forEach(add);
      }
    }

    return Array.from(new Set(files));
  }

  async function readInputRows(params, warnings, runtime = {}) {
    const files = collectInputFiles(params);
    if (files.length === 0) {
      return [
        { source_file: "builtin", source_type: "demo", row_no: 1, id: "1", amount: "100.25", text: "内置示例-就业数据" },
        { source_file: "builtin", source_type: "demo", row_no: 2, id: "2", amount: "230.50", text: "内置示例-教育数据" },
        { source_file: "builtin", source_type: "demo", row_no: 3, id: "2", amount: "230.50", text: "内置示例-重复行" },
        { source_file: "builtin", source_type: "demo", row_no: 4, id: "3", amount: "-11.00", text: "内置示例-异常值" },
      ];
    }

    const all = [];
    for (const f of files) {
      // eslint-disable-next-line no-await-in-loop
      const rows = await parsers.parseOneFile(f, warnings, params, runtime);
      all.push(...rows);
    }
    return all;
  }

  function cleanRows(rawRows) {
    let filteredRows = 0;
    const normalized = rawRows
      .map((row) => {
        const out = {};
        Object.keys(row || {}).forEach((k) => {
          out[String(k).trim()] = normalizeCell(row[k]);
        });
        return out;
      })
      .filter((r) => {
        const nonEmpty = Object.values(r).some((v) => String(v).trim() !== "");
        if (!nonEmpty) filteredRows += 1;
        return nonEmpty;
      });

    const dedup = [];
    const seen = new Set();
    const seenText = new Set();
    normalized.forEach((r) => {
      const txt = String(r.text || "").trim();
      if (txt.length >= 18) {
        const tk = txt.toLowerCase().replace(/[^a-z0-9\u4e00-\u9fff]/g, "");
        if (tk.length > 16) {
          if (seenText.has(tk)) return;
          seenText.add(tk);
        }
      }
      const key = JSON.stringify(r);
      if (!seen.has(key)) {
        dedup.push(r);
        seen.add(key);
      }
    });

    let invalidRows = 0;
    const finalRows = dedup.map((r) => {
      const out = { ...r };
      const amountKey = Object.keys(out).find((k) => k.toLowerCase() === "amount");
      if (amountKey) {
        const n = normalizeAmount(out[amountKey]);
        if (n === null && out[amountKey] !== "") invalidRows += 1;
        if (n !== null) out[amountKey] = n;
      }
      return out;
    });

    const inputRows = rawRows.length;
    const outputRows = finalRows.length;
    const duplicateRemoved = normalized.length - dedup.length;
    return {
      rows: finalRows,
      quality: {
        input_rows: inputRows,
        output_rows: outputRows,
        filtered_rows: filteredRows,
        invalid_rows: invalidRows,
        duplicate_rows_removed: duplicateRemoved,
      },
    };
  }

  function ruleParam(params, key) {
    const p = (params && typeof params === "object") ? params : {};
    if (p.rules && typeof p.rules === "object" && p.rules[key] !== undefined) return p.rules[key];
    return p[key];
  }

  function toIntOrNull(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return Math.trunc(n);
  }

  function toFloatOrNull(v) {
    if (v === null || v === undefined || v === "") return null;
    const n = Number(v);
    if (!Number.isFinite(n)) return null;
    return n;
  }

  function toBool(v, def = false) {
    if (v === null || v === undefined) return def;
    if (typeof v === "boolean") return v;
    const s = String(v).trim().toLowerCase();
    if (!s) return def;
    return ["1", "true", "yes", "y", "on"].includes(s);
  }

  function applyQualityGates(quality, params = {}) {
    const inputRows = Number(quality?.input_rows || 0);
    const outputRows = Number(quality?.output_rows || 0);
    const invalidRows = Number(quality?.invalid_rows || 0);
    const filteredRows = Number(quality?.filtered_rows || 0);

    const maxInvalidRows = toIntOrNull(ruleParam(params, "max_invalid_rows"));
    const maxFilteredRows = toIntOrNull(ruleParam(params, "max_filtered_rows"));
    const minOutputRows = toIntOrNull(ruleParam(params, "min_output_rows"));
    const maxInvalidRatio = toFloatOrNull(ruleParam(params, "max_invalid_ratio"));
    const allowEmpty = toBool(ruleParam(params, "allow_empty_output"), true);

    if (maxInvalidRows !== null && invalidRows > maxInvalidRows) {
      throw new Error(`quality gate failed: invalid_rows=${invalidRows} exceeds max_invalid_rows=${maxInvalidRows}`);
    }
    if (maxFilteredRows !== null && filteredRows > maxFilteredRows) {
      throw new Error(`quality gate failed: filtered_rows=${filteredRows} exceeds max_filtered_rows=${maxFilteredRows}`);
    }
    if (minOutputRows !== null && outputRows < minOutputRows) {
      throw new Error(`quality gate failed: output_rows=${outputRows} below min_output_rows=${minOutputRows}`);
    }
    if (maxInvalidRatio !== null) {
      const ratio = inputRows > 0 ? (invalidRows / inputRows) : 0;
      if (ratio > maxInvalidRatio) {
        throw new Error(`quality gate failed: invalid_ratio=${ratio.toFixed(6)} exceeds max_invalid_ratio=${Number(maxInvalidRatio).toFixed(6)}`);
      }
    }
    if (!allowEmpty && outputRows <= 0) {
      throw new Error("quality gate failed: output_rows=0 while allow_empty_output=false");
    }
    return {
      evaluated: true,
      max_invalid_rows: maxInvalidRows,
      max_filtered_rows: maxFilteredRows,
      min_output_rows: minOutputRows,
      max_invalid_ratio: maxInvalidRatio,
      allow_empty_output: allowEmpty,
    };
  }

  return {
    readInputRows,
    cleanRows,
    applyQualityGates,
  };
}

module.exports = {
  createOfflineIngest,
};
