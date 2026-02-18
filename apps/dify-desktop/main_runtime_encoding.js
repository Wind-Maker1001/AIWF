function createEncodingSupport({ app, fs, path, iconv }) {
  function scoreDecodedText(txt) {
    if (!txt) return Number.NEGATIVE_INFINITY;
    const replacement = (txt.match(/\uFFFD/g) || []).length;
    const mojibake = (txt.match(/[ÃÂ¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]/g) || []).length;
    const qmark = (txt.match(/\?/g) || []).length;
    const cjk = (txt.match(/[\u4E00-\u9FFF]/g) || []).length;
    const printable = (txt.match(/[A-Za-z0-9\u4E00-\u9FFF]/g) || []).length;
    return printable + cjk * 3 - replacement * 60 - mojibake * 8 - qmark * 2;
  }

  function decodeBuf(buf, enc) {
    try {
      if (enc === "gb18030") return iconv.decode(buf, "gb18030");
      return buf.toString(enc);
    } catch {
      return "";
    }
  }

  function inspectFileEncoding(filePath) {
    const ext = String(path.extname(filePath || "")).toLowerCase();
    if (![".txt", ".csv"].includes(ext)) {
      return { path: filePath, kind: "skip", encoding: "n/a", confidence: 0 };
    }
    if (!fs.existsSync(filePath)) {
      return { path: filePath, kind: "missing", encoding: "missing", confidence: 0, warning: "文件不存在" };
    }
    const buf = fs.readFileSync(filePath);
    if (buf.length >= 3 && buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) {
      return { path: filePath, kind: "text", encoding: "utf-8-bom", confidence: 100 };
    }
    if (buf.length >= 2 && buf[0] === 0xff && buf[1] === 0xfe) {
      return { path: filePath, kind: "text", encoding: "utf-16le-bom", confidence: 100 };
    }
    const utf8 = decodeBuf(buf, "utf8");
    const gb = decodeBuf(buf, "gb18030");
    const sUtf8 = scoreDecodedText(utf8);
    const sGb = scoreDecodedText(gb);
    if (sGb > sUtf8 + 5) return { path: filePath, kind: "text", encoding: "gb18030", confidence: 85 };
    if (sUtf8 > sGb + 5) return { path: filePath, kind: "text", encoding: "utf-8", confidence: 85 };
    return {
      path: filePath,
      kind: "text",
      encoding: "uncertain",
      confidence: 50,
      warning: "编码不确定，建议另存为 UTF-8 后再导入",
    };
  }

  function ensureEncodingCacheDir() {
    const dir = path.join(app.getPath("userData"), "encoding_cache");
    fs.mkdirSync(dir, { recursive: true });
    return dir;
  }

  function safeBaseName(filePath) {
    const ext = path.extname(filePath);
    const stem = path.basename(filePath, ext).replace(/[^a-zA-Z0-9._-]/g, "_");
    return { stem, ext };
  }

  function toUtf8FileIfNeeded(filePath) {
    const report = inspectFileEncoding(filePath);
    if (report.kind !== "text") return { source: filePath, output: filePath, converted: false, report };
    if (report.encoding === "utf-8" || report.encoding === "utf-8-bom" || report.encoding === "utf-16le-bom") {
      return { source: filePath, output: filePath, converted: false, report };
    }
    const srcBuf = fs.readFileSync(filePath);
    const text = report.encoding === "gb18030" ? iconv.decode(srcBuf, "gb18030") : srcBuf.toString("utf8");
    const cacheDir = ensureEncodingCacheDir();
    const { stem, ext } = safeBaseName(filePath);
    const outPath = path.join(cacheDir, `${Date.now()}_${stem}.utf8${ext}`);
    fs.writeFileSync(outPath, text, "utf8");
    return { source: filePath, output: outPath, converted: true, report };
  }

  return {
    inspectFileEncoding,
    toUtf8FileIfNeeded,
  };
}

module.exports = { createEncodingSupport };
