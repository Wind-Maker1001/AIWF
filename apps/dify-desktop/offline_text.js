const fs = require("fs");
const { TextDecoder } = require("util");
const iconv = require("iconv-lite");

function decodeBufferWithEncoding(buf, encoding) {
  const enc = String(encoding || "").toLowerCase();
  if (enc === "gb18030" || enc === "gbk") {
    try {
      return iconv.decode(buf, enc);
    } catch {
      return "";
    }
  }
  try {
    const td = new TextDecoder(encoding, { fatal: false });
    return td.decode(buf);
  } catch {
    return "";
  }
}

function decodeBufferSmart(buf) {
  if (buf.length >= 2) {
    if (buf[0] === 0xff && buf[1] === 0xfe) return decodeBufferWithEncoding(buf, "utf-16le");
  }
  if (buf.length >= 3) {
    if (buf[0] === 0xef && buf[1] === 0xbb && buf[2] === 0xbf) return decodeBufferWithEncoding(buf, "utf-8");
  }
  const utf8 = decodeBufferWithEncoding(buf, "utf-8");
  const gb = decodeBufferWithEncoding(buf, "gb18030");
  const utf8Bad = (utf8.match(/\uFFFD/g) || []).length + (utf8.match(/[ÃÂ¤¥¦§¨©ª«¬®¯°±²³´µ¶·¸¹º»¼½¾¿]/g) || []).length;
  const gbCjk = (gb.match(/[\u4E00-\u9FFF]/g) || []).length;
  const utf8Cjk = (utf8.match(/[\u4E00-\u9FFF]/g) || []).length;
  if (utf8Bad > 0 || gbCjk > utf8Cjk) return gb || utf8;
  return utf8 || gb || buf.toString("utf8");
}

function readTextFileSmart(filePath) {
  const buf = fs.readFileSync(filePath);
  return decodeBufferSmart(buf);
}

module.exports = {
  decodeBufferWithEncoding,
  decodeBufferSmart,
  readTextFileSmart,
};
