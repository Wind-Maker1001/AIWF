const fs = require("fs");
const crypto = require("crypto");
const {
  listInstalledFonts: listInstalledRuntimeFonts,
  hasAnyFontFile,
} = require("./offline_runtime");

const IMG_EXT = new Set([".png", ".jpg", ".jpeg", ".bmp", ".webp", ".tif", ".tiff"]);

function createLazyModuleLoader(moduleName) {
  let cached = null;
  let loadError = null;
  return function loadModule() {
    if (cached) return cached;
    if (loadError) throw loadError;
    try {
      cached = require(moduleName);
      return cached;
    } catch (e) {
      loadError = e;
      throw e;
    }
  };
}

const getExcelJSModule = createLazyModuleLoader("exceljs");
const getMammothModule = createLazyModuleLoader("mammoth");
const getImageSizeModule = createLazyModuleLoader("image-size");
const getPdfParseModule = createLazyModuleLoader("pdf-parse");
const getUuidModule = createLazyModuleLoader("uuid");

const ExcelJS = {};
Object.defineProperty(ExcelJS, "Workbook", {
  enumerable: true,
  get() {
    return getExcelJSModule().Workbook;
  },
});

const mammoth = {
  extractRawText(...args) {
    return getMammothModule().extractRawText(...args);
  },
};

const imageSize = {
  imageSize(...args) {
    const mod = getImageSizeModule();
    if (typeof mod === "function") return mod(...args);
    if (mod && typeof mod.imageSize === "function") return mod.imageSize(...args);
    if (mod && typeof mod.default === "function") return mod.default(...args);
    throw new Error("unsupported image-size module shape");
  },
};

function ensureDir(targetPath) {
  fs.mkdirSync(targetPath, { recursive: true });
}

function sha256File(targetPath) {
  const hash = crypto.createHash("sha256");
  hash.update(fs.readFileSync(targetPath));
  return hash.digest("hex");
}

function makeJobId() {
  if (typeof crypto.randomUUID === "function") {
    return crypto.randomUUID().replace(/-/g, "");
  }
  const uuid = getUuidModule();
  if (uuid && typeof uuid.v4 === "function") {
    return uuid.v4().replace(/-/g, "");
  }
  throw new Error("no uuid generator available");
}

function isMissingNodeModuleError(err) {
  if (!err) return false;
  if (String(err.code || "") === "MODULE_NOT_FOUND") return true;
  return /Cannot find module/i.test(String(err.message || err));
}

function normalizeCell(value) {
  if (value === null || value === undefined) return "";
  return String(value).trim();
}

function normalizeAmount(value) {
  if (value === null || value === undefined) return null;
  const normalized = String(value).replace(/[,，\s$¥￥\uFFFD?]/g, "").trim();
  if (!normalized) return null;
  const numberValue = Number(normalized);
  if (!Number.isFinite(numberValue)) return null;
  return Math.round(numberValue * 100) / 100;
}

function resolveOfficeFont(params = {}, warnings = []) {
  const lang = String(params.office_lang || "zh").toLowerCase();
  const installed = listInstalledRuntimeFonts();
  if (lang === "en") return "Calibri";

  const candidates = [
    { family: "Microsoft YaHei", files: ["msyh.ttc", "msyh.ttf", "msyhbd.ttc", "msyhbd.ttf"] },
    { family: "SimHei", files: ["simhei.ttf"] },
    { family: "SimSun", files: ["simsun.ttc", "simsun.ttf"] },
    { family: "DengXian", files: ["deng.ttf", "dengb.ttf"] },
    { family: "Noto Sans CJK SC", files: ["NotoSansCJKsc-Regular.otf", "NotoSansCJK-Regular.ttc"] },
    { family: "Source Han Sans SC", files: ["SourceHanSansSC-Regular.otf"] },
    { family: "Segoe UI", files: ["segoeui.ttf", "segoeuib.ttf"] },
  ];

  for (const candidate of candidates) {
    if (hasAnyFontFile(installed, candidate.files)) return candidate.family;
  }

  warnings.push("未检测到核心中文字体，已回退到 Calibri，可能出现中文显示异常。");
  return "Calibri";
}

async function extractPdfTextFromBuffer(buf) {
  const pdfParse = getPdfParseModule();
  if (typeof pdfParse === "function") {
    const out = await pdfParse(buf);
    return String((out && out.text) || "");
  }
  if (pdfParse && typeof pdfParse.default === "function") {
    const out = await pdfParse.default(buf);
    return String((out && out.text) || "");
  }
  const PDFParse = pdfParse && pdfParse.PDFParse;
  if (typeof PDFParse === "function") {
    let parser = null;
    try {
      parser = new PDFParse({ data: buf });
      const out = await parser.getText();
      return String((out && out.text) || "");
    } finally {
      try {
        if (parser && typeof parser.destroy === "function") parser.destroy();
      } catch {}
    }
  }
  throw new Error("unsupported pdf-parse module shape");
}

module.exports = {
  ExcelJS,
  IMG_EXT,
  ensureDir,
  extractPdfTextFromBuffer,
  imageSize,
  isMissingNodeModuleError,
  makeJobId,
  mammoth,
  normalizeAmount,
  normalizeCell,
  resolveOfficeFont,
  sha256File,
};
