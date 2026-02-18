const { createEncodingSupport } = require("./main_runtime_encoding");
const { createFontSupport } = require("./main_runtime_fonts");
const { createToolRuntimeSupport } = require("./main_runtime_tools");
const { createBridgeSupport } = require("./main_runtime_bridge");

function createRuntimeSupport({ app, fs, path, execFileSync, fork, iconv }) {
  const encoding = createEncodingSupport({ app, fs, path, iconv });
  const fonts = createFontSupport({ app, fs, path });
  const tools = createToolRuntimeSupport({ fs, path, execFileSync });
  const bridge = createBridgeSupport({ path, fork });

  return {
    inspectFileEncoding: encoding.inspectFileEncoding,
    toUtf8FileIfNeeded: encoding.toUtf8FileIfNeeded,
    checkChineseOfficeFonts: fonts.checkChineseOfficeFonts,
    installBundledFontsForCurrentUser: fonts.installBundledFontsForCurrentUser,
    checkTesseractRuntime: tools.checkTesseractRuntime,
    checkPdftoppmRuntime: tools.checkPdftoppmRuntime,
    checkTesseractLangs: tools.checkTesseractLangs,
    runOfflineCleaningInWorker: bridge.runOfflineCleaningInWorker,
    runViaBaseApi: bridge.runViaBaseApi,
    baseHealth: bridge.baseHealth,
    getTaskStoreStatus: bridge.getTaskStoreStatus,
  };
}

module.exports = { createRuntimeSupport };
