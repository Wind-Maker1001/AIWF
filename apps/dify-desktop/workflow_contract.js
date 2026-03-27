// Keep a single authored workflow-contract interpreter in the renderer module.
// Node 24 can synchronously require that ESM module, which lets the main path
// reuse the same implementation instead of maintaining a second copy here.
module.exports = require("./renderer/workflow/workflow-contract.js");
