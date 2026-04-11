// Keep a single authored workflow-contract helper module in the renderer tree.
// This file intentionally re-exports UI/error-formatting helpers and shared
// workflow constants only; executable workflow validation lives in Rust.
module.exports = require("./renderer/workflow/workflow-contract.js");
