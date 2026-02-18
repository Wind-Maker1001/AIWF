const {
  computeViaRust,
  callExternalAi,
} = require("../workflow_services");
const { summarizeCorpus } = require("../workflow_utils");

process.on("message", async (msg) => {
  try {
    const reqType = String(msg?.type || "");
    const requestId = String(msg?.id || "");
    const task = String(msg?.task || "");
    const payload = msg?.payload || {};
    if (reqType && reqType !== "run") return;
    if (!task) throw new Error("missing task");

    if (task === "compute_rust") {
      const out = await computeViaRust(String(payload.corpusText || ""), payload.options || {});
      process.send?.({ type: "ok", id: requestId, data: out });
      return;
    }

    if (task === "ai_refine") {
      const corpusText = String(payload.corpusText || "");
      const metrics = payload.metrics && typeof payload.metrics === "object"
        ? payload.metrics
        : summarizeCorpus(corpusText);
      const out = await callExternalAi(payload.workflowPayload || {}, corpusText, metrics);
      process.send?.({ type: "ok", id: requestId, data: out });
      return;
    }

    throw new Error(`unsupported isolated task: ${task}`);
  } catch (e) {
    process.send?.({
      type: "error",
      id: String(msg?.id || ""),
      error: String(e && e.stack ? e.stack : e),
    });
  }
});
