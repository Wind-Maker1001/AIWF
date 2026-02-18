const { runOfflineCleaning } = require("./offline_engine");

process.on("message", async (msg) => {
  try {
    const payload = msg && msg.payload ? msg.payload : {};
    const result = await runOfflineCleaning(payload);
    if (process.send) process.send({ type: "result", data: result });
    process.exit(0);
  } catch (e) {
    if (process.send) process.send({ type: "error", error: String(e && e.stack ? e.stack : e) });
    process.exit(1);
  }
});

