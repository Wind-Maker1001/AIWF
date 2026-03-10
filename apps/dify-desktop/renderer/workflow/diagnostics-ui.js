function createWorkflowDiagnosticsUi(els) {
  function renderNodeRuns(nodeRuns) {
    if (!Array.isArray(nodeRuns) || nodeRuns.length === 0) {
      els.nodeRuns.innerHTML = '<tr><td colspan="5" style="color:#74879b">未运行</td></tr>';
      return;
    }
    els.nodeRuns.innerHTML = "";
    nodeRuns.forEach((n) => {
      const tr = document.createElement("tr");
      const tdType = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdSec = document.createElement("td");
      const tdBytes = document.createElement("td");
      const tdErr = document.createElement("td");
      const sec = Number.isFinite(Number(n.seconds)) ? `${Number(n.seconds).toFixed(3)}s` : "-";
      tdType.textContent = String(n.type || "");
      tdStatus.textContent = String(n.status || "");
      tdSec.textContent = sec;
      const outBytes = Number.isFinite(Number(n.output_bytes)) ? Number(n.output_bytes) : 0;
      tdBytes.textContent = outBytes > 0 ? `${(outBytes / 1024).toFixed(1)} KB` : "-";
      tdErr.textContent = String(n.error_kind || "");
      tr.append(tdType, tdStatus, tdSec, tdBytes, tdErr);
      els.nodeRuns.appendChild(tr);
    });
  }

  function renderDiagRuns(summary, rustStats, perfStats) {
    const by = summary && typeof summary === "object" ? summary.by_chiplet : null;
    const entries = by && typeof by === "object" ? Object.entries(by) : [];
    const rustEntries = Array.isArray(rustStats?.items) ? rustStats.items : [];
    const perfByChiplet = new Map((Array.isArray(perfStats?.items) ? perfStats.items : []).map((x) => [String(x.chiplet || ""), x]));
    if (!entries.length) {
      els.diagRuns.innerHTML = "";
    } else {
      els.diagRuns.innerHTML = "";
      entries
        .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
        .forEach(([chiplet, item]) => {
          const tr = document.createElement("tr");
          const tdType = document.createElement("td");
          const tdFail = document.createElement("td");
          const tdP95 = document.createElement("td");
          const tdRetry = document.createElement("td");
          const tdFallback = document.createElement("td");
          const perf = perfByChiplet.get(String(chiplet || ""));
          const fr = Number(perf?.error_rate ?? item.failure_rate ?? 0) * 100;
          const p95 = Number(perf?.p95_seconds ?? item.seconds_avg ?? 0);
          const retry = Number(perf?.retry_rate ?? 0) * 100;
          const fallback = Number(perf?.fallback_rate ?? 0) * 100;
          tdType.textContent = chiplet;
          tdFail.textContent = `${fr.toFixed(1)}%`;
          tdP95.textContent = `${p95.toFixed(3)}s`;
          tdRetry.textContent = `${retry.toFixed(1)}%`;
          tdFallback.textContent = `${fallback.toFixed(1)}%`;
          tr.append(tdType, tdFail, tdP95, tdRetry, tdFallback);
          els.diagRuns.appendChild(tr);
        });
    }
    rustEntries
      .sort((a, b) => String(a.operator || "").localeCompare(String(b.operator || "")))
      .forEach((item) => {
        const tr = document.createElement("tr");
        const tdType = document.createElement("td");
        const tdFail = document.createElement("td");
        const tdP95 = document.createElement("td");
        const tdRetry = document.createElement("td");
        const tdFallback = document.createElement("td");
        const calls = Number(item.calls || 0);
        const errs = Number(item.err || 0);
        const fr = calls > 0 ? (errs / calls) * 100 : 0;
        tdType.textContent = `rust:${String(item.operator || "")}`;
        tdFail.textContent = `${fr.toFixed(1)}%`;
        tdP95.textContent = `${(Number(item.p95_ms || 0) / 1000).toFixed(3)}s`;
        tdRetry.textContent = "-";
        tdFallback.textContent = "-";
        tr.append(tdType, tdFail, tdP95, tdRetry, tdFallback);
        els.diagRuns.appendChild(tr);
      });
    if (!entries.length && !rustEntries.length) {
      els.diagRuns.innerHTML = '<tr><td colspan="5" style="color:#74879b">暂无诊断</td></tr>';
    }
  }

  async function fetchRustRuntimeStats() {
    try {
      const endpoint = String(els.rustEndpoint.value || "").trim().replace(/\/$/, "");
      if (!endpoint) return null;
      const resp = await fetch(`${endpoint}/operators/runtime_stats_v1`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ op: "summary" }),
      });
      if (!resp.ok) return null;
      return await resp.json();
    } catch {
      return null;
    }
  }

  return { renderNodeRuns, renderDiagRuns, fetchRustRuntimeStats };
}

export { createWorkflowDiagnosticsUi };
