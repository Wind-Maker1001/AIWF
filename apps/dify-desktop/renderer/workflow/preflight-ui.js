function createWorkflowPreflightUi(els, deps = {}) {
  const { focusNodeInCanvas = () => {} } = deps;

  function computePreflightRisk(issues = []) {
    const list = Array.isArray(issues) ? issues : [];
    let risk = 0;
    list.forEach((it) => {
      const level = String(it?.level || "").toLowerCase();
      const kind = String(it?.kind || "").toLowerCase();
      if (level === "error") risk += kind === "io_contract" ? 35 : 28;
      else risk += kind === "io_contract" ? 12 : 8;
      const msg = String(it?.message || "").toLowerCase();
      if (msg.includes("endpoint") && msg.includes("为空")) risk += 18;
    });
    const score = Math.max(0, Math.min(100, Math.round(risk)));
    let level = "low";
    let label = "低风险";
    let color = "#087443";
    if (score >= 70) {
      level = "high";
      label = "高风险";
      color = "#b42318";
    } else if (score >= 35) {
      level = "medium";
      label = "中风险";
      color = "#7a4a00";
    }
    return { score, level, label, color };
  }

  function renderPreflightReport(report) {
    const rep = report && typeof report === "object" ? report : { ok: true, issues: [] };
    const issues = Array.isArray(rep.issues) ? rep.issues : [];
    const errorCount = issues.filter((x) => String(x.level || "") === "error").length;
    const warnCount = issues.filter((x) => String(x.level || "") === "warning").length;
    const totalCount = issues.length;
    const risk = rep.risk && typeof rep.risk === "object" ? rep.risk : computePreflightRisk(issues);
    if (els.preflightSummary) {
      els.preflightSummary.textContent = `预检结果: ${rep.ok ? "通过" : "未通过"} | 错误 ${errorCount} | 警告 ${warnCount} | 总计 ${totalCount}`;
      els.preflightSummary.style.color = rep.ok ? "#087443" : "#b42318";
    }
    if (els.preflightRisk) {
      els.preflightRisk.textContent = `风险等级: ${risk.label} (${risk.score}/100)`;
      els.preflightRisk.style.color = risk.color;
    }
    if (!els.preflightRows) return;
    els.preflightRows.innerHTML = "";
    if (!issues.length) {
      els.preflightRows.innerHTML = '<tr><td colspan="4" style="color:#74879b">暂无问题</td></tr>';
      return;
    }
    issues.forEach((it) => {
      const tr = document.createElement("tr");
      const tdLevel = document.createElement("td");
      const tdType = document.createElement("td");
      const tdMsg = document.createElement("td");
      const tdAct = document.createElement("td");
      const level = String(it.level || "warning");
      tdLevel.textContent = level === "error" ? "错误" : "警告";
      tdLevel.style.color = level === "error" ? "#b42318" : "#7a4a00";
      tdType.textContent = String(it.kind || "");
      tdMsg.textContent = String(it.message || "");
      const nodeId = String(it.node_id || "").trim();
      if (nodeId) {
        const btn = document.createElement("button");
        btn.className = "mini";
        btn.textContent = "定位节点";
        btn.onclick = () => focusNodeInCanvas(nodeId);
        tdAct.appendChild(btn);
      } else {
        tdAct.textContent = "-";
      }
      tr.append(tdLevel, tdType, tdMsg, tdAct);
      els.preflightRows.appendChild(tr);
    });
  }

  function renderAutoFixDiff(summary) {
    const s = summary && typeof summary === "object" ? summary : null;
    if (els.preflightFixSummary) {
      if (!s || !s.changed) {
        els.preflightFixSummary.textContent = "自动修复差异: 暂无";
        els.preflightFixSummary.style.color = "#5e7389";
      } else {
        els.preflightFixSummary.textContent =
          `自动修复差异: 重复连线 ${s.removed_dup_edges || 0}，自环 ${s.removed_self_loops || 0}，断裂连线 ${s.removed_broken_edges || 0}，孤立节点 ${s.removed_isolated_nodes || 0}`;
        els.preflightFixSummary.style.color = "#087443";
      }
    }
    if (!els.preflightFixRows) return;
    els.preflightFixRows.innerHTML = "";
    if (!s || !s.changed) {
      els.preflightFixRows.innerHTML = '<tr><td colspan="2" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const groups = [
      { key: "dup_edges", label: "重复连线", rows: Array.isArray(s.dup_edges) ? s.dup_edges : [] },
      { key: "self_loops", label: "自环连线", rows: Array.isArray(s.self_loops) ? s.self_loops : [] },
      { key: "broken_edges", label: "断裂连线", rows: Array.isArray(s.broken_edges) ? s.broken_edges : [] },
      { key: "isolated_nodes", label: "孤立节点", rows: Array.isArray(s.isolated_nodes) ? s.isolated_nodes : [] },
    ];
    groups.forEach((g) => {
      g.rows.slice(0, 30).forEach((it) => {
        const tr = document.createElement("tr");
        const tdType = document.createElement("td");
        const tdDetail = document.createElement("td");
        tdType.textContent = g.label;
        if (g.key === "isolated_nodes") {
          tdDetail.textContent = String(it?.id || "");
        } else {
          const from = String(it?.from || "");
          const to = String(it?.to || "");
          tdDetail.textContent = `${from} -> ${to}`;
        }
        tr.append(tdType, tdDetail);
        els.preflightFixRows.appendChild(tr);
      });
    });
    if (!els.preflightFixRows.children.length) {
      els.preflightFixRows.innerHTML = '<tr><td colspan="2" style="color:#74879b">暂无</td></tr>';
    }
  }

  return { computePreflightRisk, renderPreflightReport, renderAutoFixDiff };
}

export { createWorkflowPreflightUi };
