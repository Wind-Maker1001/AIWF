function createWorkflowPreflightUi(els, deps = {}) {
  const {
    focusNodeInCanvas = () => {},
    createElement = (tag) => document.createElement(tag),
  } = deps;

  function computePreflightRisk(issues = []) {
    const list = Array.isArray(issues) ? issues : [];
    let risk = 0;
    list.forEach((issue) => {
      const level = String(issue?.level || "").toLowerCase();
      const kind = String(issue?.kind || "").toLowerCase();
      if (level === "error") {
        if (kind === "io_contract") risk += 35;
        else if (kind === "unknown_node_type") risk += 32;
        else risk += 28;
      } else if (kind === "io_contract") {
        risk += 12;
      } else {
        risk += 8;
      }
      const message = String(issue?.message || "").toLowerCase();
      if (message.includes("endpoint") && (message.includes("为空") || message.includes("blank"))) {
        risk += 18;
      }
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

  function normalizeIssue(issue = {}) {
    const kind = String(issue.kind || "").trim().toLowerCase();
    const level = String(issue.level || "warning").trim().toLowerCase();
    const nodeId = String(issue.node_id || "").trim();
    const originalMessage = String(issue.message || "").trim();
    const errorCode = String(issue.error_code || "").trim();
    const errorPath = String(issue.error_path || "").trim();
    const errorContract = String(issue.error_contract || "").trim();
    const contractBoundary = String(issue.contract_boundary || "").trim();
    const resolutionHint = String(issue.resolution_hint || "").trim();
    const actionTextFromIssue = String(issue.action_text || "").trim();
    if (kind === "unknown_node_type") {
      return {
        level,
        kind,
        nodeId,
        displayType: errorCode || kind,
        message: [
          "发现未注册节点类型。",
          "该节点不在当前桌面节点目录内，主路径已禁止导入、添加和运行。",
          contractBoundary ? `边界: ${contractBoundary}.` : "",
          resolutionHint ? `处理方式: ${resolutionHint}.` : "处理方式：替换为已注册节点，或先同步 Rust manifest / local node policy。",
          originalMessage ? `原始错误: ${originalMessage}` : "",
        ].filter(Boolean).join(" "),
        actionText: nodeId ? (actionTextFromIssue || "定位节点") : "同步目录或替换节点类型",
      };
    }
    if (kind === "graph_contract") {
      return {
        level,
        kind,
        nodeId,
        displayType: errorCode || kind,
        message: [
          originalMessage,
          errorPath ? `路径: ${errorPath}.` : "",
          errorContract ? `契约: ${errorContract}.` : "",
          resolutionHint ? `处理方式: ${resolutionHint}.` : "",
        ].filter(Boolean).join(" "),
        actionText: nodeId ? (actionTextFromIssue || "定位节点") : "-",
      };
    }
    return {
      level,
      kind,
      nodeId,
      displayType: errorCode || kind,
      message: originalMessage,
      actionText: nodeId ? (actionTextFromIssue || "定位节点") : "-",
    };
  }

  function renderPreflightReport(report) {
    const rep = report && typeof report === "object" ? report : { ok: true, issues: [] };
    const issues = Array.isArray(rep.issues) ? rep.issues : [];
    const errorCount = issues.filter((item) => String(item?.level || "").toLowerCase() === "error").length;
    const warnCount = issues.filter((item) => String(item?.level || "").toLowerCase() === "warning").length;
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
    issues.forEach((issue) => {
      const normalized = normalizeIssue(issue);
      const tr = createElement("tr");
      const tdLevel = createElement("td");
      const tdType = createElement("td");
      const tdMsg = createElement("td");
      const tdAct = createElement("td");
      tdLevel.textContent = normalized.level === "error" ? "错误" : "警告";
      tdLevel.style.color = normalized.level === "error" ? "#b42318" : "#7a4a00";
      tdType.textContent = normalized.displayType || normalized.kind || "";
      tdMsg.textContent = normalized.message || "";
      if (normalized.nodeId) {
        const btn = createElement("button");
        btn.className = "mini";
        btn.textContent = normalized.actionText;
        btn.onclick = () => focusNodeInCanvas(normalized.nodeId);
        tdAct.appendChild(btn);
      } else {
        tdAct.textContent = normalized.actionText;
      }
      tr.append(tdLevel, tdType, tdMsg, tdAct);
      els.preflightRows.appendChild(tr);
    });
  }

  function renderAutoFixDiff(summary) {
    const result = summary && typeof summary === "object" ? summary : null;
    if (els.preflightFixSummary) {
      if (!result || !result.changed) {
        els.preflightFixSummary.textContent = "自动修复差异: 暂无";
        els.preflightFixSummary.style.color = "#5e7389";
      } else {
        els.preflightFixSummary.textContent = [
          "自动修复差异:",
          `重复边 ${result.removed_dup_edges || 0}`,
          `自环 ${result.removed_self_loops || 0}`,
          `断裂边 ${result.removed_broken_edges || 0}`,
          `孤立节点 ${result.removed_isolated_nodes || 0}`,
        ].join(" ");
        els.preflightFixSummary.style.color = "#087443";
      }
    }
    if (!els.preflightFixRows) return;
    els.preflightFixRows.innerHTML = "";
    if (!result || !result.changed) {
      els.preflightFixRows.innerHTML = '<tr><td colspan="2" style="color:#74879b">暂无</td></tr>';
      return;
    }
    const groups = [
      { key: "dup_edges", label: "重复边", rows: Array.isArray(result.dup_edges) ? result.dup_edges : [] },
      { key: "self_loops", label: "自环", rows: Array.isArray(result.self_loops) ? result.self_loops : [] },
      { key: "broken_edges", label: "断裂边", rows: Array.isArray(result.broken_edges) ? result.broken_edges : [] },
      { key: "isolated_nodes", label: "孤立节点", rows: Array.isArray(result.isolated_nodes) ? result.isolated_nodes : [] },
    ];
    groups.forEach((group) => {
      group.rows.slice(0, 30).forEach((item) => {
        const tr = createElement("tr");
        const tdType = createElement("td");
        const tdDetail = createElement("td");
        tdType.textContent = group.label;
        if (group.key === "isolated_nodes") {
          tdDetail.textContent = String(item?.id || "");
        } else {
          const from = String(item?.from || "");
          const to = String(item?.to || "");
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
