import { statusColor } from "./panels-ui-run-shared.js";

function createWorkflowPanelsQueueRenderers(els, deps = {}) {
  const {
    setStatus = () => {},
    refreshQueue = async () => {},
  } = deps;

  function renderQueueRows(items = []) {
    if (!els.queueRows) return;
    if (!Array.isArray(items) || !items.length) {
      els.queueRows.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无</td></tr>';
      return;
    }
    els.queueRows.innerHTML = "";
    items.slice(0, 80).forEach((it) => {
      const tr = document.createElement("tr");
      const tdTask = document.createElement("td");
      const tdStatus = document.createElement("td");
      const tdOp = document.createElement("td");
      tdTask.textContent = `${String(it.label || "task")} (${String(it.task_id || "").slice(0, 8)})`;
      tdStatus.textContent = String(it.status || "");
      const queueStatusColor = statusColor(it?.status);
      if (queueStatusColor) tdStatus.style.color = queueStatusColor;
      const cancelBtn = document.createElement("button");
      cancelBtn.className = "mini del";
      cancelBtn.textContent = "取消";
      cancelBtn.onclick = async () => {
        const out = await window.aiwfDesktop.cancelWorkflowTask({ task_id: it.task_id });
        setStatus(out?.ok ? "已取消任务" : `取消失败: ${out?.error || "unknown"}`, !!out?.ok);
        await refreshQueue();
      };
      const retryBtn = document.createElement("button");
      retryBtn.className = "mini";
      retryBtn.style.marginLeft = "4px";
      retryBtn.textContent = "重试";
      retryBtn.onclick = async () => {
        const out = await window.aiwfDesktop.retryWorkflowTask({ task_id: it.task_id });
        setStatus(out?.ok ? "已加入重试队列" : `重试失败: ${out?.error || "unknown"}`, !!out?.ok);
        await refreshQueue();
      };
      tdOp.append(cancelBtn, retryBtn);
      tr.append(tdTask, tdStatus, tdOp);
      els.queueRows.appendChild(tr);
    });
  }

  function renderQueueControl(control) {
    if (!els.queueControlText) return;
    const paused = !!control?.paused;
    const quotas = control?.quotas && typeof control?.quotas === "object" ? control.quotas : {};
    const quotaText = Object.keys(quotas).length
      ? Object.entries(quotas).map(([key, value]) => `${key}:${value}`).join(", ")
      : "默认";
    els.queueControlText.textContent = `队列状态: ${paused ? "暂停" : "运行"} | 并发配额: ${quotaText}`;
  }

  return {
    renderQueueControl,
    renderQueueRows,
  };
}

export { createWorkflowPanelsQueueRenderers };
