import { NODE_CATALOG } from "./defaults.js";
import { createWorkflowStore } from "./store.js";
import { validateGraph } from "./graph.js";
import { WorkflowCanvas } from "./canvas.js";

const $ = (id) => document.getElementById(id);

const store = createWorkflowStore();

const els = {
  palette: $("palette"),
  nodeType: $("nodeType"),
  workflowName: $("workflowName"),
  inputFiles: $("inputFiles"),
  reportTitle: $("reportTitle"),
  aiEndpoint: $("aiEndpoint"),
  aiKey: $("aiKey"),
  aiModel: $("aiModel"),
  rustEndpoint: $("rustEndpoint"),
  rustRequired: $("rustRequired"),
  status: $("status"),
  nodeRuns: $("nodeRuns"),
  diagRuns: $("diagRuns"),
  log: $("log"),
  canvasWrap: $("canvasWrap"),
  canvasSurface: $("canvasSurface"),
  nodesLayer: $("nodesLayer"),
  guideLayer: $("guideLayer"),
  minimap: $("minimap"),
  edges: $("edges"),
  btnAdd: $("btnAdd"),
  btnReset: $("btnReset"),
  btnClear: $("btnClear"),
  btnRun: $("btnRun"),
  btnDiagRefresh: $("btnDiagRefresh"),
  btnExport: $("btnExport"),
  btnSaveFlow: $("btnSaveFlow"),
  btnLoadFlow: $("btnLoadFlow"),
  snapGrid: $("snapGrid"),
  btnZoomOut: $("btnZoomOut"),
  btnZoomIn: $("btnZoomIn"),
  btnZoomReset: $("btnZoomReset"),
  zoomText: $("zoomText"),
  btnAlignLeft: $("btnAlignLeft"),
  btnAlignTop: $("btnAlignTop"),
  btnDistributeH: $("btnDistributeH"),
  btnDistributeV: $("btnDistributeV"),
  btnUnlinkSelected: $("btnUnlinkSelected"),
};

const canvas = new WorkflowCanvas({
  store,
  nodeCatalog: NODE_CATALOG,
  canvasWrap: els.canvasWrap,
  canvasSurface: els.canvasSurface,
  nodesLayer: els.nodesLayer,
  guideLayer: els.guideLayer,
  minimapCanvas: els.minimap,
  edgesSvg: els.edges,
  onChange: renderAll,
  onWarn: (msg) => setStatus(msg, false),
});

const debugApiEnabled = (() => {
  try {
    const q = new URLSearchParams(window.location.search || "");
    return q.get("debug") === "1";
  } catch {
    return false;
  }
})();

if (debugApiEnabled) {
  window.__aiwfDebug = Object.freeze({
    // Simulate canvas link gesture semantics: relink same direction toggles off.
    tryLink: (from, to) => {
      const a = String(from || "");
      const b = String(to || "");
      if (!a || !b) return { ok: false, reason: "empty" };
      if (store.hasEdge(a, b)) {
        store.unlink(a, b);
        renderAll();
        return { ok: true, toggled: true };
      }
      const out = store.linkToFrom(a, b);
      if (out?.ok) renderAll();
      return out;
    },
    graph: () => store.exportGraph(),
    routeStats: () => canvas.getRouteMetrics(),
    selectNodes: (ids) => {
      canvas.setSelectedIds(Array.isArray(ids) ? ids : []);
      renderAll();
      return canvas.getSelectedIds();
    },
    setGraph: (graph) => {
      store.importGraph(graph || {});
      renderAll();
      return store.exportGraph();
    },
  });
} else {
  try { delete window.__aiwfDebug; } catch {}
}

function setStatus(text, ok = true) {
  els.status.className = `status ${ok ? "ok" : "bad"}`;
  els.status.textContent = text;
}

function renderPalette() {
  els.palette.innerHTML = "";
  NODE_CATALOG.forEach((n) => {
    const item = document.createElement("div");
    const titleWrap = document.createElement("div");
    const title = document.createElement("strong");
    const type = document.createElement("div");
    const desc = document.createElement("div");
    item.className = "palette-item";
    item.draggable = true;
    item.dataset.nodeType = String(n.type || "");
    title.textContent = String(n.name || "");
    titleWrap.appendChild(title);
    type.style.fontSize = "12px";
    type.style.color = "#4f6378";
    type.textContent = String(n.type || "");
    desc.style.fontSize = "12px";
    desc.style.color = "#6b7f94";
    desc.style.marginTop = "3px";
    desc.textContent = String(n.desc || "");
    item.append(titleWrap, type, desc);
    els.palette.appendChild(item);
  });
  Array.from(els.palette.querySelectorAll(".palette-item")).forEach((item) => {
    item.addEventListener("click", () => {
      els.nodeType.value = String(item.dataset.nodeType || "ingest_files");
    });
    item.addEventListener("dragstart", (evt) => {
      const t = String(item.dataset.nodeType || "");
      evt.dataTransfer.setData("text/plain", t);
      evt.dataTransfer.effectAllowed = "copy";
    });
  });
}

function renderNodeRuns(nodeRuns) {
  if (!Array.isArray(nodeRuns) || nodeRuns.length === 0) {
    els.nodeRuns.innerHTML = '<tr><td colspan="3" style="color:#74879b">未运行</td></tr>';
    return;
  }
  els.nodeRuns.innerHTML = "";
  nodeRuns.forEach((n) => {
    const tr = document.createElement("tr");
    const tdType = document.createElement("td");
    const tdStatus = document.createElement("td");
    const tdSec = document.createElement("td");
    const sec = Number.isFinite(Number(n.seconds)) ? `${Number(n.seconds).toFixed(3)}s` : "-";
    tdType.textContent = String(n.type || "");
    tdStatus.textContent = String(n.status || "");
    tdSec.textContent = sec;
    tr.append(tdType, tdStatus, tdSec);
    els.nodeRuns.appendChild(tr);
  });
}

function renderDiagRuns(summary) {
  const by = summary && typeof summary === "object" ? summary.by_chiplet : null;
  const entries = by && typeof by === "object" ? Object.entries(by) : [];
  if (!entries.length) {
    els.diagRuns.innerHTML = '<tr><td colspan="3" style="color:#74879b">暂无诊断</td></tr>';
    return;
  }
  els.diagRuns.innerHTML = "";
  entries
    .sort((a, b) => String(a[0]).localeCompare(String(b[0])))
    .forEach(([chiplet, item]) => {
      const tr = document.createElement("tr");
      const tdType = document.createElement("td");
      const tdFail = document.createElement("td");
      const tdSec = document.createElement("td");
      const fr = Number(item.failure_rate || 0) * 100;
      tdType.textContent = chiplet;
      tdFail.textContent = `${fr.toFixed(1)}%`;
      tdSec.textContent = `${Number(item.seconds_avg || 0).toFixed(3)}s`;
      tr.append(tdType, tdFail, tdSec);
      els.diagRuns.appendChild(tr);
    });
}

function graphPayload() {
  store.setWorkflowName(els.workflowName.value);
  return store.exportGraph();
}

function runPayload() {
  const graph = graphPayload();
  return {
    workflow_id: graph.workflow_id || "custom_v1",
    workflow: graph,
    params: {
      report_title: String(els.reportTitle.value || "").trim(),
      input_files: String(els.inputFiles.value || "").trim(),
      md_only: true,
      paper_markdown_enabled: true,
      ocr_lang: "chi_sim+eng",
    },
    ai: {
      endpoint: String(els.aiEndpoint.value || "").trim(),
      api_key: String(els.aiKey.value || "").trim(),
      model: String(els.aiModel.value || "").trim(),
      temperature: 0.2,
    },
    rust: {
      endpoint: String(els.rustEndpoint.value || "").trim(),
      required: !!els.rustRequired.checked,
    },
  };
}

function renderAll() {
  canvas.setSnap(!!els.snapGrid.checked);
  canvas.setArrangePolicy({ preventOverlapOnAlign: false });
  canvas.render();
  const pct = Math.round(canvas.getZoom() * 100);
  if (els.zoomText) els.zoomText.textContent = `${pct}%`;
}

function exportJson() {
  const json = JSON.stringify(graphPayload(), null, 2);
  els.log.textContent = json;
  setStatus("已导出流程 JSON 到右侧日志区", true);
}

function setZoom(z) {
  canvas.setZoom(z);
  renderAll();
}

async function saveFlow() {
  try {
    const graph = graphPayload();
    const name = String(els.workflowName.value || "").trim() || "workflow";
    const out = await window.aiwfDesktop.saveWorkflow(graph, name);
    if (out?.ok) setStatus(`流程已保存: ${out.path}`, true);
    else if (!out?.canceled) setStatus(`保存失败: ${out?.error || "unknown"}`, false);
  } catch (e) {
    setStatus(`保存失败: ${e}`, false);
  }
}

async function loadFlow() {
  try {
    const out = await window.aiwfDesktop.loadWorkflow();
    if (!out?.ok) {
      if (!out?.canceled) setStatus(`加载失败: ${out?.error || "unknown"}`, false);
      return;
    }
    store.importGraph(out.graph || {});
    els.workflowName.value = store.state.graph.name || "自定义流程";
    renderAll();
    setStatus(`流程已加载: ${out.path}`, true);
  } catch (e) {
    setStatus(`加载失败: ${e}`, false);
  }
}

async function runWorkflow() {
  const graph = graphPayload();
  const valid = validateGraph(graph);
  if (!valid.ok) {
    setStatus(`流程校验失败: ${valid.errors.join(" | ")}`, false);
    return;
  }
  setStatus("工作流运行中...");
  try {
    const out = await window.aiwfDesktop.runWorkflow(runPayload(), {});
    els.log.textContent = JSON.stringify(out, null, 2);
    renderNodeRuns(out.node_runs || []);
    await refreshDiagnostics();
    setStatus(out.ok ? `运行完成: ${out.run_id}` : `运行结束: ${out.status || "failed"}`, !!out.ok);
  } catch (e) {
    setStatus(`运行失败: ${e}`, false);
  }
}

async function refreshDiagnostics() {
  try {
    const out = await window.aiwfDesktop.getWorkflowDiagnostics({ limit: 80 });
    renderDiagRuns(out || {});
  } catch {}
}

els.btnAdd.addEventListener("click", () => {
  const t = String(els.nodeType.value || "").trim();
  if (!t) {
    setStatus("节点类型不能为空", false);
    return;
  }
  store.addNode(t, 60, 60);
  renderAll();
});

els.btnReset.addEventListener("click", () => {
  store.reset();
  els.workflowName.value = store.state.graph.name || "自由编排流程";
  renderAll();
  setStatus("已重置默认流程", true);
});

els.btnClear.addEventListener("click", () => {
  store.clear();
  renderAll();
  setStatus("画布已清空", true);
});

els.btnRun.addEventListener("click", runWorkflow);
els.btnDiagRefresh.addEventListener("click", refreshDiagnostics);
els.btnExport.addEventListener("click", exportJson);
els.btnSaveFlow.addEventListener("click", saveFlow);
els.btnLoadFlow.addEventListener("click", loadFlow);
els.snapGrid.addEventListener("change", () => renderAll());
els.btnZoomIn.addEventListener("click", () => setZoom(canvas.getZoom() + 0.1));
els.btnZoomOut.addEventListener("click", () => setZoom(canvas.getZoom() - 0.1));
els.btnZoomReset.addEventListener("click", () => setZoom(1));
function applyArrange(mode, label) {
  const out = canvas.alignSelected(mode);
  if (!out || !out.ok) return;
  if (Number(out.moved || 0) <= 0) {
    setStatus(`${label}: 节点已处于目标布局`, true);
    return;
  }
  setStatus(`${label}: 已调整 ${out.moved}/${out.total} 个节点`, true);
}

els.btnAlignLeft.addEventListener("click", () => applyArrange("left", "左对齐"));
els.btnAlignTop.addEventListener("click", () => applyArrange("top", "上对齐"));
els.btnDistributeH.addEventListener("click", () => applyArrange("hspace", "水平分布"));
els.btnDistributeV.addEventListener("click", () => applyArrange("vspace", "垂直分布"));
els.btnUnlinkSelected.addEventListener("click", () => {
  const ids = canvas.getSelectedIds();
  if (ids.length < 2) {
    setStatus("请先框选至少两个节点再取消连线", false);
    return;
  }
  const selected = new Set(ids);
  const before = store.state.graph.edges.length;
  store.state.graph.edges = store.state.graph.edges.filter((e) => !(selected.has(e.from) && selected.has(e.to)));
  const removed = before - store.state.graph.edges.length;
  if (removed > 0) {
    renderAll();
    setStatus(`已取消 ${removed} 条框选节点连线`, true);
  } else {
    setStatus("框选节点之间不存在可取消的连线", false);
  }
});
els.canvasWrap.addEventListener(
  "wheel",
  (evt) => {
    if (!evt.ctrlKey) return;
    evt.preventDefault();
    setZoom(canvas.getZoom() + (evt.deltaY < 0 ? 0.08 : -0.08));
  },
  { passive: false }
);

els.canvasWrap.addEventListener("dragover", (evt) => {
  evt.preventDefault();
  evt.dataTransfer.dropEffect = "copy";
});

els.canvasWrap.addEventListener("drop", (evt) => {
  evt.preventDefault();
  const t = String(evt.dataTransfer.getData("text/plain") || "").trim();
  if (!t) return;
  const snapEnabled = !!els.snapGrid.checked;
  const grid = 24;
  const world = canvas.clientToWorld(evt.clientX, evt.clientY);
  const rawX = world.x - 105;
  const rawY = world.y - 43;
  const x = snapEnabled ? Math.round(rawX / grid) * grid : rawX;
  const y = snapEnabled ? Math.round(rawY / grid) * grid : rawY;
  store.addNode(t, x, y);
  renderAll();
  setStatus(`已拖入节点: ${t}`, true);
});

renderPalette();
renderAll();
renderNodeRuns([]);
renderDiagRuns({});
refreshDiagnostics();
setStatus("就绪。可拖拽节点并连线后运行。", true);
